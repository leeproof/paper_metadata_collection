import asyncio
import re
import time
import aiohttp
from pyalex import config
from concurrent.futures import ProcessPoolExecutor
from typing import List, Optional
import pandas as pd
import platform
import json

# === ADD (공통 헬퍼) ==========================================
from pathlib import Path

def _update_metrics(prefix, year_start, year_end, **updates):
    import os, json, math
    from numbers import Integral, Real
    try:
        import numpy as np
        _HAS_NP = True
    except Exception:
        _HAS_NP = False

    def _jsonable(v):
        if v is None: return None
        if isinstance(v, (bool, int, float, str)): return v
        if _HAS_NP:
            if isinstance(v, np.integer):  return int(v)
            if isinstance(v, np.floating): return None if (isinstance(v, float) and math.isnan(v)) else float(v)
            if isinstance(v, np.bool_):    return bool(v)
        if isinstance(v, Integral): return int(v)
        if isinstance(v, Real):     return float(v)
        if isinstance(v, (list, tuple)): return [_jsonable(x) for x in v]
        if isinstance(v, dict):          return {str(k): _jsonable(val) for k, val in v.items()}
        return str(v)

    # ★ 앵커 파일과 같은 폴더에 저장
    anchor = updates.pop("__anchor", None)
    out_dir = Path(anchor).parent if anchor else Path(".")
    path = out_dir / f"{prefix}_{year_start}_{year_end}_metrics.json"

    data = {}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            data = {}

    for k, v in updates.items():
        jv = _jsonable(v)
        if jv is not None:
            data[str(k)] = jv

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")




# =============================================================



def normalize_author_name(author_info):
    """저자명 정규화"""
    if isinstance(author_info, dict):
        name = author_info.get('author', {}).get('display_name', '') if 'author' in author_info else author_info.get('display_name', '')
    else:
        name = str(author_info)
    return ' '.join(name.lower().split())

def validate_author_match(openalex_work, crossref_item):
    """저자 순서별 매칭 검증"""
    openalex_authors = openalex_work.get('authorships', [])
    crossref_authors = crossref_item.get('author', [])
    
    if len(openalex_authors) < 1 or len(crossref_authors) < 1:
        return False
    
    # 1저자 매칭
    openalex_first = normalize_author_name(openalex_authors[0])
    crossref_first = normalize_author_name({'display_name': ' '.join(filter(None, [crossref_authors[0].get('given'), crossref_authors[0].get('family')]))})
    
    if openalex_first != crossref_first:
        return False
    
    # 2저자 매칭 (있는 경우에만)
    if len(openalex_authors) > 1 and len(crossref_authors) > 1:
        openalex_second = normalize_author_name(openalex_authors[1])
        crossref_second = normalize_author_name({'display_name': ' '.join(filter(None, [crossref_authors[1].get('given'), crossref_authors[1].get('family')]))})
        return openalex_second == crossref_second
    elif len(openalex_authors) > 1 or len(crossref_authors) > 1:
        return False  # 한쪽만 2저자가 있으면 매칭 실패
    
    return True

# 1️⃣ 조회할 필드 정의 (OpenAlex API에서 가져올 메타데이터 필드)
FIELDS = [
    'id',                     # 고유 식별자
    'doi',                    # DOI
    'primary_location',       # 저널명
    'display_name',           # 논문 제목
    'publication_date',       # 출판일자
    'authorships',            # 저자 정보
    # 'abstract_inverted_index', # 초록
]

def reconstruct_abstract(inverted_index: dict) -> str:
    
    # OpenAlex의 abstract_inverted_index를 평문 문자열로 복원
    
    if not isinstance(inverted_index, dict) or not inverted_index:
        return ""
    pairs = []
    for word, positions in inverted_index.items():
        if isinstance(positions, list):
            for pos in positions:
                if isinstance(pos, int):
                    pairs.append((pos, word))
    pairs.sort(key=lambda x: x[0])
    return " ".join(w for _, w in pairs)


# Windows 환경에서만 ProactorEventLoop 사용

if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# 3️⃣ 정규식 패턴 미리 컴파일
_bracket_rx    = re.compile(r"[\[【].*?[】\]]")
_nonascii_rx   = re.compile(r"[^\x00-\x7F]+")
_multispace_rx = re.compile(r"\s+")


def sanitize_title(raw: str) -> str:
    """제목에서 대괄호, 비-ASCII, 다중 공백 제거"""
    if not raw:
        return ""
    t = _bracket_rx.sub("", raw)
    t = _nonascii_rx.sub(" ", t)
    return _multispace_rx.sub(" ", t).strip()

# 4️⃣ authorships 변환 헬퍼 함수

def process_crossref_authors(authors):
    new_auths = []
    for au in authors:
        full = " ".join(filter(None, [au.get('given'), au.get('family')]))
        new_auths.append({
            "author": {"display_name": full, "orcid": au.get('ORCID')},
            "institutions": [],
            "countries": [],
            "is_corresponding": au.get('sequence') == 'first'
        })
    return new_auths


def process_datacite_authors(creators):
    new_auths = []
    for cr in creators:
        nm = cr.get('name', '')
        new_auths.append({
            "author": {"display_name": nm, "orcid": None},
            "institutions": [],
            "countries": [],
            "is_corresponding": False
        })
    return new_auths

# 5️⃣ DOI 패턴 헬퍼

def normalize_doi(doi: str) -> str:
    return doi.replace("https://doi.org/", "").strip() if doi else ""

def longest_common_prefix(strs: List[str]) -> str:
    if not strs:
        return ""
    shortest = min(strs, key=len)
    for i, ch in enumerate(shortest):
        if any(other[i] != ch for other in strs):
            return shortest[:i]
    return shortest

def guess_doi_pattern_from_samples(works: List[dict], sample_size: int = 10) -> List[str]:
    # 1) DOI 정규화
    dois = [normalize_doi(w['doi']) for w in works if w.get('doi')]
    # 2) '10.'으로 시작하는 유효 DOI만 필터
    dois = [d for d in dois if d.startswith('10.')]

    # 3) 위치별 12개 샘플링 (처음·20%·40%·60%·80%·마지막 각 2개씩)
    n = len(dois)
    positions = [
        0,
        int(0.2 * (n - 1)),
        int(0.4 * (n - 1)),
        int(0.6 * (n - 1)),
        int(0.8 * (n - 1)),
        n - 1
    ]
    sample = []
    for pos in positions:
        for offset in (0, 1):
            idx = min(pos + offset, n - 1)
            sample.append(dois[idx])

    # 4) 결과 출력 및 공통 prefix 계산
    print('정규화된 DOI 샘플:', sample)
    pattern = longest_common_prefix(sample)
    print('추출된 공통 prefix:', pattern)

    return [pattern] if pattern else []


def is_target_journal_doi(doi: str, patterns: List[str]) -> bool:
    d = normalize_doi(doi)
    if not d:
        return False
    if not patterns:
        return d.startswith('10.')
    return any(d.startswith(p) for p in patterns)

# 6️⃣ 비동기 보강 함수 정의
async def enrich_crossref_async(session: aiohttp.ClientSession,
                                 work: dict,
                                 executor: ProcessPoolExecutor,
                                 patterns: List[str]) -> Optional[dict]:
    if work.get('doi'):
        return None
    loop = asyncio.get_running_loop()
    title = await loop.run_in_executor(executor, sanitize_title,
                                        work.get('display_name', ''))
    if not title:
        return None
    
    # 1저자 확인
    authorships = work.get('authorships', [])
    if len(authorships) < 1:
        return None
    
    first = authorships[0].get('author', {}).get('display_name', '')
    
    for params in [
        {'query.bibliographic': title, 'query.author': first, 'rows':5},
        {'query.title': title, 'query.author': first, 'rows':5}
    ]:
        try:
            async with session.get(
                'https://api.crossref.org/works',
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 200:
                    items = (await resp.json()).get('message', {}).get('items', [])
                    
                    # 각 결과를 저자 매칭으로 검증
                    for item in items:
                        if validate_author_match(work, item):
                            doi = item.get('DOI')
                            if doi:
                                return {'work_id': work['id'],
                                        'doi': doi,
                                        'authorships': process_crossref_authors(item.get('author', [])),
                                        'source': 'crossref'}
        except Exception:
            continue
    return None


async def enrich_datacite_async(session: aiohttp.ClientSession,
                                 work: dict,
                                 executor: ProcessPoolExecutor,
                                 patterns: List[str]) -> Optional[dict]:
    if work.get('doi'):
        return None
    loop = asyncio.get_running_loop()
    title = await loop.run_in_executor(executor, sanitize_title,
                                        work.get('display_name', ''))
    if not title:
        return None
    try:
        async with session.get(
            'https://api.datacite.org/works',
            params={'query.title': title, 'page[size]':1},
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status == 200:
                items = (await resp.json()).get('data', [])
                if items:
                    attr = items[0].get('attributes', {})
                    doi = attr.get('doi')
                    if doi:
                        return {'work_id': work['id'],
                                'doi': doi,
                                'authorships': process_datacite_authors(
                                    attr.get('creators', [])),
                                'source': 'datacite'}
    except Exception:
        pass
    return None

# 7️⃣ 배치 단위 보강 실행
async def enrich_works_batch(batch: List[dict],
                              executor: ProcessPoolExecutor,
                              patterns: List[str]) -> None:
    connector = aiohttp.TCPConnector(limit=50, limit_per_host=10,
                                     force_close=False)
    timeout   = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for w in batch:
            if not w.get('doi'):
                tasks.append(enrich_crossref_async(session, w, executor, patterns))
                tasks.append(enrich_datacite_async(session, w, executor, patterns))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # 수정: 패턴에 맞지 않는 DOI는 자동 제외
        enhanced = {
            r['work_id']: r
            for r in results
            if isinstance(r, dict)
            and is_target_journal_doi(r['doi'], patterns)
        }
        for w in batch:
            if w['id'] in enhanced:
                r = enhanced[w['id']]
                w.update({'doi': r['doi'], 'authorships': r['authorships']})
                print(f"🔖 보강 완료: {w['id']} → DOI={r['doi']} (소스: {r['source']})")

def main(issns, year_start, year_end, email, prefix='', 
         include_only_with_abstract: bool = False):
    import asyncio
    from pyalex import Sources, Works, config
    from concurrent.futures import ProcessPoolExecutor
    import json
    import re
    # 0️⃣ 설정 초기화
    config.email = email
    BATCH_SIZE = 50

    # 1️⃣ ProcessPoolExecutor 사용
    executor = ProcessPoolExecutor()

    # 2️⃣ 저널 ID 조회 (다중 ISSN 지원)
    journal_ids = []
    for issn in issns:
        src_list = next(
            Sources().filter(issn=[issn])
                     .select(['id','display_name'])
                     .paginate(per_page=5)
        )
        if isinstance(src_list, list):
            journal_id   = src_list[0]['id']
            display_name = src_list[0]['display_name']
        else:
            journal_id   = src_list['id']
            display_name = src_list['display_name']
        journal_ids.append(journal_id)
        print(f"💡 선택된 저널: {display_name} (ID={journal_id})")

    # 3️⃣ 논문 수집 (연도별 분할 조회)
    works = []
    for journal_id in journal_ids:
        for year in range(year_start, year_end+1):
            from_date = f"{year}-01-01"
            to_date   = f"{year}-12-31"
            yearly = []

            works_q = Works().filter(
            journal=journal_id,
            from_publication_date=from_date,
            to_publication_date=to_date
        )
            if include_only_with_abstract:
                works_q=works_q.filter(has_abstract='true')

            for page in works_q.select(FIELDS).paginate(per_page=200):
                yearly.extend(page)

            print(f"✅ {year}년 [{journal_id}] 수집: {len(yearly)}건")
            works.extend(yearly)

    print(f"✅ 전체 수집된 논문 수 (중복 전): {len(works)}건")



    # 4️⃣ 중복 제거
    unique_works = list({w['id']: w for w in works}.values())
    print(f"✅ 수집된 논문 수 (중복 제거 후): {len(unique_works)}건")

    # # 역인덱스를 평문 초록으로 변환 -> 'abstract' 필드에 저장

    # for w in unique_works:
    #     # abstract가 이미 평문으로 있으면 그대로 둠
    #     if w.get('abstract'):
    #         continue

    #     aii = w.get('abstract_inverted_index', {})
    #     if isinstance(aii, dict) and aii:
    #         abs_text = reconstruct_abstract(aii)
    #         # 중복 공백 제거 및 양쪽 공백 제거
    #         abs_text = re.sub(r'\s+', ' ', abs_text).strip()
    #         w['abstract'] = abs_text
    #     else:
    #         w['abstract'] = ""

    #     w.pop('abstract_inverted_index', None)  # 원본 역인덱스 삭제


    # 5️⃣ 초기 DOI 상태 분석
    empty = [w['id'] for w in unique_works if not w.get('doi')]
    print(f"  • DOI 미보유: {len(empty)}건")

    # 6️⃣ DOI 패턴 추정
    patterns = guess_doi_pattern_from_samples(unique_works)

    # 7️⃣ 배치 단위 보강
    total = len(unique_works)
    for i in range(0, total, BATCH_SIZE):
        batch = unique_works[i:i+BATCH_SIZE]
        print(f"🔄 배치 {i//BATCH_SIZE+1}/{(total+BATCH_SIZE-1)//BATCH_SIZE} 처리 중…")
        asyncio.run(enrich_works_batch(batch, executor, patterns))
        print(f"📊 진행률: {min(i+BATCH_SIZE,total)}/{total} ({min(i+BATCH_SIZE,total)/total*100:.1f}%)")

    # 8️⃣ executor 정리
    executor.shutdown(wait=True)


    # 최종 DOI 보강 통계
    final = sum(1 for w in unique_works if w.get('doi'))
    
    missing_doi_count = len(empty)
    enriched_count = sum(1 for w in unique_works if w['id'] in empty and w.get('doi'))
    enrich_rate = round(enriched_count / missing_doi_count * 100, 1 ) if missing_doi_count else 0.0
    print(f"📈 최종 DOI 보강: {final}/{len(unique_works)} ({final/len(unique_works)*100:.1f}%)")
    print(f"✅ 실제로 보강 완료한 건수: {enriched_count}건")
    print(f"✅ 결측→보강 성공률: {enrich_rate}%")

    # ✅ metrics.json 업데이트
    _update_metrics(
        prefix, year_start, year_end,
        total_collected=len(unique_works),
        doi_missing=missing_doi_count,
        doi_enriched=enriched_count,
        doi_enrich_rate=enrich_rate,
        __anchor=f"{prefix}_{year_start}_{year_end}.json",  # 또는 out_file 변수
    )

    # 9️⃣ 스트리밍 방식으로 최종 합본만 저장

    # for w in unique_works:
    #     w.pop('abstract_inverted_index', None)  # 역인덱스 제거

    max_per_file = 9500
    out_file = f'{prefix}_{year_start}_{year_end}.json'
    with open(out_file, 'w', encoding='utf-8') as fp:
        fp.write('[\n')
        total = len(unique_works)
        for start in range(0, total, max_per_file):
            for i, item in enumerate(unique_works[start:start+max_per_file]):
                json.dump(item, fp, ensure_ascii=False)
                if start + i + 1 < total:
                    fp.write(',\n')
        fp.write('\n]')
    print(f"✅ 최종 합본 저장 완료: {out_file} ({total}건)")



if __name__ == '__main__':

    start = time.time()
    # 실행 시 인자 전달
    issns = ['0043-1354', '0011-9164', '0733-9429']
    year_start = 2015
    year_end = 2024
    email = 's0124kw@gmail.com'
    main(issns, year_start, year_end, email, '', include_only_with_abstract=True)
    print(f"⏱️ 총 소요: {time.time() - start:.1f}s")
