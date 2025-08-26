import asyncio
import re
import time
import aiohttp
from pyalex import config
from typing import List, Optional
import platform
import json


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



# DOI prefix 정규식
_DOI_PREFIX_RX = re.compile(r"^10\.\d{4,9}/", re.I)

def _extract_prefix(doi: str) -> Optional[str]:
    if not doi:
        return None
    doi = doi.lower().replace("https://doi.org/", "").strip()
    m = _DOI_PREFIX_RX.match(doi)
    return m.group(0) if m else None


# 다중 prefix 추출용

def guess_doi_pattern_from_samples(works: List[dict], sample_size: int = 10) -> List[str]:
    dois = [normalize_doi(w.get('doi')) for w in works if w.get('doi')]
    dois = [d for d in dois if d and d.startswith('10.')]
    if not dois:
        return []

    # 간단 샘플링(분포성 확보)
    n = len(dois)
    idxs = [0, int(0.25*(n-1)), int(0.5*(n-1)), int(0.75*(n-1)), n-1]
    sample = [dois[i] for i in idxs if 0 <= i < n]

    # prefix 추출 → 상위 다수 후보 선정
    prefixes = []
    for d in sample:
        p = _extract_prefix(d)
        if p:
            prefixes.append(p if p.endswith('/') else p + '/')

    # 중복 제거
    prefixes = sorted(set(prefixes))
    return prefixes  # ⚠️ 하나가 아니라 여러 개 허용


def is_target_journal_doi(doi: str, patterns: List[str]) -> bool:
    d = normalize_doi(doi)
    if not d:
        return False
    if not patterns:
        return d.startswith('10.')
    return any(d.startswith(p) for p in patterns)

# API 친화 헤더 + 백오프 유틸
DEFAULT_HEADERS = {
    "User-Agent": "OpenAlex-DOI-Enricher/1.0 (+mailto:{})".format("s0124kw@gmail.com")
}

async def _backoff_sleep(attempt: int, base: float = 0.5, cap: float = 8.0):
    """지수 백오프 + 소량 지터"""
    import random
    delay = min(cap, base * (2 ** (attempt - 1)))
    await asyncio.sleep(delay + random.random() * 0.2)

# 6️⃣ 비동기 보강 함수 정의
async def enrich_crossref_async(
    session: aiohttp.ClientSession,
    work: dict,
    patterns: List[str],
    max_rows: int = 3,
    total_timeout: float = 10.0,
    max_retries: int = 2
) -> Optional[dict]:
    """Crossref 우선 조회: prefix 사전 적용 + 저자/제목 매칭 전에 prefix 컷."""
    if work.get('doi'):
        return None

    title_raw = work.get('display_name', '')
    title = sanitize_title(title_raw)  # [CHANGED] 동기 처리(가벼움)
    if not title:
        return None

    authorships = work.get('authorships', [])
    if len(authorships) < 1:
        return None
    first = authorships[0].get('author', {}).get('display_name', '')

    # prefix 후보 (여러 개면 순차 시도)
    prefixes = patterns or [None]  # None이면 필터 없이 시도

    base_params_list = [
        {'query.bibliographic': title, 'query.author': first, 'rows': max_rows},
        {'query.title': title, 'query.author': first, 'rows': max_rows},
    ]

    for prefix in prefixes:
        for params in base_params_list:
            # [ADDED] 조회 전에 prefix 필터 사전 적용
            call_params = dict(params)
            if prefix:
                call_params['filter'] = f'prefix:{prefix}'

            for attempt in range(1, max_retries + 1):
                try:
                    async with session.get(
                        'https://api.crossref.org/works',
                        params=call_params,
                        timeout=aiohttp.ClientTimeout(total=total_timeout)
                    ) as resp:
                        if resp.status == 200:
                            items = (await resp.json()).get('message', {}).get('items', [])
                            for item in items:
                                doi = item.get('DOI')
                                if not doi:
                                    continue
                                # [ADDED] 제목/저자 매칭 전에 prefix 1차 컷 (2중 방어)
                                if prefix and not doi.lower().startswith(prefix.lower()):
                                    continue

                                if validate_author_match(work, item):
                                    return {
                                        'work_id': work['id'],
                                        'doi': doi,
                                        'authorships': process_crossref_authors(item.get('author', [])),
                                        'source': 'crossref'
                                    }
                            break  # 200이지만 유효 후보 없음 → 다음 params/prefix 시도
                        elif resp.status in {429, 500, 502, 503, 504} and attempt < max_retries:
                            await _backoff_sleep(attempt)
                            continue
                        else:
                            break  # 비재시도 에러
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < max_retries:
                        await _backoff_sleep(attempt)
                        continue
                    else:
                        break
    return None


async def enrich_datacite_async(
    session: aiohttp.ClientSession,
    work: dict,
    total_timeout: float = 10.0,
    max_retries: int = 2
) -> Optional[dict]:
    """Crossref 실패 시에만 호출되는 폴백."""
    if work.get('doi'):
        return None

    title = sanitize_title(work.get('display_name', ''))
    if not title:
        return None

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(
                'https://api.datacite.org/works',
                params={'query.title': title, 'page[size]': 1},
                timeout=aiohttp.ClientTimeout(total=total_timeout)
            ) as resp:
                if resp.status == 200:
                    items = (await resp.json()).get('data', [])
                    if items:
                        attr = items[0].get('attributes', {})
                        doi = attr.get('doi')
                        if doi:
                            return {
                                'work_id': work['id'],
                                'doi': doi,
                                'authorships': process_datacite_authors(attr.get('creators', [])),
                                'source': 'datacite'
                            }
                    break
                elif resp.status in {429, 500, 502, 503, 504} and attempt < max_retries:
                    await _backoff_sleep(attempt)
                    continue
                else:
                    break
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < max_retries:
                await _backoff_sleep(attempt)
                continue
            else:
                break
    return None

# 7️⃣ 전량을 한꺼번에 처리하는 함수

async def enrich_works_all(
    works: List[dict],
    patterns: List[str],
    concurrency: int = 30
) -> None:
    """
    한 번의 세션/루프 안에서:
      1) DOI 미보유 전체에 대해 Crossref 병렬
      2) 남은 것에 한해 DataCite 폴백 병렬
    """
    connector = aiohttp.TCPConnector(
        limit=concurrency * 2,
        limit_per_host=concurrency,
        force_close=False
    )
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=DEFAULT_HEADERS
    ) as session:
        sem = asyncio.Semaphore(concurrency)

        async def _run_crossref(w):
            async with sem:
                return await enrich_crossref_async(session, w, patterns)

        async def _run_datacite(w):
            async with sem:
                return await enrich_datacite_async(session, w)

        # 1) Crossref 전체 병렬
        targets = [w for w in works if not w.get('doi')]
        crossref_results = await asyncio.gather(
            *[_run_crossref(w) for w in targets],
            return_exceptions=True
        )

        # 결과 반영 (+ post-filter 안전망)
        resolved_ids = set()
        for r in crossref_results:
            if isinstance(r, dict) and is_target_journal_doi(r.get('doi'), patterns):
                for w in works:
                    if w['id'] == r['work_id']:
                        w.update({'doi': r['doi'], 'authorships': r['authorships']})
                        resolved_ids.add(r['work_id'])

        # 2) 못 채운 것만 DataCite 폴백
        remain = [w for w in works if not w.get('doi')]
        datacite_results = await asyncio.gather(
            *[_run_datacite(w) for w in remain],
            return_exceptions=True
        )

        # 결과 반영 (+ post-filter 안전망)
        for r in datacite_results:
            if isinstance(r, dict) and is_target_journal_doi(r.get('doi'), patterns):
                for w in works:
                    if w['id'] == r['work_id']:
                        w.update({'doi': r['doi'], 'authorships': r['authorships']})


def main(issns, year_start, year_end, email, prefix='',
         include_only_with_abstract: bool = False):
    from pyalex import Sources, Works
    # 0️⃣ 설정 초기화
    config.email = email

    # 1️⃣ 저널 ID 조회 (다중 ISSN 지원)
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

    # 2️⃣ 논문 수집 (연도별 분할 조회)
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
                works_q = works_q.filter(has_abstract='true')

            for page in works_q.select(FIELDS).paginate(per_page=200):
                yearly.extend(page)

            print(f"✅ {year}년 [{journal_id}] 수집: {len(yearly)}건")
            works.extend(yearly)

    print(f"✅ 전체 수집된 논문 수 (중복 전): {len(works)}건")

    # 3️⃣ 중복 제거
    unique_works = list({w['id']: w for w in works}.values())
    print(f"✅ 수집된 논문 수 (중복 제거 후): {len(unique_works)}건")

    # [REMOVED] abstract 평문화 블록은 주석 처리(필요 시 사용)
    # for w in unique_works:
    #     if w.get('abstract'):
    #         continue
    #     aii = w.get('abstract_inverted_index', {})
    #     if isinstance(aii, dict) and aii:
    #         abs_text = reconstruct_abstract(aii)
    #         abs_text = re.sub(r'\s+', ' ', abs_text).strip()
    #         w['abstract'] = abs_text
    #     else:
    #         w['abstract'] = ""
    #     w.pop('abstract_inverted_index', None)

    # 4️⃣ 초기 DOI 상태 분석
    empty_ids = [w['id'] for w in unique_works if not w.get('doi')]
    print(f"  • DOI 미보유: {len(empty_ids)}건")

    # 5️⃣ DOI 패턴 추정 (patterns는 사전/사후 모두에서 안전망 역할)
    patterns = guess_doi_pattern_from_samples(unique_works)

    # 6️⃣ [ADDED] 전체 병렬 보강 (배치 X, 단 한 번의 루프/세션)
    print("🔄 전체 DOI 보강 시작… (Crossref → DataCite 폴백)")
    asyncio.run(enrich_works_all(unique_works, patterns, concurrency=30))

    # 7️⃣ 최종 통계
    final = sum(1 for w in unique_works if w.get('doi'))
    enriched_from_empty = sum(1 for w in unique_works if w['id'] in empty_ids and w.get('doi'))
    print(f"📈 최종 DOI 보강: {final}/{len(unique_works)} ({(final/len(unique_works)*100 if unique_works else 0):.1f}%)")
    print(f"✅ 실제로 보강 완료한 건수: {enriched_from_empty}건")

    # 8️⃣ 최종 합본 저장 (원본 로직 유지)
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
    # 실행 시 인자 전달 (원본 기본값 유지)
    issns = ['0043-1354', '0011-9164', '0733-9429']
    year_start = 2015
    year_end = 2024
    email = 's0124kw@gmail.com'
    main(issns, year_start, year_end, email, '', include_only_with_abstract=True)
    print(f"⏱️ 총 소요: {time.time() - start:.1f}s")
