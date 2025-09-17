import asyncio
import aiohttp
import json
import ast
import re
import pickle
import time  # 시간 측정용
from pathlib import Path
from tqdm.asyncio import tqdm
import pandas as pd
from collections import defaultdict

# 사전 컴파일된 정규식 패턴으로 정제 속도 개선
PAREN_REGEX = re.compile(r"\([^)]*\)")
PUNCT_REGEX = re.compile(r"[\"',.&:/\\-]")
WHITESPACE_REGEX = re.compile(r"\s+")

def clean_name(name: str) -> str:
    name = name.lower()
    name = PAREN_REGEX.sub("", name)
    name = PUNCT_REGEX.sub(" ", name)
    name = WHITESPACE_REGEX.sub(" ", name).strip()
    return name

async def fetch_ror(session: aiohttp.ClientSession, name: str, cache: dict, sem: asyncio.Semaphore) -> str:
    if name in cache:
        return cache[name]
    url = f"https://api.ror.org/v2/organizations?query={aiohttp.helpers.quote(name)}"
    retries, backoff = 3, 1
    async with sem:
        for attempt in range(1, retries + 1):
            try:
                response = await asyncio.wait_for(session.get(url), timeout=10)
                async with response as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        items = data.get('items', [])
                        if items:
                            best = max(items, key=lambda x: x.get('score', 0))
                            cache[name] = best.get('id', '')
                            return cache[name]
                        cache[name] = ''
                        return ''
                    if resp.status in {429, 500, 502, 503, 504} and attempt < retries:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                    else:
                        cache[name] = ''
                        return ''
            except (asyncio.TimeoutError, aiohttp.ClientError):
                if attempt < retries:
                    await asyncio.sleep(backoff)
                    backoff *= 2
    return ''

# CSV 내 'authorships' 필드 JSON/리터럴 파싱
def parse_affiliations(raw: str) -> list:
    if not raw or pd.isna(raw):
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(raw)
        except Exception:
            return []

async def process(input_csv, output_csv, cache_file, concurrency=20):
    import pandas as pd
    import pickle
    import time
    from pathlib import Path
    import aiohttp
    import asyncio
    from collections import defaultdict
    import json
    import ast
    import re
    start_time = time.perf_counter()
    df = pd.read_csv(input_csv)
    cache = {}
    if Path(cache_file).exists():
        try:
            cache = pickle.loads(Path(cache_file).read_bytes())
        except Exception:
            cache = {}

    # 누락된 기관 및 affiliation별 매핑 대상 수집 (3단계 우선순위)
    name_to_entities = defaultdict(list)
    for idx, row in df.iterrows():
        infos = parse_affiliations(row.get('authorships', ''))
        for auth in infos:
            # 1) institutions[].display_name 우선
            for inst in auth.get('institutions', []):
                if not inst.get('ror'):
                    name = inst.get('display_name')
                    if name:
                        clean = clean_name(name)
                        name_to_entities[clean].append(('inst', idx, auth, inst))
            # 2) affiliations[].raw_affiliation_string 다음
            for aff in auth.get('affiliations', []):
                raw = aff.get('raw_affiliation_string')
                if raw and not aff.get('ror'):
                    parts = [p.strip() for p in raw.split(',')]
                    cand = next((p for p in parts if re.search(r'(University|College|Institute)', p, re.I)), parts[-1])
                    clean = clean_name(cand)
                    name_to_entities[clean].append(('aff', idx, auth, aff))
            # 3) raw_affiliation_strings 최후 보류 (ORCID 등 플랫폼/식별자 라벨은 제외)
            SKIP_PAT = re.compile(
                r'\b(ORCID|orcid\.org|Scopus|ResearcherID|Publons|Web of Science|'
                r'Google Scholar|ResearchGate|email|E-mail)\b', re.I
            )

            for raw in auth.get('raw_affiliation_strings', []):
                if not raw:
                    continue
                # ORCID, Scopus 등 플랫폼명/식별자 라벨은 제외
                if SKIP_PAT.search(raw):
                    continue
                clean = clean_name(raw)
                if len(clean) > 3:  # 정보가 너무 적은 경우는 제외
                    name_to_entities[clean].append(('raw', idx, auth, raw))


    augment_count = 0  # 보강된 ROR 개수
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit_per_host=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = {asyncio.create_task(fetch_ror(session, name, cache, sem)): name for name in name_to_entities}
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetching ROR IDs", unit="task"):
            name = tasks[future]
            ror_id = await future
            # fetched ROR을 해당 객체에 반영
            if ror_id:
                for etype, idx, auth, obj in name_to_entities[name]:
                    if etype == 'inst':
                        if not obj.get('ror'):
                            augment_count += 1
                        obj['ror'] = ror_id
                    elif etype == 'aff':
                        if not obj.get('ror'):
                            augment_count += 1
                        obj['ror'] = ror_id
                    elif etype == 'raw':
                        for aff in auth.get('affiliations', []):
                            if aff.get('raw_affiliation_string') == obj and not aff.get('ror'):
                                augment_count += 1
                                aff['ror'] = ror_id

    # 캐시 저장
    Path(cache_file).write_bytes(pickle.dumps(cache))

    # DataFrame에 반영 및 저장
    for idx, row in df.iterrows():
        updated = False
        infos = parse_affiliations(row.get('authorships', ''))
        for auth in infos:
            for inst in auth.get('institutions', []):
                if inst.get('ror') is not None:
                    updated = True
            for aff in auth.get('affiliations', []):
                if aff.get('ror') is not None:
                    updated = True
        if updated:
            df.at[idx, 'authorships'] = json.dumps(infos, ensure_ascii=False)

    df.to_csv(output_csv, index=False)
    elapsed = time.perf_counter() - start_time
    print(f"총 {augment_count}개 ROR 보강 완료, 소요 시간: {elapsed:.2f}초")

if __name__ == "__main__":
    INPUT_CSV = Path("0723_2015_2024_optimized.csv")      # 불러올 CSV 파일 경로
    OUTPUT_CSV = Path("0723_2015_2024_optimized_ror.csv")  # 저장할 CSV 파일 경로
    CACHE_FILE = Path("ror_cache.pkl")                   # 캐시 파일 경로
    CONCURRENCY = 20                                      # 동시 요청 수
    asyncio.run(process(INPUT_CSV, OUTPUT_CSV, CACHE_FILE, CONCURRENCY))
