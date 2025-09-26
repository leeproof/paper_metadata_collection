import pandas as pd, asyncio, aiohttp, json, ast, re
from pathlib import Path
from tqdm.asyncio import tqdm as atqdm
from collections import defaultdict
from urllib.parse import urlencode


# === ADD (공통 헬퍼) ==========================================
from pathlib import Path
import csv
from numbers import Integral, Real

def _update_metrics(prefix, year_start, year_end, __anchor=None, **updates):
    """
    메트릭을 CSV 한 장(key,value)으로만 기록.
    __anchor 폴더가 없으면 조용히 스킵(기존 정책 유지)
    """
    from pathlib import Path
    import csv
    from numbers import Integral, Real

    def _norm(v):
        try:
            import numpy as np
            if isinstance(v, (np.integer,)): return int(v)
            if isinstance(v, (np.floating,)): return float(v)
        except Exception:
            pass
        if isinstance(v, Integral): return int(v)
        if isinstance(v, Real):     return float(v)
        return v

    updates = {k: _norm(v) for k, v in updates.items()}
    if __anchor is None:  # 기록 위치가 없으면 건너뜀
        return updates

    p = Path(__anchor)
    out_dir = p if p.is_dir() else p.parent
    if not out_dir.exists():
        return updates

    out_csv = out_dir / f"{prefix}_{year_start}_{year_end}_metrics.csv"
    existing = {}
    if out_csv.exists():
        try:
            with out_csv.open("r", encoding="utf-8", newline="") as f:
                rdr = csv.reader(f)
                header = next(rdr, None)
                if header and header[:2] == ["key", "value"]:
                    for row in rdr:
                        if len(row) >= 2:
                            existing[row[0]] = row[1]
        except Exception:
            pass

    for k, v in updates.items():
        existing[k] = "" if v is None else str(v)

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["key", "value"])
        for k in sorted(existing.keys()):
            wr.writerow([k, existing[k]])

    return updates
# =============================================================



# 반복 컴파일 방지
SKIP_PAT = re.compile(
                r'\b(ORCID|orcid\.org|Scopus|ResearcherID|Publons|Web of Science|'
                r'Google Scholar|ResearchGate|email|E-mail)\b', re.I
            )

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

async def process(input_csv, output_csv, cache_file, concurrency=20,
                  anchor_path: str | None = None):
    """
    2트랙 ROR 보강:
      A) institutions[].id(OpenAlex I-xxxxx) → OpenAlex Institution API로 ror 직통 조회
      B) affiliations[].raw_affiliation_string / raw_affiliation_strings 전체 문장 → ROR affiliation 매칭
    저장: 파싱 참조를 그대로 문자열화(재파싱 금지)
    """
    import pandas as pd, asyncio, aiohttp, json, ast
    from pathlib import Path
    from collections import defaultdict
    from urllib.parse import urlencode
    import pickle
    from tqdm.asyncio import tqdm as atqdm

    # ---- CSV 로드 ----
    df = pd.read_csv(input_csv)

    # ---- JSON/리터럴 파서 (파일 내 동일 함수와 호환) ----
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

    # ---- 후보 수집(참조 보존) ----
    openalex_id_to_entities = defaultdict(list)   # inst_id -> [(etype, idx, auth, inst_dict)]
    affstr_to_entities      = defaultdict(list)   # aff_text -> [(etype, idx, auth, aff_dict_or_raw)]
    row_infos               = {}                  # idx -> parsed infos (이 참조 객체에 ror 주입)

    for idx, row in df.iterrows():
        infos = parse_affiliations(row.get("authorships", ""))
        row_infos[idx] = infos
        for auth in (infos or []):
            # A) institutions: ror 비어 있고 I-ID 있으면 직통 후보
            for inst in (auth.get("institutions") or []):
                if inst.get("ror"):
                    continue
                inst_id = inst.get("id") or inst.get("openalex") or ""
                if isinstance(inst_id, str) and inst_id.startswith("https://openalex.org/I"):
                    openalex_id_to_entities[inst_id].append(("inst", idx, auth, inst))
            # B) affiliations: raw_affiliation_string(s) 전체 문장 후보
            for aff in (auth.get("affiliations") or []):
                if isinstance(aff, dict) and not aff.get("ror"):
                    raw = aff.get("raw_affiliation_string")
                    if isinstance(raw, str) and len(raw.strip()) >= 4:
                        affstr_to_entities[raw.strip()].append(("aff", idx, auth, aff))
            for raw in (auth.get("raw_affiliation_strings") or []):
                if isinstance(raw, str) and len(raw.strip()) >= 4:
                    affstr_to_entities[raw.strip()].append(("raw", idx, auth, raw))

    augment_count = 0

    # ---- 캐시 로드 ----
    cache = {}
    if cache_file and Path(cache_file).exists():
        try:
            cache = pickle.loads(Path(cache_file).read_bytes())
        except Exception:
            cache = {}

    # ---- HTTP 헬퍼 ----
    async def fetch_ror_via_openalex_inst(session, inst_id: str, sem: asyncio.Semaphore) -> str:
        if inst_id in cache:
            return cache[inst_id]
        url = inst_id.replace("https://openalex.org/", "https://api.openalex.org/")
        retries, backoff = 3, 1
        async with sem:
            for attempt in range(1, retries + 1):
                try:
                    resp = await asyncio.wait_for(session.get(url), timeout=10)
                    async with resp:
                        if resp.status == 200:
                            data = await resp.json()
                            r = (data or {}).get("ror", "") or ""
                            cache[inst_id] = r
                            return r
                        if resp.status in {429, 500, 502, 503, 504} and attempt < retries:
                            await asyncio.sleep(backoff); backoff *= 2
                        else:
                            cache[inst_id] = ""; return ""
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < retries:
                        await asyncio.sleep(backoff); backoff *= 2
        return ""

    async def fetch_ror_via_affiliation(session, aff_text: str, sem: asyncio.Semaphore) -> str:
        key = ("aff", aff_text)
        if key in cache:
            return cache[key]
        url = f"https://api.ror.org/organizations?{urlencode({'affiliation': aff_text})}"
        retries, backoff = 3, 1
        async with sem:
            for attempt in range(1, retries + 1):
                try:
                    resp = await asyncio.wait_for(session.get(url), timeout=10)
                    async with resp:
                        if resp.status == 200:
                            data = await resp.json()
                            items = (data or {}).get("items") or []
                            r = (items[0].get("id") if items else "") or ""
                            cache[key] = r
                            return r
                        if resp.status in {429, 500, 502, 503, 504} and attempt < retries:
                            await asyncio.sleep(backoff); backoff *= 2
                        else:
                            cache[key] = ""; return ""
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < retries:
                        await asyncio.sleep(backoff); backoff *= 2
        return ""

    # ---- 실행(A→B, 병렬) ----
    async def _run():
        nonlocal augment_count
        sem = asyncio.Semaphore(concurrency)
        timeout = aiohttp.ClientTimeout(total=None)
        conn = aiohttp.TCPConnector(limit_per_host=concurrency)
        async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
            # A) I-ID 직통
            tasks_a = {asyncio.create_task(fetch_ror_via_openalex_inst(session, inst_id, sem)): inst_id
                       for inst_id in openalex_id_to_entities}
            if tasks_a:
                for fut in atqdm(asyncio.as_completed(tasks_a), total=len(tasks_a),
                                 desc="Fetching ROR by OpenAlex I-ID", unit="task"):
                    inst_id = tasks_a[fut]
                    ror_id  = await fut
                    if ror_id:
                        for _, idx, auth, inst in openalex_id_to_entities[inst_id]:
                            if not inst.get("ror"):
                                augment_count += 1
                            inst["ror"] = ror_id

            # B) affiliation 문자열
            tasks_b = {asyncio.create_task(fetch_ror_via_affiliation(session, aff, sem)): aff
                       for aff in affstr_to_entities}
            if tasks_b:
                for fut in atqdm(asyncio.as_completed(tasks_b), total=len(tasks_b),
                                 desc="Fetching ROR by affiliation", unit="task"):
                    aff    = tasks_b[fut]
                    ror_id = await fut
                    if ror_id:
                        for _, idx, auth, obj in affstr_to_entities[aff]:
                            if isinstance(obj, dict):
                                if not obj.get("ror"):
                                    augment_count += 1
                                obj["ror"] = ror_id

    await _run()

    # ---- 캐시 저장 ----
    if cache_file:
        try:
            Path(cache_file).write_bytes(pickle.dumps(cache))
        except Exception:
            pass

    # ---- 저장(재파싱 금지: 참조 그대로 문자열화) ----
    for idx, infos in row_infos.items():
        df.at[idx, "authorships"] = json.dumps(infos, ensure_ascii=False)
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)

    # (metrics는 real4/real2에서 집계. 필요시 여기서 ror_enriched만 기록 가능)

if __name__ == "__main__":
    INPUT_CSV = Path("0723_2015_2024_optimized.csv")      # 불러올 CSV 파일 경로
    OUTPUT_CSV = Path("0723_2015_2024_optimized_ror.csv")  # 저장할 CSV 파일 경로
    CACHE_FILE = Path("ror_cache.pkl")                   # 캐시 파일 경로
    CONCURRENCY = 20                                      # 동시 요청 수
    asyncio.run(process(INPUT_CSV, OUTPUT_CSV, CACHE_FILE, CONCURRENCY))
