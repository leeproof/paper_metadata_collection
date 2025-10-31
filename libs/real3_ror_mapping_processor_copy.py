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
    최소 변경 요약:
      - 행별로 authorships 파싱 결과를 row_infos[idx]에 보관하고,
        여기에 ROR를 써넣은 뒤 그대로 DataFrame에 되돌려 저장.
      - OpenAlex I-ID가 있으면 Institution API로 ror 직통 조회,
        그렇지 않으면 raw_affiliation 문자열을 affiliation 매칭에 그대로 사용.
    """
    import pandas as pd
    import pickle, time, json, ast, re, asyncio, aiohttp
    from pathlib import Path
    from collections import defaultdict
    from urllib.parse import urlencode
    from tqdm.asyncio import tqdm as atqdm

    start_time = time.perf_counter()
    df = pd.read_csv(input_csv)

    # 캐시 로드
    cache = {}
    if Path(cache_file).exists():
        try:
            cache = pickle.loads(Path(cache_file).read_bytes())
        except Exception:
            cache = {}

    # 후보 보관용 + 행별 authorships 객체 보관
    iid_to_entities = defaultdict(list)   # inst_id -> [(idx, auth, inst_dict)]
    name_to_entities = defaultdict(list)  # 질의문자열 -> [(etype, idx, auth, obj)]
    row_infos: dict[int, list] = {}

    # ---- 후보 수집 (행 단위로 authorships 파싱 및 보관)
    for idx, row in df.iterrows():
        infos = parse_affiliations(row.get('authorships', ''))
        row_infos[idx] = infos  # ★ 핵심: 이 객체를 끝까지 유지/수정/저장

        # A) institutions: I-ID 직통 후보
        for auth in infos:
            for inst in auth.get('institutions', []) or []:
                if inst.get('ror'):
                    continue
                inst_id = inst.get('id') or inst.get('openalex') or ""
                if isinstance(inst_id, str) and inst_id.startswith("https://openalex.org/I"):
                    iid_to_entities[inst_id].append((idx, auth, inst))

        # B) affiliations: raw_affiliation 문자열 그대로 사용
        for auth in infos:
            # 1) affiliations[].raw_affiliation_string
            for aff in auth.get('affiliations', []) or []:
                raw = aff.get('raw_affiliation_string')
                if raw and not aff.get('ror'):
                    q = str(raw).strip()
                    if len(q) > 3:
                        name_to_entities[q].append(('aff', idx, auth, aff))
            # 2) raw_affiliation_strings (간단 노이즈만 필터)
            for raw in auth.get('raw_affiliation_strings', []) or []:
                if not raw:
                    continue
                s = str(raw).strip()
                if 'http' in s.lower() or 'orcid' in s.lower() or len(s) <= 3:
                    continue
                name_to_entities[s].append(('raw', idx, auth, raw))

    augment_count = 0

    # ---- HTTP helpers
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
                        if resp.status in {429,500,502,503,504} and attempt < retries:
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
        # ▶ v2로 통일
        url = f"https://api.ror.org/v2/organizations?{urlencode({'affiliation': aff_text})}"
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
                            cache[key] = r or ""
                            return r or ""
                        if resp.status in {429,500,502,503,504} and attempt < retries:
                            await asyncio.sleep(backoff); backoff *= 2
                        else:
                            cache[key] = ""; return ""
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < retries:
                        await asyncio.sleep(backoff); backoff *= 2
        return ""

    # ---- 실행(A→B)
    async def _run():
        nonlocal augment_count
        sem = asyncio.Semaphore(concurrency)
        conn = aiohttp.TCPConnector(limit_per_host=concurrency)
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
            # A) OpenAlex I-ID 직통
            if iid_to_entities:
                tasks_a = {asyncio.create_task(fetch_ror_via_openalex_inst(session, inst_id, sem)): inst_id
                           for inst_id in iid_to_entities}
                async for fut in atqdm(asyncio.as_completed(tasks_a), total=len(tasks_a),
                                       desc="Fetching ROR by OpenAlex I-ID", unit="task"):
                    inst_id = tasks_a[fut]
                    ror_id = await fut
                    if ror_id:
                        for idx, auth, inst in iid_to_entities[inst_id]:
                            if not inst.get("ror"):
                                augment_count += 1
                            inst["ror"] = ror_id

            # B) affiliation 문자열
            if name_to_entities:
                names = list(name_to_entities.keys())

                # 1) 태스크는 '리스트'로 만들고
                tasks = [asyncio.create_task(fetch_ror_via_affiliation(session, name, sem))
                        for name in names]

                # 2) task -> name 매핑을 zip으로 고정
                task_to_name = dict(zip(tasks, names))

                # 3) as_completed에는 'tasks'(리스트)를 넘김
                async for fut in atqdm(asyncio.as_completed(tasks), total=len(tasks),
                                    desc="Fetching ROR by affiliation", unit="task"):
                    # dict 역참조 대신 안전한 매핑 사용
                    name = task_to_name.get(fut)
                    ror_id = await fut

                    if not name or not ror_id:
                        continue

                    for etype, idx, auth, obj in name_to_entities[name]:
                        if etype == 'aff':
                            if not obj.get('ror'):
                                obj['ror'] = ror_id
                                augment_count += 1

                        elif etype == 'raw':
                            updated = False

                            # (A) affiliations[]가 실제로 있다면 거기에 먼저 기록
                            for aff in (auth.get('affiliations') or []):
                                if aff.get('raw_affiliation_string') == obj:
                                    if not aff.get('ror'):
                                        aff['ror'] = ror_id
                                        augment_count += 1
                                    updated = True
                                    break

                            if not updated:
                                # (B) affiliations가 없거나 raw만 있는 흔한 케이스:
                                #     institutions[]를 보장하고 최소 {'ror': ...}를 추가
                                if not auth.get('institutions'):
                                    auth['institutions'] = []
                                # 중복 방지
                                existing_rors = {
                                    (inst.get('ror') or "").strip().lower()
                                    for inst in (auth['institutions'] or [])
                                }
                                rid = (ror_id or "").strip().lower()
                                if rid and rid not in existing_rors:
                                    auth['institutions'].append({'ror': ror_id})
                                    augment_count += 1
                                    updated = True

                            if not updated and ror_id:
                                # (C) 최후 폴백: 문자열 필드에라도 남겨 정규식 추출 가능하게
                                prev = (auth.get('raw_affiliation_ror') or "").strip().lower()
                                if (ror_id or "").strip().lower() != prev:
                                    auth['raw_affiliation_ror'] = ror_id
                                    augment_count += 1

    await _run()

    # 캐시 저장
    try:
        Path(cache_file).write_bytes(pickle.dumps(cache))
    except Exception:
        pass

    # ★★★ 핵심: row_infos의 변경사항을 그대로 저장 ★★★
    for idx in range(len(df)):
        infos = row_infos.get(idx)
        if infos is not None:
            df.at[idx, 'authorships'] = json.dumps(infos, ensure_ascii=False)

    # 저장 완료
    df.to_csv(output_csv, index=False)

    # === METRICS: ror_missing / ror_enriched / ror_enrich_rate 기록 (최소 추가) ===
    import re as _re
    from pathlib import Path as _Path
    import pandas as _pd

    _rx_ror = _re.compile(r'https?://ror\.org/[0-9a-z]+', _re.I)

    def _missing_count(path_or_df):
        """authorships에 ROR URL이 하나도 없는 행의 개수"""
        if isinstance(path_or_df, (str, _Path)):
            _tmp = _pd.read_csv(path_or_df, dtype={'authorships': str})
        else:
            _tmp = path_or_df
        col = _tmp['authorships'].fillna('').astype(str)
        return int((col.apply(lambda s: len(_rx_ror.findall(s)) == 0)).sum())

    # 보강 전/후 결측 수
    before_missing = _missing_count(input_csv)  # 보강 '전' 파일 기준
    after_missing  = _missing_count(df)         # 보강 '후' 메모리 DF 기준

    ror_missing   = int(before_missing)
    ror_enriched  = int(max(0, before_missing - after_missing))
    ror_enrich_rate = round((ror_enriched / ror_missing) * 100, 1) if ror_missing else 0.0



    # metrics 파일명 규칙 유추: {prefix}_{YYYY}_{YYYY}_metrics.csv
    stem_out = _Path(output_csv).stem
    m = _re.search(r"(.+?)_(\d{4})_(\d{4})$", stem_out)
    if not m:
        # 혹시 output 이름이 다르면 input 파일명에서 재시도
        m = _re.search(r"(.+?)_(\d{4})_(\d{4})$", _Path(input_csv).stem)

    if m:
        px, ys, ye = m.group(1), int(m.group(2)), int(m.group(3))
        _update_metrics(
            px, ys, ye,
            ror_missing=ror_missing,
            ror_enriched=ror_enriched,
            ror_enrich_rate=ror_enrich_rate,
            __anchor=(anchor_path or output_csv),  # 같은 폴더에 기록
        )
    else:
        # 규칙을 못 찾으면 조용히 스킵(기존 정책 유지)
        pass
    # === /METRICS ===

    elapsed = time.perf_counter() - start_time
    print(f"[real3] ROR 보강 완료: +{augment_count}건, 소요 {elapsed:.2f}s")




if __name__ == "__main__":
    INPUT_CSV = Path("0723_2015_2024_optimized.csv")      # 불러올 CSV 파일 경로
    OUTPUT_CSV = Path("0723_2015_2024_optimized_ror.csv")  # 저장할 CSV 파일 경로
    CACHE_FILE = Path("ror_cache.pkl")                   # 캐시 파일 경로
    CONCURRENCY = 20                                      # 동시 요청 수
    asyncio.run(process(INPUT_CSV, OUTPUT_CSV, CACHE_FILE, CONCURRENCY))
