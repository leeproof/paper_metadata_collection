import asyncio
import aiohttp
import json
import os
import ast
import re
import re as _re
import pickle
import time  # 시간 측정용
from pathlib import Path
from tqdm.asyncio import tqdm
import pandas as pd
from collections import defaultdict

# === PATCH A-1: ROR logging helpers (add after imports) ===


# 환경변수로 로그 on/off (기본 on): ROR_LOG=0 이면 끔
ROR_LOG = os.environ.get("ROR_LOG", "1") != "0"

def _to_ror_url(rid: str) -> str:
    rid = (rid or "").strip()
    if not rid:
        return ""
    return rid if rid.startswith("http") else f"https://ror.org/{rid}"

def _safe_rowid(df, idx, col="id"):
    try:
        return str(df.at[idx, col])
    except Exception:
        return f"row#{idx}"

def _log_ror(where: str, work_id: str, detail: str, ror_url: str):
    if ROR_LOG:
        print(f"[ROR+1][{where}] work={work_id} {detail} -> ror={ror_url}")
# === /PATCH A-1 ===

# === PATCH A-2: calculate-style fallback matching helpers ===
def _normalize_aff_like_count(s: str) -> str:
    """Same normalization rule used by count.py: strip, rstrip punctuation, collapse spaces."""
    s = (s or "").strip()
    s = s.rstrip(" .,\t")
    s = " ".join(s.split())
    return s
# === /PATCH A-2 ===


# === ADD (공통 헬퍼) ==========================================

from numbers import Integral, Real

# 기본 stop / address 토큰
_STOP_TOKENS = set()

_ADDR_TOKENS = {
    "p.o.", "po", "box", "street", "st.", "road", "rd.", "avenue", "ave.", "blvd",
    "building", "bldg", "room", "rm", "floor", "fl", "city", "state", "zip", "postal"
}

# 대학/기관 이름 패턴 (콤마 토막에서 univ/institute/college 등 찾기)
_UNI_KEYWORDS = (
    "univ", "universi", "universit", "university",    # university, universiti 등
    "college", "colleg",
    "polytechnic", "polytech",
    "institut", "institute", "instituto",
    "center", "centre", "school", "faculty", "department",
)

# 주소(거리/도로) 패턴 (Engler-Bunte-Ring 1 같은 토막 찾기)
_ADDR_KEYWORDS2 = (
    "street", "st.", "strasse", "straße", "str.",
    "road", "rd.", "avenue", "ave", "av.",
    "ring", "way", "boulevard", "blvd", "laan", "weg",
)

def _clean_aff_text(raw: str) -> str:
    """
    affiliation 문자열을 ROR 검색용으로 가볍게 정제.
    (지금은 보조용으로만 쓰고, 메인 검색은 _collect_ror_queries_from_aff가 담당)
    """
    if not isinstance(raw, str):
        return ""
    s = raw.strip()
    if not s:
        return ""

    # 1) 첫 콤마 이전 후보(기관 핵심명) 우선
    head = s.split(",", 1)[0]
    if len(head) >= 5:
        s = head

    # 2) 괄호/특수문자 단순화
    s = _re.sub(r"\([^)]*\)", " ", s)         # 괄호 제거
    s = _re.sub(r"[/\\|•··—–\-]+", " ", s)    # 구분자 제거
    s = _re.sub(r"\s+", " ", s).strip()

    # 3) 숫자/우편번호 패턴 제거
    s = _re.sub(r"\b\d{2,}(-\d{2,})*\b", " ", s)

    # 4) address 토큰 등장 전까지만 남기기
    parts = [p.strip() for p in s.split()]
    cut = []
    for w in parts:
        lw = w.lower().strip(".,")
        if lw in _ADDR_TOKENS:
            break
        cut.append(w)
    s = " ".join(cut) if cut else s

    # 5) STOP_TOKENS: 첫 단어는 살리고, 2번째 이후만 제거
    parts = s.split()
    kept = []
    for i, w in enumerate(parts):
        lw = w.lower()
        if i > 0 and lw in _STOP_TOKENS:
            continue
        kept.append(w)
    s = " ".join(kept)

    return s.strip()


def _iter_affiliation_queries(aff_text: str) -> list[str]:
    """
    하나의 affiliation 문자열에서 ROR 쿼리용 후보 문자열들을 뽑는다.

    전략:
      1) 전체 문자열을 _clean_aff_text 로 정리한 결과
      2) 콤마(,)로 나눈 각 토막을 _clean_aff_text 로 정리한 결과
      3) 인접한 토막들을 2개 이상 붙인 조합(연속 구간)도 _clean_aff_text로 정리
    """
    raw = (aff_text or "").strip()
    if not raw:
        return []

    candidates: list[str] = []

    def _add(q: str):
        q = (q or "").strip()
        if not q:
            return
        if len(q) < 4:
            return
        # 이미 같은(대소문자 무시) 후보가 있으면 스킵
        ql = q.lower()
        for c in candidates:
            if c.lower() == ql:
                return
        candidates.append(q)

    # 1) 전체 문자열 clean 버전
    base = _clean_aff_text(raw)
    _add(base)

    # 2) 콤마로 나눈 토막들 (각각 단독)
    parts = [p.strip() for p in raw.split(",") if p.strip()]

    for seg in parts:
        cleaned = _clean_aff_text(seg)
        _add(cleaned)

    # 3) 인접한 토막들을 2개 이상 붙인 조합 (예: [0,1], [1,2], [0,1,2] ...)
    n = len(parts)
    for i in range(n):
        combo = parts[i]
        for j in range(i + 1, n):
            combo = combo + ", " + parts[j]
            cleaned = _clean_aff_text(combo)
            _add(cleaned)

    return candidates


def _extract_univ_segment(aff: str) -> str:
    """
    콤마로 나눈 토막 중, 'univ', 'college', 'institute' 등이 들어있는
    '대학/기관 이름 토막'만 추출.
    예: 'School of Industrial Technology, University Science of Malaysia, 11800 ...'
        -> 'University Science of Malaysia'
    """
    if not isinstance(aff, str):
        return ""
    parts = [p.strip() for p in aff.split(",") if p.strip()]
    if not parts:
        return ""
    for seg in parts:
        low = seg.lower()
        if any(k in low for k in _UNI_KEYWORDS):
            return seg
    return ""


def _extract_address_segment(aff: str) -> str:
    """
    콤마 토막 중 Engler-Bunte-Ring 1 같이 '숫자 + 거리명' 형태로 보이는
    주소 구간을 추출.
    """
    if not isinstance(aff, str):
        return ""
    parts = [p.strip() for p in aff.split(",") if p.strip()]
    cand = []
    for seg in parts:
        low = seg.lower()
        has_digit = any(ch.isdigit() for ch in seg)
        if has_digit and any(k in low for k in _ADDR_KEYWORDS2):
            cand.append(seg)
    return cand[0] if cand else ""


def _collect_ror_queries_from_aff(aff: str) -> list[str]:
    """
    affiliation 한 줄에서 ROR query 후보 문자열들을 여러 개 뽑는다.

    변경 후 전략:
      - 대학/기관 이름 세그먼트에 대해,
        콤마가 있다면 마지막 토막(=국가명)을 자동으로 붙인다.
      - 단, affiliation 전체 문자열에 따옴표(' 또는 ")가 하나도 없으면
        국가명을 붙이지 않는다.
      - 전체 affiliation 문자열(풀매칭)은 그대로 포함.
    """
    if not isinstance(aff, str):
        return []
    aff = aff.strip()
    if not aff:
        return []

    qs: list[str] = []

    # --- 0) 콤마 토막 분리 ---
    parts = [p.strip() for p in aff.split(",") if p.strip()]
    has_country = len(parts) >= 2
    country = parts[-1] if has_country else ""

    # --- 1) affiliation 문자열에 따옴표가 있는지 검사 ---
    # 따옴표가 하나도 없으면 국가명을 붙이지 않는다.
    aff_has_quote = ("'" in aff) or ('"' in aff)

    # --- 2) 대학/기관 segment 추출 ---
    for seg in parts:
        low = seg.lower()
        if any(k in low for k in _UNI_KEYWORDS):
            # --- 국가 추가 조건 ---
            if has_country and aff_has_quote:
                q = f"{seg} {country}".strip()
            else:
                q = seg.strip()

            if q and q not in qs:
                qs.append(q)

    # --- 3) 원본 전체 문자열도 후보로 추가 (풀 매칭 로그용) ---
    if len(aff) > 3 and aff not in qs:
        qs.append(aff)

    # # --- 4) 주소 segment도 후보로 추가 ---
    # addr_seg = _extract_address_segment(aff)
    # if addr_seg and addr_seg not in qs:
    #     qs.append(addr_seg)

    return qs




def _update_metrics(prefix, year_start, year_end, __anchor=None, **updates):
    """
    메트릭을 CSV 한 장(key,value)으로만 기록.
    __anchor 폴더가 없으면 조용히 스킵(기존 정책 유지)
    """
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

# ====== 여기부터: 기존 process() 전체를 이 코드로 교체 ======

async def process(input_csv, output_csv, cache_file, concurrency=20,
                  anchor_path: str | None = None):
    """
    단순/안전 버전 ROR 보강 파이프라인

    - CSV에서 authorships를 읽어서,
      ROR URL이 전혀 없는 행들만 대상으로 ROR API 호출.
    - affiliation 문자열에서 여러 쿼리 후보를 만들고(_collect_ror_queries_from_aff 사용),
      쿼리별로 한 번만 ROR API를 호출해서 중복 감소.
    - ROR을 찾은 경우, 해당 행의 authorships 구조 안에
      - affiliations[*].ror
      - institutions[*].ror
      - raw_affiliation_ror
      중 한 곳 이상에 반드시 박히도록 함.
    - 또한 row_rors(행 단위로 받은 ROR URL 목록)를 ror_new 컬럼에 JSON 배열로 저장.
    """

    import pandas as pd
    import asyncio
    import aiohttp
    import pickle
    from collections import defaultdict
    from urllib.parse import urlencode
    from tqdm.asyncio import tqdm as atqdm

    # -------- 0. 입력 CSV 로드 --------
    df = pd.read_csv(input_csv, dtype={"authorships": str})

    # 이미 ROR URL이 있는지 검사하는 정규식
    rx_ror = re.compile(r"https?://ror\.org/[0-9a-z]+", re.I)

    # ROR이 전혀 없는 행만 대상으로
    rows_needing_ror: set[int] = set()
    for idx, s in df["authorships"].items():
        txt = str(s or "")
        if not rx_ror.search(txt):
            rows_needing_ror.add(idx)

    # ✅ real3 기준 ROR 완전 결측 행 수 (보강 전)
    missing_before = len(rows_needing_ror)
    print(f"[real3] ROR URL이 전혀 없는 행 수(before): {missing_before}")

    if not rows_needing_ror:
        print("[real3] ROR이 이미 있는 행만 존재 → 보강 불필요")
        df["ror_new"] = ["[]"] * len(df)
        df.to_csv(output_csv, index=False)
        return

    # -------- 1. 행별 쿼리 후보 수집 --------
    # row_queries[idx] = 이 행에서 ROR API에 날릴 쿼리 문자열들의 집합
    row_queries: dict[int, set[str]] = defaultdict(set)
    # 전체 쿼리 집합(중복 제거용)
    all_queries: set[str] = set()

    def _parse_authorships(raw: str):
        if not raw or pd.isna(raw):
            return []
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(raw)
            except Exception:
                return []

    for idx in rows_needing_ror:
        auth_list = _parse_authorships(df.at[idx, "authorships"])
        if not isinstance(auth_list, list):
            continue

        for auth in auth_list:
            # 1) affiliations[*].raw_affiliation_string
            for aff in (auth.get("affiliations") or []):
                raw_aff = aff.get("raw_affiliation_string")
                if not raw_aff:
                    continue
                s = str(raw_aff).strip()
                if not s:
                    continue
                # 이 raw 문자열에서 여러 개의 ROR 쿼리 후보 뽑기
                qs = _collect_ror_queries_from_aff(s)
                if qs:
                    print(f"[raw->query] raw='{s}' → queries={qs}")
                for q in qs:
                    row_queries[idx].add(q)
                    all_queries.add(q)

            # 2) raw_affiliation_strings 리스트
            for raw_aff in auth.get("raw_affiliation_strings", []) or []:
                if not raw_aff:
                    continue
                s = str(raw_aff).strip().strip(",.; ")
                if not s:
                    continue
                # URL/ORCID는 제외
                if "http" in s.lower() or "orcid" in s.lower():
                    continue
                # 숫자만 있는 경우 제외
                if s.replace(" ", "").isdigit():
                    continue

                qs = _collect_ror_queries_from_aff(s)
                if qs:
                    print(f"[raw->query] raw='{s}' → queries={qs}")
                for q in qs:
                    row_queries[idx].add(q)
                    all_queries.add(q)

    if not all_queries:
        print("[real3] ROR 쿼리 후보가 하나도 생성되지 않았습니다.")
        df["ror_new"] = ["[]"] * len(df)
        df.to_csv(output_csv, index=False)
        return

    # -------- 2. ROR API 호출 (쿼리별로 한 번씩) --------
    # cache: ( "affq", query_text ) -> ror_id(또는 '')

    cache: dict = {}
    if cache_file and Path(cache_file).exists():
        try:
            cache = pickle.loads(Path(cache_file).read_bytes())
        except Exception:
            cache = {}

    async def fetch_ror_for_query(session, query_text: str, sem: asyncio.Semaphore) -> str:
        raw_q = (query_text or "").strip()
        if not raw_q:
            return ""

        key = ("affq", raw_q)
        if key in cache:
            return cache[key] or ""

        async def _try(url: str) -> str:
            retries, backoff = 3, 1
            async with sem:
                for attempt in range(1, retries + 1):
                    try:
                        resp = await asyncio.wait_for(session.get(url), timeout=10)
                        async with resp:
                            if resp.status == 200:
                                data = await resp.json()
                                items = (data or {}).get("items") or []
                                rid = (items[0].get("id") if items else "") or ""
                                return rid
                            if resp.status in {429, 500, 502, 503, 504} and attempt < retries:
                                await asyncio.sleep(backoff)
                                backoff *= 2
                                continue
                            return ""
                    except (asyncio.TimeoutError, aiohttp.ClientError):
                        if attempt < retries:
                            await asyncio.sleep(backoff)
                            backoff *= 2
                            continue
                        return ""

        print("[ROR_CALL_QUERY]", repr(raw_q))
        url = "https://api.ror.org/v2/organizations?" + urlencode({"query": raw_q})
        rid = await _try(url)
        print("[ROR_RESULT_QUERY]", repr(raw_q), "→", repr(rid))
        cache[key] = rid or ""
        return rid or ""

    # 쿼리 → ROR URL 매핑
    query_to_ror: dict[str, str] = {}

    async def _fetch_all():
        sem = asyncio.Semaphore(concurrency)
        conn = aiohttp.TCPConnector(limit_per_host=concurrency)
        timeout = aiohttp.ClientTimeout(total=None)

        async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
            queries = list(all_queries)

            # ✅ future->query 매핑 제거: gather + zip으로 1:1 보장
            coros = [fetch_ror_for_query(session, q, sem) for q in queries]
            results = await asyncio.gather(*coros, return_exceptions=True)

            for q, rid in zip(queries, results):
                if isinstance(rid, Exception):
                    print(f"❌ query={q!r} fetch 실패: {rid}")
                    continue
                if q and rid:
                    urlval = _to_ror_url(rid)
                    if urlval:
                        query_to_ror[q] = urlval

    await _fetch_all()

    # 캐시 저장
    if cache_file:
        try:
            Path(cache_file).write_bytes(pickle.dumps(cache))
        except Exception:
            pass

    
    # ✅ PATCH A-3: calculate-style fallback 준비 (query 정규화 버전 맵)
    # query_to_ror는 '검색 쿼리' 기준이라 행(row_queries)과 연결이 끊길 수 있어,
    # calculate.py 방식(텍스트 포함 여부)으로 주입할 때 사용할 정규화된 맵을 추가로 만든다.
    norm_query_to_ror: dict[str, str] = {}
    for _q, _u in query_to_ror.items():
        nq = _normalize_aff_like_count(_q)
        if nq and _u and (nq not in norm_query_to_ror):
            norm_query_to_ror[nq] = _to_ror_url(_u)
# -------- 3. ROR 결과를 행별 authorships에 실제로 써넣기 --------
    row_rors: dict[int, set[str]] = defaultdict(set)
    augment_count = 0

    for idx in rows_needing_ror:
        auth_list = _parse_authorships(df.at[idx, "authorships"])
        if not isinstance(auth_list, list) or not auth_list:
            continue

        # 이 행에 해당하는 쿼리들 중 ROR을 받은 것들
        urls: set[str] = set()
        for q in row_queries.get(idx, []):
            # 1) 원문 쿼리로 직접 매칭
            url = query_to_ror.get(q)
            if url:
                urls.add(_to_ror_url(url))
                continue

            # 2) 혹시 공백/구두점 차이로 키가 안 맞으면 정규화 키로도 매칭
            nq = _normalize_aff_like_count(q)
            nu = norm_query_to_ror.get(nq)
            if nu:
                urls.add(_to_ror_url(nu))

        if not urls:
            # 1) calculate.py 방식: authorships 문자열에 정규화된 쿼리(nq)가 포함되면 매칭
            auth_str = df.at[idx, "authorships"]
            if not isinstance(auth_str, str):
                auth_str = "" if auth_str is None else str(auth_str)

            matched_norm = None
            for nq, nu in norm_query_to_ror.items():
                if nq and (nq in _normalize_aff_like_count(auth_str)):
                    matched_norm = nq
                    if nu:
                        urls.add(_to_ror_url(nu))
                    break

            # 2) (핵심 수정) authorships 구조에서 affiliations[*].raw_affiliation_string / raw_affiliation_strings를 꺼내 exact-key 매칭
            if not urls:
                for auth in auth_list:
                    if not isinstance(auth, dict):
                        continue

                    # ✅ affiliations[*].raw_affiliation_string
                    for aff in (auth.get("affiliations") or []):
                        if not isinstance(aff, dict):
                            continue
                        raw = _normalize_aff_like_count(aff.get("raw_affiliation_string") or "")
                        if raw and (raw in norm_query_to_ror):
                            matched_norm = raw
                            urls.add(_to_ror_url(norm_query_to_ror[raw]))
                            break
                    if urls:
                        break

                    # ✅ raw_affiliation_strings[*]
                    for s in (auth.get("raw_affiliation_strings") or []):
                        raw = _normalize_aff_like_count(str(s) or "")
                        if raw and (raw in norm_query_to_ror):
                            matched_norm = raw
                            urls.add(_to_ror_url(norm_query_to_ror[raw]))
                            break
                    if urls:
                        break

            if not urls:
                continue

        # 일단 이 행에서 받은 ROR URL들을 기록(ror_new용)
        row_rors[idx].update(urls)

        # 여러 개가 있을 수 있지만, 최소 하나는 authorships 안에 박자
        u = sorted(urls)[0]
        injected = False

        # ✅ affiliations/institutions 위치 상관없이 authorships에 "무조건" URL 심기
        for auth in auth_list:
            if not isinstance(auth, dict):
                continue

            bag = auth.setdefault("ror_urls", [])
            if not isinstance(bag, list):
                bag = []
                auth["ror_urls"] = bag

            if u not in bag:
                bag.append(u)
                injected = True

            # (옵션) 단일값도 같이 넣고 싶으면
            if not auth.get("ror_url"):
                auth["ror_url"] = u
                injected = True

        if injected:
            augment_count += 1
            df.at[idx, "authorships"] = json.dumps(auth_list, ensure_ascii=False)

    print(f"[real3] ROR 보강 완료: +{augment_count}건")

    # -------- 4. ror_new 컬럼 채우기 --------
    ror_new_col = []
    for idx in range(len(df)):
        urls = sorted(row_rors.get(idx, []))
        ror_new_col.append(json.dumps(urls, ensure_ascii=False))
    df["ror_new"] = ror_new_col


    # -------- 5. CSV 저장 --------
    # ✅ real3 보강 이후에도 여전히 ROR URL이 전혀 없는 행 수 재계산
    still_missing = 0
    for _, s in df["authorships"].items():
        txt = str(s or "")
        if not rx_ror.search(txt):
            still_missing += 1

    print(f"[real3] ROR URL이 전혀 없는 행 수(after): {still_missing}, "
          f"delta={missing_before - still_missing}")
    
    df.to_csv(output_csv, index=False)

# ====== 여기까지: process() 전체 교체 ======



if __name__ == "__main__":
    INPUT_CSV = Path("0723_2015_2024_optimized.csv")      # 불러올 CSV 파일 경로
    OUTPUT_CSV = Path("0723_2015_2024_optimized_ror.csv")  # 저장할 CSV 파일 경로
    CACHE_FILE = Path("ror_cache.pkl")                   # 캐시 파일 경로
    CONCURRENCY = 20                                      # 동시 요청 수
    asyncio.run(process(INPUT_CSV, OUTPUT_CSV, CACHE_FILE, CONCURRENCY))
