import os, json, pandas as pd, asyncio
from pathlib import Path
from typing import List, Optional
from pyalex import Sources, config
import re
import csv
from contextlib import redirect_stdout
from pandas.errors import ParserError

# 각 단계별 기존 스크립트 import (함수화)
from libs import real1_test_openalex_sequential_optimized_0722 as real1
from libs import real2_test_openalex_to_csv as real2
from libs import real3_ror_mapping_processor as real3
from libs import real4_ror_extract as real4
from libs import real5_name_mapping as real5
from libs import real6_visualize_fast as real6


# Google Drive adapter
import json as _json
import ast as _ast
from collections.abc import Mapping

try:
    import streamlit as st
except Exception:
    st = None # 로컬 환경에서도 동작하도록 선택적 import
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

FORCE_JOURNAL_KEY: str | None = None  # ✅ 실행 동안 대표키를 강제로 고정


def _parse_keys_years_from_final(final_csv_path: str):
    """
    최종 CSV 파일명에서 keys(저널 슬러그들)와 연도 범위를 파싱.
    허용 패턴:
      1) <keys>_<y1>_<y2>_ror_extract_name.csv  (다연도)
      2) <keys>_<y1>_ror_extract_name.csv       (단일연도 → y2=y1로 처리)
    """
    name = Path(final_csv_path).name

    # 1) y1_y2 패턴 시도
    m = re.match(r"^(?P<keys>.+)_(?P<y1>\d{4})_(?P<y2>\d{4})_ror_extract_name\.csv$", name)
    if m:
        keys = m.group("keys").split("-")
        return keys, int(m.group("y1")), int(m.group("y2"))

    # 2) y1 단일 패턴 시도 → y2=y1
    m = re.match(r"^(?P<keys>.+)_(?P<y1>\d{4})_ror_extract_name\.csv$", name)
    if m:
        keys = m.group("keys").split("-")
        y = int(m.group("y1"))
        return keys, y, y

    raise ValueError(f"Unexpected final csv filename: {name}")

def _collect_metrics_paths_from_final(final_csv_path: str):
    """
    (멀티저널 × 멀티연도)용 metrics 수집:
      - 최종 CSV 파일명에서 keys와 연도범위(y1..y2)를 파싱
      - 각 (key, year)에 대해 아래 우선순위로 1개씩 찾음:
        1) 로컬 중첩: <base>/<key>/<year>/<key>_<year>_<year>_metrics.csv
        2) 로컬 평면: <base>/<key>_<year>_<year>_metrics.csv
        3) Drive 정확 경로: ROOT/<key>/<year>/<key>_<year>_<year>_metrics.csv
        4) Drive 전역 이름검색: 파일명이 일치하는 아무 경로
      - 찾으면 표준 로컬 경로(<base>/<key>/<year>/...)에 두고 반환 리스트에 포함
    """
    from pathlib import Path as _Path

    base_dir = _Path(final_csv_path).parent
    keys, y1, y2 = _parse_keys_years_from_final(final_csv_path)

    import re as _re
    def _norm_key(k: str) -> str:
        s = (k or "").strip().lower()
        s = s.replace("-", "_")
        s = _re.sub(r"\s+", "_", s)
        s = _re.sub(r"_+", "_", s)
        return s
    keys = [_norm_key(k) for k in keys]  # ← 이 한 줄이 핵심

    found = []
    seen = set()

    for key in keys:
        for y in range(y1, y2 + 1):
            std_local = base_dir / key / str(y) / f"{key}_{y}_{y}_metrics.csv"  # 표준 저장 위치
            flat_local = base_dir / f"{key}_{y}_{y}_metrics.csv"

            hit = None
            # 1) 로컬(중첩/평면)
            for cand in (std_local, flat_local):
                if cand.exists():
                    hit = cand
                    break

            # 2) Drive에서 가져오기 (표준 경로에 저장)
            if hit is None:
                std_local.parent.mkdir(parents=True, exist_ok=True)
                # 2-a) 예상 폴더 구조에서 시도
                if _gdrive_download_metric_by_key_exact_path(key, y, std_local):
                    hit = std_local
                else:
                    # 2-b) 전역 이름검색
                    if _gdrive_download_metric_by_name_search(key, y, std_local):
                        hit = std_local

            if hit is not None:
                rp = str(hit.resolve()) if hit.exists() else str(hit)
                if rp not in seen:
                    found.append(hit)
                    seen.add(rp)

    # (선택) 디버그: 기대 개수 vs 실제 수집 개수
    expected = len(keys) * (y2 - y1 + 1)
    print(f"[SUMMARY] metrics targets: expected={expected}, collected={len(found)}")
    if len(found) != expected:
        try:
            missing = []
            have = {p.name for p in found}
            for key in keys:
                for y in range(y1, y2 + 1):
                    name = f"{key}_{y}_{y}_metrics.csv"
                    if name not in have:
                        missing.append(name)
            if missing:
                print("[SUMMARY] missing metrics:", ", ".join(missing[:20]), ("... (+more)" if len(missing) > 20 else ""))
        except Exception:
            pass

    return found

# ==============================================================
# 기관정보(ROR) 기준 결측/보완 계산 헬퍼
# ==============================================================

def _load_mapped_affiliations_for_final(final_csv_path: str) -> list[str]:
    """
    final_csv_path(…_ror_extract_name.csv)를 기준으로,
    workdir_tmp 전체에서 mapped_affiliations_*.csv를 찾아
    normalized_affiliation 문자열을 모두 모은다.
    (같은 이름이면 중복 제거)
    """
    from pathlib import Path
    import pandas as pd

    base = Path(final_csv_path)
    mapped_set = set()

    # workdir_tmp 전체에서 mapped_affiliations_*.csv 수집
    root = base.parent  # 보통 LOCAL_WORKDIR (= workdir_tmp)
    try:
        for p in root.rglob("mapped_affiliations_*.csv"):
            try:
                df = pd.read_csv(p, encoding="utf-8-sig")
            except Exception:
                continue
            if "normalized_affiliation" not in df.columns:
                continue
            for s in df["normalized_affiliation"].dropna().astype(str):
                s = s.strip().rstrip(" .,\t")
                s = " ".join(s.split())
                if s:
                    mapped_set.add(s)
    except Exception:
        pass

    return sorted(mapped_set)


def _compute_inst_missing_and_recovered_for_final(final_csv_path: str) -> tuple[int, int]:
    """
    final_csv 기준으로,
    - inst_missing  : authorships 안에 ROR URL이 전혀 없는 논문 수
    - inst_recovered: 그 중에서 mapped_affiliations에 있는 기관명이 하나라도
                      등장하는 논문 수
    를 반환한다.
    """
    import pandas as pd
    import re
    from pathlib import Path

    base = Path(final_csv_path)
    try:
        df = pd.read_csv(base, encoding="utf-8-sig")
    except Exception:
        return 0, 0

    if "authorships" not in df.columns:
        return 0, 0

    rx_ror = re.compile(r"https?://ror\.org/[0-9a-z]+", re.I)

    authorships = df["authorships"].astype(str).fillna("")
    has_ror = authorships.apply(lambda s: bool(rx_ror.search(s)))
    mask_missing = ~has_ror
    inst_missing = int(mask_missing.sum())

    # 보완 후보 기관명 로딩
    mapped_affs = _load_mapped_affiliations_for_final(final_csv_path)
    if not mapped_affs or inst_missing == 0:
        return inst_missing, 0

    # ROR이 없는 논문들만 대상으로, 기관명이 포함되는지 검사
    missing_auth = authorships[mask_missing]

    def _has_mapped_aff(s: str) -> bool:
        for aff in mapped_affs:
            if aff and aff in s:
                return True
        return False

    inst_recovered = int(missing_auth.apply(_has_mapped_aff).sum())
    return inst_missing, inst_recovered


def _update_metrics_ror_for_piece(metrics_path: Path, final_csv_path: Path) -> None:
    """
    1) final_csv_path 기준으로 기관정보 결측/보완 수 계산
    2) metrics.csv를 읽어서 다음을 반영:
       - ror_missing_before_extract = 최초 결측 수
       - ror_missing                = 보강 후 남은 결측 수
       - ror_enriched               = 기관정보 보완 수
       - ror_missing_after_extract  = metrics.csv에서는 제거
    """
    import pandas as pd
    import csv as _csv

    try:
        inst_missing, inst_recovered = _compute_inst_missing_and_recovered_for_final(str(final_csv_path))
    except Exception as e:
        print(f"[WARN] _update_metrics_ror_for_piece: inst calc failed for {final_csv_path}: {e}")
        return

    missing_after = max(0, inst_missing - inst_recovered)
    enriched = max(0, inst_recovered)

    kv: dict[str, str] = {}

    # 기존 metrics 있으면 읽어서 다른 값은 유지
    if metrics_path.exists():
        try:
            df = pd.read_csv(metrics_path, dtype=str)

            # key/value 정규화
            if "key" not in df.columns or "value" not in df.columns:
                cols = list(df.columns)
                if len(cols) >= 2:
                    df = df.rename(columns={cols[0]: "key", cols[1]: "value"})
                elif len(cols) == 1:
                    df = df.reset_index().rename(columns={"index": "key", cols[0]: "value"})

            if "key" in df.columns and "value" in df.columns:
                for _, row in df.iterrows():
                    k = str(row["key"]).strip()
                    if not k:
                        continue
                    v = "" if pd.isna(row["value"]) else str(row["value"]).strip()
                    kv[k] = v
        except Exception as e:
            print(f"[WARN] _update_metrics_ror_for_piece: read failed for {metrics_path}: {e}")

    # ROR 관련 키 덮어쓰기
    kv["ror_missing_before_extract"] = str(inst_missing)
    kv["ror_missing"] = str(missing_after)
    kv["ror_enriched"] = str(enriched)
    # 요구사항: metrics.csv에서 ror_missing_after~는 제거
    kv.pop("ror_missing_after_extract", None)

    try:
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with metrics_path.open("w", newline="", encoding="utf-8-sig") as f:
            w = _csv.writer(f)
            w.writerow(["key", "value"])
            for k, v in kv.items():
                w.writerow([k, v])
    except Exception as e:
        print(f"[WARN] _update_metrics_ror_for_piece: write failed for {metrics_path}: {e}")


def build_summary_from_metrics_for_final(final_csv_path: str, out_csv_path=None):
    import pandas as pd
    from pathlib import Path

def build_summary_from_metrics_for_final(final_csv_path: str, out_csv_path=None):
    import pandas as pd
    from pathlib import Path

    def _to_num(x):
        if x is None: return None
        s = str(x).strip()
        if s=="" or s.lower()=="nan": return None
        s = s.replace(",","")
        if s.endswith("%"): s = s[:-1]
        try:
            f = float(s); return int(f) if float(int(f))==f else f
        except Exception: return None

    totals = {
        "total_collected": 0.0, "final_csv_rows": 0.0,
        "authorships_removed": 0.0,
        "id_pattern_removed": 0.0,
        "col_mismatch_removed": 0.0,            
        "doi_missing": 0.0, "doi_enriched": 0.0,
        "ror_missing": 0.0, "ror_enriched": 0.0,
        "ror_missing_before_extract": 0.0, "ror_missing_after_extract": 0.0,
    }
    editorial = {"authorships_removed","authorships_removed_empty_list"}  # id_pattern_removed 제외

    got_any=False
    for _,_,m in _iter_metrics_dfs_from_final(final_csv_path):
        if "key" not in m.columns or "value" not in m.columns:
            cols = list(m.columns)
            if len(cols) == 1:
                m = m.reset_index().rename(columns={"index":"key", cols[0]:"value"})
            elif len(cols) >= 2:
                m = m.rename(columns={cols[0]:"key", cols[1]:"value"})
        # ✅ 파일 내부 중복 key는 '마지막 값'만 사용 (append로 중복 누적되는 경우 방지)
        m = m.dropna(subset=["key"]).copy()
        m["key"] = m["key"].astype(str).str.strip()
        m = m[m["key"] != ""]
        m = m.drop_duplicates(subset=["key"], keep="last")

        got_any = True
        for _, row in m.iterrows():
            k = (row.get("key") or "").strip()
            v = _to_num(row.get("value"))
            if v is None:
                continue
            if k in editorial:
                totals["authorships_removed"] += v
            elif k == "json_rows":
                totals["total_collected"] += v
            elif k in totals:
                totals[k] += v

    def _ival(x):
        try: return int(x) if x is not None else 0
        except Exception: return 0

    if not got_any:
        df = pd.DataFrame([{
            "최초 수집 논문 수":0,
            "최종 수집 논문 수":0,
            "Editorial Material 삭제 수":0,
            "ID 패턴 불일치 삭제 수":0,
            "컬럼수 불일치 삭제 수": 0,
            "검증_차이(=0이어야 정상)": 0,
            "최종 CSV 행 수(합계)":0,
            "DOI 결측 수(합산)":0,
            "DOI 보강 수(합산)":0,
            "DOI 보강률":"0.00%",
            "ROR ID 결측 수(합산)":0,
            "ROR ID 보강 수(합산)":0,
            "ROR ID 보강률":"0.00%",
        }])
        if out_csv_path: df.to_csv(out_csv_path, index=False, encoding="utf-8-sig")
        return df

    tot = totals.copy()

    # -------------------------------
    # 🔥 기관정보 기준으로 ROR 지표 재정의
    # -------------------------------
    inst_missing, inst_recovered = _compute_inst_missing_and_recovered_for_final(final_csv_path)

    # 1) before/after 필드 재설정
    tot["ror_missing_before_extract"] = inst_missing
    tot["ror_missing_after_extract"]  = max(0, inst_missing - inst_recovered)

    # 2) summary에서 실제로 쓰이는 ROR ID 결측/보강 수 재정의
    #    - ROR ID 결측 수(합산)   = 보강 후 남은 결측 수
    #    - ROR ID 보강 수(합산)   = 보강된 문헌 수
    #    - 분모 (ROR ID 결측 + 보강) = 최초 결측 수(inst_missing)
    tot["ror_missing"]  = max(0, inst_missing - inst_recovered)  # after 기준
    tot["ror_enriched"] = max(0, inst_recovered)

    doi_denom = _ival(tot["doi_missing"]) + _ival(tot["doi_enriched"])
    ror_denom = _ival(tot["ror_missing"]) + _ival(tot["ror_enriched"])

    doi_rate = f"{(100.0*_ival(tot['doi_enriched'])/doi_denom):.2f}%" if doi_denom else "0.00%"
    ror_rate = f"{(100.0*_ival(tot['ror_enriched'])/ror_denom):.2f}%" if ror_denom else "0.00%"

    # 최초/최종 분리: json_rows 우선, 없으면 total_collected 폴백
    _first_rows = _ival(tot.get("json_rows", None))
    if _first_rows is None:
        _first_rows = _ival(tot.get("total_collected", 0))

    _start = _ival(tot.get("json_rows", tot["total_collected"]))
    _final = _ival(tot["final_csv_rows"])
    _ed = _ival(tot["authorships_removed"])
    _id = _ival(tot["id_pattern_removed"])
    _col = _ival(tot["col_mismatch_removed"])
    _check = _start - _ed - _id - _col - _final

    df = pd.DataFrame([{
        "최초 수집 논문 수": _start,
        "최종 수집 논문 수": _final,
        "Editorial Material 삭제 수": _ed,
        "ID 패턴 불일치 삭제 수": _id,
        "컬럼수 불일치 삭제 수": _col,
        "검증_차이(=0이어야 정상)": _check,
        "최종 CSV 행 수(합계)": _final,
        "DOI 결측 수(합산)": _ival(tot["doi_missing"]),
        "DOI 보강 수(합산)": _ival(tot["doi_enriched"]),
        "DOI 보강률": doi_rate,
        "ROR ID 결측 수(합산)": _ival(tot["ror_missing"]),
        "ROR ID 보강 수(합산)": _ival(tot["ror_enriched"]),
        "ROR ID 보강률": ror_rate,
    }])
    if out_csv_path:
        Path(out_csv_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv_path, index=False, encoding="utf-8-sig")
    return df


def run_pipeline(issns: List[str], year_start: int, year_end: int,
                 email: str = 's0124kw@gmail.com', include_only_with_abstract: bool = False,
                 make_html: bool = False):

    """
    전체 파이프라인 실행 함수
    issns: 저널 ISSN 리스트
    year_start, year_end: 연도 범위
    email: OpenAlex API용 이메일
    """

    """
    파일명 prefix 계산
    """
    config.email = email
    journal_prefixes = []
    for issn in issns:
        try:
            src = next(
                Sources().filter(issn=[issn])
                         .select(['display_name'])
                         .paginate(per_page=1)
            )
            display = src[0]['display_name'] if isinstance(src, list) else src['display_name']
        except Exception:
            display = issn  # fallback: ISSN 그대로 사용
        safe = re.sub(r'\W+', '_', display).strip('_')
        journal_prefixes.append(safe)
    prefix = "-".join(journal_prefixes)


    # 1️⃣ 논문 메타데이터 수집 및 DOI 보강 (real1)
    json_base = f"{prefix}_{year_start}_{year_end}"
    json_parts = [f"{json_base}_part{i+1}.json" for i in range(3)]
    json_merged = f"{json_base}.json"
    csv_file = f"{json_base}.csv"
    csv_ror = f"{json_base}_ror.csv"
    csv_ror_extract = f"{json_base}_ror_extract.csv"
    csv_ror_extract_name = f"{json_base}_ror_extract_name.csv"
    html_ror_extract_name_network = f"{json_base}_ror_extract_name_network.html"
    cache_file = Path("ror_cache.pkl")

    # 1. real1: 논문 수집 및 보강
    print("[1/7] 논문 메타데이터 수집 및 DOI 보강...")
    real1.main(issns=issns, year_start=year_start, year_end=year_end, email=email, prefix=prefix, include_only_with_abstract=include_only_with_abstract)

    # 2. real2: JSON → CSV 변환
    print("[2/7] JSON → CSV 변환...")
    real2.main(input_json=json_merged, output_csv=csv_file, prefix=prefix, year_start=year_start, year_end=year_end)

    # 3. real3: ROR 매핑
    print("[3/7] ROR 매핑...")
    asyncio.run(real3.process(
        input_csv=Path(csv_file),
        output_csv=Path(csv_ror),
        cache_file=cache_file,
        concurrency=20
    ))

    # 4. real4: ROR 추출
    print("[4/7] ROR 추출 및 통계...")
    real4.main(input_csv=csv_ror, output_csv=csv_ror_extract, prefix=prefix, year_start=year_start, year_end=year_end)

    # 5. real5: ROR ID → 기관명 매핑
    print("[5/7] ROR ID → 기관명 매핑...")
    real5.main(input_csv=csv_ror_extract, output_csv=csv_ror_extract_name, prefix=prefix, year_start=year_start, year_end=year_end)

    # 6. real6: 협력 네트워크 시각화 (HTML 생성)
    html_path = None
    if make_html:
        print("[6/7] 협력 네트워크 시각화...")
        html_path = real6.main(
            input_csv=csv_ror_extract_name,
            output_html=html_ror_extract_name_network,
            size_by="eigenvector",
            color_by="eigenvector"
        )
        if html_path:
            print(f"→ 시각화 파일 생성: {html_path}")


    print("[7/7] 전체 파이프라인 완료!")

    return csv_ror_extract_name, html_path


def make_html_from_csv(final_csv_path: str) -> str:
    """
    최종 CSV로 시각화를 2가지 버전(degree / eigenvector)으로 생성한 뒤,
    하나의 HTML에 두 뷰를 iframe으로 나란히 담아 반환(함수 시그니처/리턴타입 변경 없음).
    """
    if not final_csv_path.endswith("_ror_extract_name.csv"):
        raise ValueError("final_csv_path must end with _ror_extract_name.csv")

    base = final_csv_path[:-len("_ror_extract_name.csv")]

    # 1) 개별 시각화 HTML 생성(같은 폴더에 유지)
    html_degree = f"{base}_degree_network.html"
    html_eigen  = f"{base}_eigenvector_network.html"

    # 권장 조합: 크기=degree / 색=eigenvector  &  크기=eigenvector / 색=degree
    real6.main(
        input_csv=final_csv_path,
        output_html=html_degree,
        size_by="degree",
        color_by="eigenvector"
    )
    real6.main(
        input_csv=final_csv_path,
        output_html=html_eigen,
        size_by="eigenvector",
        color_by="degree"
    )

    # 2) 같은 폴더의 두 HTML을 iframe으로 불러오는 래퍼 생성
    deg_name = Path(html_degree).name
    eig_name = Path(html_eigen).name

    output_html = final_csv_path.replace(
        "_ror_extract_name.csv",
        "_ror_extract_name_network.html"
    )

    wrapper = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Collaboration Networks (Degree & Eigenvector)</title>
  <style>
    body {{ background:#111; color:#eaeaea; font-family:system-ui, sans-serif; }}
    .row {{ display:flex; gap:12px; flex-wrap:wrap; padding:12px; }}
    .panel {{ flex:1 1 48%; min-width:380px; border:1px solid #333; border-radius:8px; background:#151515; }}
    .panel h3 {{ margin:8px 12px; font-weight:600; }}
    iframe {{ width:100%; height:820px; border:0; background:#111; border-top:1px solid #333; }}
  </style>
</head>
<body>
  <div class="row">
    <div class="panel">
      <h3>Degree-based view (size=degree, color=eigenvector)</h3>
      <iframe src="{deg_name}"></iframe>
    </div>
    <div class="panel">
      <h3>Eigenvector-based view (size=eigenvector, color=degree)</h3>
      <iframe src="{eig_name}"></iframe>
    </div>
  </div>
</body>
</html>
"""
    Path(output_html).write_text(wrapper, encoding="utf-8")
    return output_html

def make_html_string_from_csv(final_csv_path: str, size_by: str, color_by: str) -> str:
    """
    최종 CSV에서 네트워크 HTML 문자열 생성(파일 저장 없음).
    - size_by: "degree" 또는 "eigenvector"
    - color_by: "eigenvector" 또는 "degree"
    """
    res = real6.main(
        input_csv=final_csv_path,
        output_html=None,      # ← 파일 저장 안 함
        size_by=size_by,
        color_by=color_by
    )

    # 안전장치: 혹시 경로 문자열이 돌아오면 파일을 읽어서 HTML 문자열로 치환
    from pathlib import Path as _Path
    if isinstance(res, str):
        s = res.strip().lower()
        if s.startswith("<!doctype") or s.startswith("<html"):
            return res
        if res.endswith(".html") and _Path(res).exists():
            return _Path(res).read_text(encoding="utf-8")
    raise RuntimeError("HTML 생성 실패: 반환값이 비었거나 HTML이 아닙니다.")


# 저장 전략 설정
USE_JOURNAL_NAME_SLUG: bool = True
LOCAL_WORKDIR = Path("workdir_tmp")
LOCAL_WORKDIR.mkdir(parents=True, exist_ok=True)

_slug_rx = re.compile(r"[^a-zA-Z0-9]+")
def _to_slug(name: str) -> str:
    return _slug_rx.sub("_", name.strip().lower()).strip("_")

_JSLUG_CACHE: dict[str, str] = {}
def _resolve_journal_name_slug(issn: str) -> str:
    """ISSN -> OpenAlex display_name -> 파일시스템 안전 슬러그"""
    if issn in _JSLUG_CACHE:
        return _JSLUG_CACHE[issn]
    try:
        src = next(
            Sources().filter(issn=[issn])
                    .select(['display_name'])
                    .paginate(per_page=1)
        )
        display = src[0]['display_name'] if isinstance(src, list) else src['display_name']
        base_name = display
    except Exception:
        base_name = issn
    slug = _to_slug(base_name)
    _JSLUG_CACHE[issn] = slug
    return slug

def _key_name_for(issn: str) -> str:
    """파일명/폴더명에 쓸 키(저널명 슬러그 또는 ISSN)"""
    # ✅ 대표키가 이미 정해졌다면 무조건 그것만 사용
    if 'FORCE_JOURNAL_KEY' in globals() and FORCE_JOURNAL_KEY:
        return FORCE_JOURNAL_KEY
    return _resolve_journal_name_slug(issn) if USE_JOURNAL_NAME_SLUG else issn

# ----- Secrets/ENV에서 Drive 인증/폴더 정보 읽기 -----

def _get_gdrive_root_folder_id() -> Optional[str]:
    """
    Streamlit Cloud Secrets 또는 환경변수에서 루트 폴더 ID 읽기
    - secrets: GDRIVE_FOLDER_ID
    - env    : GDRIVE_FOLDER_ID
    """
    if st is not None:
        try:
            return st.secrets["GDRIVE_FOLDER_ID"]
        except Exception:
            pass
    return os.getenv("GDRIVE_FOLDER_ID")


def _get_service_account_info() -> Optional[dict]:
    """
    ✅ 오직 Streamlit secrets의 [gcp_service_account]만 지원
       (필요 시 ENV GDRIVE_SA_JSON JSON 문자열은 보조용)
    - 문자열/딕셔너리 모두 허용
    - private_key 의 '\\n' → 실제 개행으로 치환
    """
    raw = None
    # 1) Streamlit secrets
    if st is not None:
        try:
            raw = st.secrets["gcp_service_account"]
        except Exception:
            print("[GDRIVE] gcp_service_account not found in st.secrets")
            raw = None
    # 2) (선택) 환경변수 JSON 보조
    if raw is None:
        raw = os.getenv("GDRIVE_SA_JSON")
        if raw is None:
            print("[GDRIVE] GDRIVE_SA_JSON not found in env")
    if raw is None:
        return None

    # 딕셔너리면 그대로 사용
    if isinstance(raw, Mapping):
        sa = dict(raw)
    else:
        s = str(raw).strip()
        # JSON 먼저 시도
        try:
            sa = _json.loads(s)
        except Exception:
            # 일부 환경에서 단일따옴표 dict 문자열로 들어온 경우
            try:
                sa = _ast.literal_eval(s)
                if not isinstance(sa, Mapping):
                    raise ValueError("literal_eval did not return a mapping")
                sa = dict(sa)
            except Exception as e:
                print("[GDRIVE] Could not parse gcp_service_account string:", repr(e))
                print("[GDRIVE] First 120 chars:", s[:120])
                return None

    # private_key 개행 정규화
    pk = sa.get("private_key")
    if not pk:
        print("[GDRIVE] private_key missing in gcp_service_account")
        return None
    # private_key가 환경/Secrets에 '\\n' 형태로 들어올 때 실제 개행으로 변환
    pk = sa.get("private_key")
    if pk:
        try:
            sa["private_key"] = str(pk).replace("\\n", "\n")
        except Exception:
            sa["private_key"] = pk
    return sa


def _gdrive_client():
    # ✅ oauth2client 기반으로 변경
    from oauth2client.service_account import ServiceAccountCredentials
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive

    sa_info = _get_service_account_info()
    if not sa_info:
        raise RuntimeError("Google Drive 서비스계정 정보가 없습니다. secrets 또는 ENV를 확인하세요.")

    scopes = ['https://www.googleapis.com/auth/drive']
    # 핵심: google-auth 대신 oauth2client로 크리덴셜 생성
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scopes=scopes)

    gauth = GoogleAuth()
    gauth.credentials = creds
    gauth.Authorize()  # ← oauth2client 크리덴셜은 authorize() 지원
    drive = GoogleDrive(gauth)
    return drive


def _find_folder(drive: GoogleDrive, parent_id: str, name: str) -> Optional[str]:
    q = f"'{parent_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and title='{name}'"
    lst = drive.ListFile({'q': q, 'includeItemsFromAllDrives': True, 'supportsAllDrives': True}).GetList()
    return lst[0]['id'] if lst else None

def _ensure_folder(drive: GoogleDrive, parent_id: str, name: str) -> str:
    fid = _find_folder(drive, parent_id, name)
    if fid:
        return fid
    f = drive.CreateFile({'title': name, 'parents':[{'id': parent_id}],
                          'mimeType': 'application/vnd.google-apps.folder'})
    f.Upload()
    return f['id']

def _find_file(drive: GoogleDrive, parent_id: str, name: str) -> Optional[str]:
    q = f"'{parent_id}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder' and title='{name}'"
    lst = drive.ListFile({'q': q, 'includeItemsFromAllDrives': True, 'supportsAllDrives': True}).GetList()
    return lst[0]['id'] if lst else None

def _download_file(drive: GoogleDrive, file_id: str, local_path: Path):
    f = drive.CreateFile({'id': file_id})
    local_path.parent.mkdir(parents=True, exist_ok=True)
    f.GetContentFile(str(local_path))

def _upload_file(drive: GoogleDrive, parent_id: str, local_path: Path, name: Optional[str] = None):
    """동일 이름이 있으면 덮어쓰기(업서트)"""
    name = name or local_path.name
    q = (
        f"'{parent_id}' in parents and trashed=false "
        f"and mimeType!='application/vnd.google-apps.folder' and title='{name}'"
    )
    exist = drive.ListFile({
        'q': q, 'includeItemsFromAllDrives': True, 'supportsAllDrives': True
    }).GetList()

    if exist:
        f = drive.CreateFile({'id': exist[0]['id']})
        f.SetContentFile(str(local_path))
        f.Upload(param={'supportsAllDrives': True})
        return f['id']

    f = drive.CreateFile({'title': name, 'parents':[{'id': parent_id}]})
    f.SetContentFile(str(local_path))
    f.Upload(param={'supportsAllDrives': True})
    return f['id']


def _gdrive_locate_metric_by_key(key: str, year: int):
    """
    ROOT / <key> / <year> / <key>_<year>_<year>_metrics.csv 를 찾아 file_id를 반환.
    (ISSN 없이 key(slug)만 가지고 찾기)
    """
    root_id = _get_gdrive_root_folder_id()
    if not root_id:
        raise RuntimeError("GDRIVE_FOLDER_ID가 설정되지 않았습니다.")
    drive = _gdrive_client()

    # 폴더 찾아가기
    key_folder = _ensure_folder(drive, root_id, key)
    year_folder = _ensure_folder(drive, key_folder, str(year))

    metrics_name = f"{key}_{year}_{year}_metrics.csv"
    file_id = _find_file(drive, year_folder, metrics_name)
    return drive, year_folder, metrics_name, file_id


# ----- issn/year 원격 파일 위치 -----
def _gdrive_locate_piece(issn: str, year: int):
    """
    ROOT / <key> / <year> / <key>_<year>_ror_extract_name.csv
    """
    root_id = _get_gdrive_root_folder_id()
    if not root_id:
        raise RuntimeError("GDRIVE_FOLDER_ID가 설정되지 않았습니다.")
    key = _key_name_for(issn)
    drive = _gdrive_client()
    key_folder = _ensure_folder(drive, root_id, key)
    year_folder = _ensure_folder(drive, key_folder, str(year))
    fname = f"{key}_{year}_ror_extract_name.csv"
    file_id = _find_file(drive, year_folder, fname)
    return drive, key, key_folder, year_folder, fname, file_id


def _gdrive_download_metric_by_key_exact_path(key: str, year: int, local_path: Path) -> bool:
    """
    예상 경로(키/연도 폴더)에서 metrics 파일을 찾아 로컬로 저장.
    기본: ROOT/{key}/{year}/
    Fallback: ROOT/storage/{key}/{year}/   (공유드라이브 구조 지원)
    """
    try:
        root_id = _get_gdrive_root_folder_id()
        if not root_id:
            return False
        drive = _gdrive_client()

        def _locate_year_folder():
            # 1) ROOT/{key}/{year}
            key_id = _find_folder(drive, root_id, key)
            if key_id:
                yid = _find_folder(drive, key_id, str(year))
                if yid:
                    return yid
            # 2) ROOT/storage/{key}/{year}
            storage_id = _find_folder(drive, root_id, "storage")
            if storage_id:
                key2 = _find_folder(drive, storage_id, key)
                if key2:
                    yid2 = _find_folder(drive, key2, str(year))
                    if yid2:
                        return yid2
            return None

        year_folder_id = _locate_year_folder()
        if not year_folder_id:
            return False

        metrics_name = f"{key}_{year}_{year}_metrics.csv"
        file_id = _find_file(drive, year_folder_id, metrics_name)
        if not file_id:
            return False

        _download_file(drive, file_id, local_path)
        return local_path.exists()
    except Exception as e:
        print(f"[WARN] _gdrive_download_metric_by_key_exact_path: {key=} {year=} {e}")
        return False

def _gdrive_download_metric_by_name_search(key: str, year: int, local_path: Path) -> bool:
    """
    Google Drive 전체에서 파일명으로 검색(팀드라이브 포함).
    1) 정확 일치(title = ...)
    2) 실패 시 부분 일치(title contains ...)로 완화
    """
    try:
        root_id = _get_gdrive_root_folder_id()
        if not root_id:
            return False
        drive = _gdrive_client()

        metrics_name = f"{key}_{year}_{year}_metrics.csv"

        # 1) 정확 일치
        q_eq = (
            f"title = '{metrics_name}' and trashed = false "
            f"and mimeType != 'application/vnd.google-apps.folder'"
        )
        lst = drive.ListFile({
            'q': q_eq,
            'includeItemsFromAllDrives': True,
            'supportsAllDrives': True
        }).GetList()

        # 2) 부분 일치(정확 일치가 없을 때만)
        if not lst:
            # 예: key 일부 + 연도 + 'metrics.csv'만 맞아도 회수
            #    title contains 'journal_of_hydraulic_engineering_2024' and title contains 'metrics.csv'
            q_ct = (
                f"title contains '{key}_{year}_{year}' and "
                f"title contains 'metrics.csv' and "
                f"trashed = false and mimeType != 'application/vnd.google-apps.folder'"
            )
            lst = drive.ListFile({
                'q': q_ct,
                'includeItemsFromAllDrives': True,
                'supportsAllDrives': True
            }).GetList()

        if not lst:
            return False

        file_id = lst[0]['id']  # 첫 일치 항목 사용
        _download_file(drive, file_id, local_path)
        return local_path.exists()
    except Exception as e:
        print(f"[WARN] _gdrive_download_metric_by_name_search: {key=} {year=} {e}")
        return False



from io import StringIO

def _gdrive_read_metric_csv_by_key_exact_path_to_df(key: str, year: int):
    """
    정확 경로에서 metrics를 메모리로 읽기
      기본: ROOT/{key}/{year}/
      Fallback: ROOT/storage/{key}/{year}/
    """
    import pandas as pd
    from io import StringIO

    try:
        root_id = _get_gdrive_root_folder_id()
        if not root_id:
            return None
        drive = _gdrive_client()

        def _locate_year_folder():
            # 1) ROOT/{key}/{year}
            key_id = _find_folder(drive, root_id, key)
            if key_id:
                yid = _find_folder(drive, key_id, str(year))
                if yid:
                    return yid
            # 2) ROOT/storage/{key}/{year}
            storage_id = _find_folder(drive, root_id, "storage")
            if storage_id:
                key2 = _find_folder(drive, storage_id, key)
                if key2:
                    yid2 = _find_folder(drive, key2, str(year))
                    if yid2:
                        return yid2
            return None

        year_folder_id = _locate_year_folder()
        if not year_folder_id:
            return None

        metrics_name = f"{key}_{year}_{year}_metrics.csv"
        file_id = _find_file(drive, year_folder_id, metrics_name)
        if not file_id:
            return None

        f = drive.CreateFile({'id': file_id})
        csv_text = f.GetContentString(mimetype='text/csv')
        return pd.read_csv(StringIO(csv_text), dtype=str)
    except Exception as e:
        print(f"[WARN] _gdrive_read_metric_csv_by_key_exact_path_to_df: {key=} {year=} {e}")
        return None

def _gdrive_read_metric_csv_by_name_search_to_df(key: str, year: int):
    """Drive 전체에서 파일명으로 찾아 DF로 읽기 (부분 일치까지 허용)"""
    import pandas as pd
    try:
        root_id = _get_gdrive_root_folder_id()
        if not root_id:
            return None
        drive = _gdrive_client()
        metrics_name = f"{key}_{year}_{year}_metrics.csv"

        # 1) 정확 일치
        q = f"title = '{metrics_name}' and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
        lst = drive.ListFile({'q': q, 'includeItemsFromAllDrives': True, 'supportsAllDrives': True}).GetList()

        # 2) 부분 일치 (정확 일치 없을 때만)
        if not lst:
            q2 = (
                f"title contains '{key}_{year}_{year}' and title contains 'metrics.csv' "
                f"and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
            )
            lst = drive.ListFile({'q': q2, 'includeItemsFromAllDrives': True, 'supportsAllDrives': True}).GetList()

        if not lst:
            return None
        f = drive.CreateFile({'id': lst[0]['id']})
        csv_text = f.GetContentString(mimetype='text/csv')
        return pd.read_csv(StringIO(csv_text), dtype=str)
    except Exception as e:
        print(f"[WARN] _gdrive_read_metric_csv_by_name_search_to_df: {key=} {year=} {e}")
        return None

def _iter_metrics_dfs_from_final(final_csv_path: str):
    """최종 CSV 파일명에서 (keys, years) 복원 → 각 (key, year)의 metrics DF를 메모리로 yield"""
    from pathlib import Path as _Path
    import re as _re

    base_dir = _Path(final_csv_path).parent
    keys, y1, y2 = _parse_keys_years_from_final(final_csv_path)

    # 키 정규화(드라이브 폴더/파일 명과 어긋나지 않게 단일 슬러그 규칙)
    def _norm_key(k: str) -> str:
        s = (k or "").strip().lower()
        s = s.replace("-", "_")
        s = _re.sub(r"\s+", "_", s)
        s = _re.sub(r"_+", "_", s)
        return s
    keys = [_norm_key(k) for k in keys]

    for key in keys:
        for y in range(y1, y2 + 1):
            # 1) 로컬(있으면 바로 읽기: 저장 용량 증가 없음)
            nested = base_dir / key / str(y) / f"{key}_{y}_{y}_metrics.csv"
            flat   = base_dir / f"{key}_{y}_{y}_metrics.csv"
            df = None
            for cand in (nested, flat):
                if cand.exists():
                    try:
                        tmp = pd.read_csv(cand, dtype=str)

                        # --- 헤더/인덱스 정규화 ---
                        # 1) 'key','value'가 없다면, 첫 두 컬럼을 key/value로 간주
                        if not {"key","value"}.issubset(set(tmp.columns)):
                            cols = list(tmp.columns)
                            # (a) 두 컬럼 이상 → 앞의 두 개만 사용
                            if len(cols) >= 2:
                                tmp = tmp.rename(columns={cols[0]: "key", cols[1]: "value"})
                            # (b) 한 컬럼 + 인덱스에 키가 있는 케이스 → 인덱스를 key로 승격
                            elif len(cols) == 1:
                                only = cols[0]
                                # 인덱스가 의미 있고, 단일 값 컬럼이 value일 가능성
                                tmp = tmp.reset_index().rename(columns={"index":"key", only:"value"})
                            else:
                                tmp = None

                        # 2) 'key','value'가 생겼다면 필요한 두 컬럼만 유지
                        if tmp is not None and {"key","value"}.issubset(set(tmp.columns)):
                            df = tmp[["key","value"]].copy()
                            break
                        else:
                            # 그래도 안 맞으면 스킵
                            df = None

                    except Exception as e:
                        print(f"[WARN] local metrics read failed: {cand} {e}")

            # 2) Drive 메모리 읽기 (정확 경로 → 전역 검색)
            if df is None:
                df = _gdrive_read_metric_csv_by_key_exact_path_to_df(key, y)
            if df is None:
                df = _gdrive_read_metric_csv_by_name_search_to_df(key, y)

            if df is not None and not df.empty and "key" in df.columns and "value" in df.columns:
                yield key, y, df
            else:
                print(f"[WARN] metrics not found for key={key}, year={y}")


def _gdrive_piece_exists(issn: str, year: int) -> bool:
    try:
        _, _, _, _, _, file_id = _gdrive_locate_piece(issn, year)
        return file_id is not None
    except Exception:
        return False

def _gdrive_download_piece(issn: str, year: int, local_path: Path) -> bool:
    try:
        drive, key, _, year_folder, _, file_id = _gdrive_locate_piece(issn, year)
        if not file_id:
            return False

        # 1) 최종 CSV 내려받기
        _download_file(drive, file_id, local_path)

        # 2) 보조 파일들 찾기 위한 헬퍼
        def _find_file(drive, parent_id, name):
            for f in drive.ListFile({'q': f"'{parent_id}' in parents and trashed=false"}).GetList():
                if f['title'] == name:
                    return f['id']
            return None

        # 2-1) metrics.csv
        metrics_name = f"{key}_{year}_{year}_metrics.csv"
        m_id = _find_file(drive, year_folder, metrics_name)
        if m_id:
            _download_file(drive, m_id, local_path.parent / metrics_name)

        # 2-2) run_log_YYYY.txt
        log_name = f"run_log_{year}.txt"
        log_id = _find_file(drive, year_folder, log_name)
        if log_id:
            _download_file(drive, log_id, local_path.parent / log_name)

        # 2-3) mapped_affiliations_YYYY.csv
        mapped_name = f"mapped_affiliations_{year}.csv"
        map_id = _find_file(drive, year_folder, mapped_name)
        if map_id:
            _download_file(drive, map_id, local_path.parent / mapped_name)

        return True
    except Exception:
        return False

def _gdrive_upload_piece(issn: str, year: int, local_path: Path):
    drive, key, _, year_folder, fname, _ = _gdrive_locate_piece(issn, year)

    # 1) 최종 CSV 업로드
    _upload_file(drive, year_folder, local_path, fname)

    # 2) metrics.csv가 로컬 연도 폴더에 있으면 함께 업로드
    metrics_csv = local_path.parent / f"{key}_{year}_{year}_metrics.csv"
    if metrics_csv.exists():
        _upload_file(drive, year_folder, metrics_csv, metrics_csv.name)

    # 3) run_log_YYYY.txt 업로드
    log_file = local_path.parent / f"run_log_{year}.txt"
    if log_file.exists():
        _upload_file(drive, year_folder, log_file, log_file.name)

    # 4) mapped_affiliations_YYYY.csv 업로드
    mapped_csv = local_path.parent / f"mapped_affiliations_{year}.csv"
    if mapped_csv.exists():
        _upload_file(drive, year_folder, mapped_csv, mapped_csv.name)


# ======================================================================
# 연·저널 단위 저장/재사용 (Drive 사용)
# ======================================================================

# [# ADDED] ISSN 정규화
_issn_rx = re.compile(r"^\d{4}-\d{3}[\dxX]$")

def _normalize_issn_list(issns: List[str]) -> List[str]:
    norm = []
    for s in issns:
        if not s:
            continue
        s = s.strip()
        if "-" not in s and len(s) == 8:
            s = s[:4] + "-" + s[4:]
        if _issn_rx.match(s):
            norm.append(s.upper())
        else:
            print(f"[WARN] 잘못된 ISSN 형식 건너뜀: {s!r}")
    # 입력 순서 유지 중복 제거
    seen, out = set(), []
    for t in norm:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

# [# ADDED] 로컬 임시(조각 다운로드/생성 위치)
def _local_piece_path(issn: str, year: int) -> Path:
    key = _key_name_for(issn)
    return LOCAL_WORKDIR / key / str(year) / f"{key}_{year}_ror_extract_name.csv"


# --- NEW: run_log → mapped_affiliations 생성용 헬퍼들 ---

def _normalize_aff(s: str) -> str:
    """
    count.py에서 쓰던 normalize 규칙과 동일하게 맞춤:
    - 앞뒤 공백 제거
    - 끝의 점/쉼표/탭 제거
    - 연속 공백을 하나로
    """
    s = s.strip()
    s = s.rstrip(" .,\t")
    s = " ".join(s.split())
    return s


def _build_mapped_affiliations_from_log(log_path: Path, out_csv: Path) -> None:
    """
    run_log_YYYY.txt 안의 [ROR_RESULT_QUERY] 로그를 읽어
    매핑 성공한 쿼리 문자열을 normalize 후
    normalized_affiliation 단일 컬럼 CSV로 저장.
    """
    import re, csv

    if not log_path.exists():
        print(f"[_build_mapped_affiliations_from_log] 로그 파일 없음: {log_path}")
        return

    try:
        lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as e:
        print(f"[_build_mapped_affiliations_from_log] 로그 읽기 실패: {e}")
        return

    # count.py에서 쓰던 패턴과 동일
    ror_pattern = re.compile(
        r'\[ROR_RESULT_QUERY\]\s*([\'"])(.*?)\1\s*(?:→|->)\s*([\'"])(.*?)\3'
    )
    # group 2 → query_str, group 4 → ror_id

    success_norms = set()
    for line in lines:
        m = ror_pattern.search(line)
        if not m:
            continue
        query_str = m.group(2)
        ror_id = m.group(4)
        if not ror_id:
            continue  # 빈 따옴표 = 매핑 실패
        norm = _normalize_aff(query_str)
        if norm:
            success_norms.add(norm)

    if not success_norms:
        print(f"[_build_mapped_affiliations_from_log] 매핑 성공 쿼리가 없어 CSV 생략: {log_path}")
        return

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["normalized_affiliation"])
        for val in sorted(success_norms):
            w.writerow([val])

    print(f"[_build_mapped_affiliations_from_log] {len(success_norms)}개 기관명을 {out_csv.name}에 기록")


# [# ADDED] 연·저널 1건 처리 (있으면 다운로드, 없으면 생성 후 업로드)
# [# ADDED] 연·저널 1건 처리 (있으면 다운로드, 없으면 생성 후 업로드)
def _run_one_piece(issn: str, year: int, email: str,
                   include_only_with_abstract: bool = False) -> Path:
    local_out = _local_piece_path(issn, year)
    
    # 이미 완성된 조각이 있고, 강제 덮어쓰기 옵션이 없으면 그대로 사용
    if local_out.exists() and not bool(os.environ.get('OVERWRITE_LOCAL_PIECE')):
        return local_out

    try:
        # 1) 원격 존재 시 → 다운로드
        if _gdrive_piece_exists(issn, year):
            if _gdrive_download_piece(issn, year, local_out):
                return local_out

        # 2) 없으면 생성
        local_out.parent.mkdir(parents=True, exist_ok=True)
        prefix = _key_name_for(issn)

        config.email = email
        real1.main(
            issns=[issn],
            year_start=year,
            year_end=year,
            email=email,
            prefix=prefix,
            include_only_with_abstract=include_only_with_abstract,
            anchor_path=str(local_out),
        )

        json_merged    = f"{prefix}_{year}_{year}.json"
        tmp_csv        = f"{prefix}_{year}_{year}.csv"
        tmp_csv_ror    = f"{prefix}_{year}_{year}_ror.csv"
        tmp_csv_ror_ex = f"{prefix}_{year}_{year}_ror_extract.csv"
        tmp_csv_name   = f"{prefix}_{year}_{year}_ror_extract_name.csv"

        real2.main(
            input_json=json_merged,
            output_csv=tmp_csv,
            prefix=prefix,
            year_start=year,
            year_end=year,
            anchor_path=str(local_out),
        )

        # real3
        log_file = local_out.parent / f"run_log_{year}.txt"

        # 이전 로그가 있다면 지우고 새로 생성
        try:
            if log_file.exists():
                log_file.unlink()
        except Exception:
            pass

        print(f"[INFO] ROR 매핑 로그를 {log_file} 에 기록합니다.")

        with log_file.open("w", encoding="utf-8") as lf, redirect_stdout(lf):
            asyncio.run(
                real3.process(
                    input_csv=Path(tmp_csv),
                    output_csv=Path(tmp_csv_ror),
                    cache_file=Path("ror_cache.pkl"),
                    concurrency=20,
                    anchor_path=str(local_out),
                )
            )

        # 3-1. run_log로부터 mapped_affiliations_{year}.csv 생성
        mapped_csv = local_out.parent / f"mapped_affiliations_{year}.csv"
        _build_mapped_affiliations_from_log(log_file, mapped_csv)

        real4.main(
            input_csv=tmp_csv_ror,
            output_csv=tmp_csv_ror_ex,
            prefix=prefix,
            year_start=year,
            year_end=year,
            anchor_path=str(local_out),
        )

        real5.main(
            input_csv=tmp_csv_ror_ex,
            output_csv=tmp_csv_name,
            prefix=prefix,
            year_start=year,
            year_end=year,
        )

        # 최종 조각: 로컬 표준 위치로 이동
        Path(tmp_csv_name).parent.mkdir(parents=True, exist_ok=True)
        Path(tmp_csv_name).replace(local_out)

        # 2-1) 이 연도의 metrics.csv에 기관정보 보완 수를 반영하고,
        #      ror_missing_after_extract는 제거한다.
        metrics_path = local_out.parent / f"{prefix}_{year}_{year}_metrics.csv"
        _update_metrics_ror_for_piece(metrics_path, local_out)

        # 3) 원격 업로드
        _gdrive_upload_piece(issn, year, local_out)

        # 연도 단위 처리 후 잠깐 쉬기(백오프)
        # 429 에러 완화(ROR 쿼리 몰리면 발생)
        import time, random
        time.sleep(5 + random.uniform(0, 0.5))

        return local_out

    except Exception:
        # ✅ 어떤 단계에서든 실패하면, 깨진 조각 CSV는 남기지 않도록 정리
        if local_out.exists():
            try:
                local_out.unlink()
            except Exception:
                # 삭제도 실패하면 그냥 무시하고 원래 예외만 다시 올림
                pass
        raise

# [# ADDED] 조각 병합 → 최종 CSV
def _collect_merge(issns: List[str], year_start: int, year_end: int) -> Path:
    issns = _normalize_issn_list(issns)
    piece_paths: List[Path] = []
    keys: List[str] = []

    for issn in issns:
        key = _key_name_for(issn)
        keys.append(key)
        for y in range(year_start, year_end + 1):
            local_piece = _local_piece_path(issn, y)
            if local_piece.exists():
                piece_paths.append(local_piece)
            else:
                if _gdrive_download_piece(issn, y, local_piece):
                    piece_paths.append(local_piece)

    if not piece_paths:
        raise FileNotFoundError("선택 범위의 조각 CSV가 없습니다.")

    dfs = []
    for p in piece_paths:
        p = Path(p)
        if not p.exists():
            print(f"[_collect_merge] 경고: 조각 파일 없음: {p}")
            continue

        # 1) 우선: C 엔진으로 빠르고 견고하게 시도 (low_memory=False 허용)
        try:
            df_piece = pd.read_csv(
                p,
                encoding="utf-8-sig",
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                escapechar='\\',
                low_memory=False
            )
            dfs.append(df_piece)
            continue
        except ParserError as e:
            print(f"[_collect_merge] C engine ParserError for {p}: {e} -> retry with python engine")
        except Exception as e:
            print(f"[_collect_merge] C engine exception for {p}: {e} -> retry with python engine")

        # 2) C 엔진 실패 시: python 엔진으로 재시도 (low_memory 옵션 없음)
        try:
            df_piece = pd.read_csv(
                p,
                engine="python",
                encoding="utf-8-sig",
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                escapechar='\\',
                on_bad_lines='skip' # 기존에는 error로 했음
            )
            dfs.append(df_piece)
            continue
        except Exception as e:
            print(f"[_collect_merge] python engine read failed for {p}: {e} -> falling back to csv.DictReader")

        # 3) 최후의 안전망: csv.DictReader로 무조건 읽어서 모든 행을 확보 (누락 방지)
        try:
            rows = []
            with open(p, 'r', encoding='utf-8-sig', newline='') as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    rows.append(row)
            df_piece = pd.DataFrame(rows)
            dfs.append(df_piece)
            print(f"[_collect_merge] csv.DictReader fallback succeeded for {p} (rows={len(df_piece)})")
        except Exception as e2:
            print(f"[_collect_merge] csv.DictReader fallback also failed for {p}: {e2}")
            raise
    # 병합
    if not dfs:
        merged = pd.DataFrame()
    else:
        merged = pd.concat(dfs, ignore_index=True)



    # # [# ADDED] 중복 제거(가능하면 DOI 기준)                      final.ipynb와 app.py의 결과파일이 동일할 수 있도록 수정
    # if "doi" in merged.columns:
    #     merged = merged.drop_duplicates(subset=["doi"])
    # else:
    #     merged = merged.drop_duplicates()

    # 보기 좋은 열 순서(선택)
    preferred = [c for c in ["title", "doi", "published_year", "host_venue_issn_l",
                             "institution_name", "ror_id"] if c in merged.columns]
    merged = merged[[*preferred, *[c for c in merged.columns if c not in preferred]]]

    merged_name = f"{'-'.join(keys)}_{year_start}_{year_end}_ror_extract_name.csv"
    out_path = LOCAL_WORKDIR / merged_name
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


# ======================================================================
# [엔드포인트] run_pipeline_cached — app.py가 호출 (Drive 영구 저장)
# ======================================================================
def run_pipeline_cached(issns: List[str], year_start: int, year_end: int,
                        email: str = 's0124kw@gmail.com',
                        include_only_with_abstract: bool = False,
                        make_html: bool = False,
                        base_dir: Path = Path("storage")):
    """
    1) 각 저널×연도 조각이 있으면 재사용, 없으면 생성 후 Drive 업로드
    2) 조각들을 모아 로컬에서 최종 CSV 병합
    3) (옵션) HTML 생성
    주의: base_dir 인자는 호환성만 유지(Drive 사용으로 무시)
    """
    issns = _normalize_issn_list(issns)

    # 이전 실행의 잔재 제거 (전역 강제 고정 사용 안함)
    global FORCE_JOURNAL_KEY
    FORCE_JOURNAL_KEY = None

    for issn in issns:
        for y in range(int(year_start), int(year_end) + 1):
            try:
                _run_one_piece(issn, y, email, include_only_with_abstract)
            except Exception as e:
                import traceback, sys
                # 에러 로깅: 어떤 ISSN/연도에서 실패했는지 명확히 남김
                print(f"[run_pipeline_cached] ERROR processing ISSN={issn}, year={y}: {e}", file=sys.stderr)
                traceback.print_exc()
                # (선택) 실패시 placeholder *_metrics.csv를 생성할 수도 있음(요약 집계 완성을 위해)
                # continue 하여 다음 연도/ISSN으로 진행
                continue

    final_csv_path = _collect_merge(issns, int(year_start), int(year_end))

    # per-year metrics.csv 합산 -> Summary 반영
    final_path = Path(final_csv_path)
    summary_path = final_path.with_name(final_path.stem + "_ror_extract_name_summary.csv")
    build_summary_from_metrics_for_final(str(final_csv_path), str(summary_path))

    html_path = None
    if make_html:
        html_path = make_html_from_csv(str(final_csv_path))

    return str(final_csv_path), html_path


# ======================================================================
# (옵션) 로컬 단독 테스트
# ======================================================================
if __name__ == "__main__":
    example_issns = ['0043-1354','0011-9164','0733-9429']
    out_csv, _ = run_pipeline_cached(
        issns=example_issns,
        year_start=2017, year_end=2019,
        email='Your Email Here',
        include_only_with_abstract=False,
        make_html=False
    )
    print("FINAL:", out_csv)