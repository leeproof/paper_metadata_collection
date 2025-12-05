import os
os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"] = "none"

import streamlit as st
import streamlit.components.v1 as components
import nest_asyncio
import pandas as pd
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from io import BytesIO
import zipfile
import json
import ast
import csv
import re
import io


def _compute_inst_missing_and_recovered(final_csv_path: str):
    """
    반환:
      기관정보 기준 결측 수, 기관정보 보완 수
    """
    df = pd.read_csv(final_csv_path, dtype={"authorships": str}, encoding="utf-8-sig")

    # 1) 기관정보 기준 결측 수: authorships 안에 ROR URL이 하나도 없는 행
    rx_ror = re.compile(r"https?://ror\.org/[0-9a-z]+", re.I)
    has_ror = df["authorships"].astype(str).apply(lambda s: bool(rx_ror.search(s)))
    mask_missing = ~has_ror
    df_missing = df[mask_missing].copy()
    inst_missing = int(mask_missing.sum())

    # 2) 기관정보 보완 수: 위 missing 행들 중, 매핑된 기관명이 한 개라도 들어 있는 행
    mapped_affs = _load_mapped_affiliations_for_final(final_csv_path)
    if not mapped_affs or inst_missing == 0:
        return inst_missing, 0  # 보완 수 0

    def _has_any_mapped(cell: str) -> bool:
        if not isinstance(cell, str):
            cell = str(cell)
        for aff in mapped_affs:
            if aff and aff in cell:
                return True
        return False

    recovered_mask = df_missing["authorships"].apply(_has_any_mapped)
    inst_recovered = int(recovered_mask.sum())

    return inst_missing, inst_recovered


# ------------------- START: Streamlit UI 진단 함수 -------------------
def diag_metrics_ui():
    """Streamlit에서 실행할 수 있는 진단 UI.
    - 프로젝트 루트(.)에서 *_metrics.csv, *metrics_aggregated*.csv 검색
    - 폴더별 개수, 샘플 파일명, 샘플 파일 내용(최대 40행), 그리고 코드내 FORCE_JOURNAL_KEY 위치 검색 결과 출력
    """
    import textwrap

    st.subheader("진단 도구: metrics 파일, FORCE_JOURNAL_KEY 점검")

    base = Path(".")

    # 1) 파일 수 및 폴더별 개수
    metrics_files = sorted(base.rglob("*_metrics.csv"))
    st.write(f"전체 *_metrics.csv 파일 수: **{len(metrics_files)}**")

    # 폴더별 집계
    folders = {}
    for p in metrics_files:
        folders.setdefault(str(p.parent), []).append(p)
    if folders:
        df = pd.DataFrame([{"folder": k, "count": len(v)} for k, v in folders.items()]).sort_values("count", ascending=False)
        st.write("폴더별 *_metrics.csv 개수 (상위 30):")
        st.dataframe(df.head(30))
    else:
        st.write("폴더별 *_metrics.csv 파일 없음")

    # 2) 샘플 파일들 출력 (상위 폴더 각 1개씩 최대 10개)
    st.write("---")
    st.write("샘플 파일 및 내용 (폴더별 최대 1개, 각 파일 최대 40행 표시)")
    sample_show_count = 0
    for folder, files in sorted(folders.items(), key=lambda x: -len(x[1])):
        if sample_show_count >= 10:
            break
        p = files[0]
        st.markdown(f"**{folder} / {p.name}**")
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
            lines = text.splitlines()
            preview = "\n".join(lines[:40])
            st.code(preview, language="text")
        except Exception as e:
            st.write(f"  (파일 열기 실패) {e}")
        sample_show_count += 1

    # 4) 프로젝트 내 코드에서 FORCE_JOURNAL_KEY, _key_name_for, _resolve_journal_name_slug 위치 검색
    st.write("---")
    st.write("코드 안에서 FORCE_JOURNAL_KEY 관련 문자열 검색 결과 (파일명: line_number : 코드라인)")
    search_terms = ["FORCE_JOURNAL_KEY", "_key_name_for(", "_resolve_journal_name_slug("]
    code_hits = []
    for py in sorted(base.rglob("*.py")):
        try:
            txt = py.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for idx, line in enumerate(txt.splitlines(), start=1):
            for term in search_terms:
                if term in line:
                    code_hits.append((str(py), idx, line.strip()))
    if code_hits:
        for p, ln, line in code_hits[:200]:
            st.write(f"- {p}:{ln}: `{textwrap.shorten(line, width=200)}`")
    else:
        st.write("관련 키워드 없음")

    # 5) 현재 전역 FORCE_JOURNAL_KEY 값 출력(존재하면)
    st.write("---")
    try:
        import sys
        # 우선 현재 모듈 네임스페이스에서 찾아보고, 없으면 흔히 쓰이는 모듈명들(app, __main__)을 확인
        val = None
        cur_mod = sys.modules.get(__name__)
        if cur_mod is not None and hasattr(cur_mod, "FORCE_JOURNAL_KEY"):
            val = getattr(cur_mod, "FORCE_JOURNAL_KEY")
        else:
            for alt in ("app", "__main__"):
                m = sys.modules.get(alt)
                if m and hasattr(m, "FORCE_JOURNAL_KEY"):
                    val = getattr(m, "FORCE_JOURNAL_KEY")
                    break
        st.write("app.FORCE_JOURNAL_KEY =", val)
    except Exception as e:
        st.write("전역 FORCE_JOURNAL_KEY 조회 실패:", e)

    st.write("---")
    st.write("진단 도구 실행 완료. 위 출력(특히 폴더별 metrics 분포와 code hits)을 복사해서 저에게 붙여넣어 주세요.")
# ------------------- END: Streamlit UI 진단 함수 -------------------


from new import run_pipeline, make_html_from_csv, run_pipeline_cached, make_html_string_from_csv

# 한국 시간대 설정
now_kst = datetime.datetime.now(ZoneInfo("Asia/Seoul"))

# 이벤트 루프 충돌 방지 (async 사용 시 필수)
nest_asyncio.apply()


st.set_page_config(page_title="OpenAlex Paper Metadata Pipeline", layout="wide")

# ---- View state persistence (rerun에도 탭 유지) ----
if "view" not in st.session_state:
    _default = st.query_params.get("view", "Preview")
    st.session_state.view = _default if _default in ("Preview", "다운로드", "Summary") else "Preview"

def _on_change_view():
    st.session_state.view = st.session_state._view_radio
    st.query_params["view"] = st.session_state.view



# 캐시 데코레이터 제거 (파일 생성/외부 I/O는 캐시 비권장)
def cached_run(issns, year_start, year_end, email,
               progress_bar=None, progress_text=None):

    # storage 하위에 결과 저장
    final_csv_path, _ = run_pipeline_cached(
        issns=issns, year_start=year_start, year_end=year_end, email=email,
        include_only_with_abstract=False, make_html=True,
        base_dir=Path("storage")
    )

    if progress_bar:
        progress_bar.progress(0.7)
    if progress_text:
        progress_text.info("최종 CSV 생성 및 확인 중 ..")

    #최종 CSV 경로 문자열 -> Path 객체
    out_path = Path(final_csv_path)

    final_html = make_html_from_csv(str(out_path))

    if progress_bar:
        progress_bar.progress(1.0)
    if progress_text:
        progress_text.success("모든 구간 처리 및 최종 파일 생성 완료")

    st.sidebar.caption(f"마지막 업데이트 · {now_kst:%H:%M:%S}")

    return str(out_path), final_html

# === 새 헬퍼: 센트럴리티 CSV 찾기 ===
def _find_centrality_csvs(final_csv_path: str):
    base = Path(final_csv_path).with_suffix("")  # ..._ror_extract_name
    base_str = str(base)
    candidates = [
        f"{base_str}_centrality.csv",
        f"{base_str}_ror_extract_name_centrality.csv",
    ]
    # logs에서 workdir_tmp에 저장되는 경우도 커버
    search_dirs = [base.parent, base.parent / "workdir_tmp"]
    found = None
    for d in search_dirs:
        for name in candidates:
            p = d / Path(name).name
            if p.exists():
                found = p
                break
        if found: break
    return found


def _mk_metric_pattern(prefix_all, y0, y1):
    return re.compile(rf"^{re.escape(prefix_all)}_{y0}_{y1}_metrics\.csv$", re.I)

def _summary_metrics_common_multi(sel_csv_list: list[str]):
    """
    멀티 최종 CSV 리스트를 받아 공통 요약지표를 합산해 1행 DataFrame으로 반환.
    - 각 최종 CSV에 대해 new.py의 build_summary_from_metrics_for_final()을 호출
    - 숫자 항목만 합산, 비율은 합산 결과로 재계산
    - 파일 저장/중간산출 없음 (메모리 직행)
    """

    from new import build_summary_from_metrics_for_final

    # 누적 버킷
    tot = {
        "최초 수집 논문 수": 0,
        "최종 수집 논문 수": 0,
        "Editorial Material 삭제 수": 0,
        "ID 패턴 불일치 삭제 수": 0,
        "최종 CSV 행 수(합계)": 0,
        "DOI 결측 수(합산)": 0,
        "DOI 보강 수(합산)": 0,
        "ROR ID 결측 수(합산)": 0,
        "ROR ID 보강 수(합산)": 0,
    }

    def _to_int(x):
        try:
            s = str(x).strip()
            if s.endswith("%"):
                s = s[:-1]
            s = s.replace(",", "")
            v = float(s)
            return int(v) if v.is_integer() else int(round(v))
        except Exception:
            return 0

    any_ok = False
    for path in sel_csv_list:
        try:
            df = build_summary_from_metrics_for_final(path)
            if isinstance(df, str):
                df = pd.read_csv(df, encoding="utf-8-sig")

            if df is None or df.empty:
                continue
            any_ok = True
            row = df.iloc[0]
            for k in tot.keys():
                tot[k] += _to_int(row.get(k, 0))
        except Exception as e:
            # 각 파일 오류는 스킵하고 다음으로 진행
            print(f"[WARN] multi summary read failed: {path} -> {e}")

    if not any_ok:
        return pd.DataFrame([{
            "최초 수집 논문 수": 0,
            "최종 수집 논문 수": 0,
            "Editorial Material 삭제 수": 0,
            "ID 패턴 불일치 삭제 수": 0,
            "최종 CSV 행 수(합계)": 0,
            "DOI 결측 수(합산)": 0,
            "DOI 보강 수(합산)": 0,
            "DOI 보강률": "0.00%",
            "ROR ID 결측 수(합산)": 0,
            "ROR ID 보강 수(합산)": 0,
            "ROR ID 보강률": "0.00%",
        }])

    # 비율 재계산
    doi_denom = tot["DOI 결측 수(합산)"] + tot["DOI 보강 수(합산)"]
    ror_denom = tot["ROR ID 결측 수(합산)"] + tot["ROR ID 보강 수(합산)"]
    doi_rate = f"{(100.0 * tot['DOI 보강 수(합산)'] / doi_denom):.2f}%" if doi_denom else "0.00%"
    ror_rate = f"{(100.0 * tot['ROR ID 보강 수(합산)'] / ror_denom):.2f}%" if ror_denom else "0.00%"

    return pd.DataFrame([{
        **tot,
        "DOI 보강률": doi_rate,
        "ROR ID 보강률": ror_rate,
    }])




# --- diagnostics helper (붙여넣기: app.py에 한 번만 추가) ---
def run_metrics_diagnostic(final_csv_list=None, extra_base_dirs=None):
    """
    final_csv_list: list of final CSV paths (strings). 보통 st.session_state.runs의 각 item['csv']들.
    extra_base_dirs: 추가로 스캔할 경로 리스트 (strings) e.g. ['./workdir_tmp']
    Returns: (metrics_files, per_file_list, totals_dict)
    Also returns a small CSV in-memory for download.
    """
    def to_float_safe(x):
        try:
            if x is None:
                return None
            s = str(x).strip()
            if s == "":
                return None
            s = s.replace(",", "")
            return float(s)
        except Exception:
            return None

    # 1) collect candidate dirs
    scan_dirs = set()
    if final_csv_list:
        for p in final_csv_list:
            try:
                pth = Path(p)
                scan_dirs.add(pth.parent)
                scan_dirs.add(pth.parent / "workdir_tmp")
            except Exception:
                continue
    # allow caller to pass extra dirs
    if extra_base_dirs:
        for d in extra_base_dirs:
            scan_dirs.add(Path(d))

    # default fallback: current dir + './workdir_tmp'
    if not scan_dirs:
        scan_dirs.add(Path("."))
        scan_dirs.add(Path("workdir_tmp"))

    # 2) discover metrics files
    metrics_files = []
    for d in scan_dirs:
        if not d.exists():
            continue
        metrics_files.extend(list(d.rglob("*_metrics.csv")))
    metrics_files = sorted(set(metrics_files))

    # 3) read files and aggregate
    totals = {}
    per_file = []
    for mf in metrics_files:
        record = {}
        try:
            df = pd.read_csv(mf, encoding='utf-8-sig', on_bad_lines='warn')
            # support 'key','value' or two-column fallback
            if "key" in df.columns and "value" in df.columns:
                rows = list(zip(df["key"].astype(str), df["value"]))
            else:
                rows = []
                for r in df.itertuples(index=False):
                    if len(r) >= 2:
                        rows.append((str(r[0]), getattr(r, "1", r[1] if len(r) > 1 else None)))
            for k, v in rows:
                k = str(k).strip()
                if not k:
                    continue
                num = to_float_safe(v)
                record[k] = v if num is None else num
                if num is not None:
                    totals[k] = totals.get(k, 0.0) + num
        except Exception as e:
            record["_error"] = str(e)
        per_file.append((mf, record))

    # prepare CSV output of aggregated totals
    buf = io.StringIO()
    if totals:
        pd.DataFrame([totals]).to_csv(buf, index=False, encoding='utf-8')
    else:
        buf.write("")  # empty
    buf.seek(0)
    return metrics_files, per_file, totals, buf
# --- end diagnostics helper ---


# === 기관정보 관련 헬퍼: mapped affiliation 불러오기 + 결측/보완 계산 ===
def _load_mapped_affiliations_for_final(final_csv_path: str) -> list[str]:
    """
    final_csv_path(…_ror_extract_name.csv)를 기준으로,
    같은 폴더 / workdir_tmp 에 있는 다음 패턴의 파일들에서
    normalized_affiliation 값을 모두 모아 반환한다.

      - <prefix>_mapped_affiliations.csv      (구버전 호환)
      - mapped_affiliations_YYYY.csv          (연도별 신규 버전)
    """
    from pathlib import Path
    import pandas as pd

    base = Path(final_csv_path)
    stem = base.stem.replace("_ror_extract_name", "")

    candidates: list[Path] = []

    # 1) 예전 규칙: <prefix>_mapped_affiliations.csv
    candidates.append(base.with_name(f"{stem}_mapped_affiliations.csv"))
    candidates.append(base.parent / "workdir_tmp" / f"{stem}_mapped_affiliations.csv")

    # 2) 새 규칙: mapped_affiliations_YYYY.csv (연도별)
    try:
        for p in base.parent.glob("mapped_affiliations_*.csv"):
            candidates.append(p)
    except Exception:
        pass

    try:
        wt = base.parent / "workdir_tmp"
        if wt.exists():
            for p in wt.glob("*_mapped_affiliations.csv"):
                candidates.append(p)
            for p in wt.glob("mapped_affiliations_*.csv"):
                candidates.append(p)
    except Exception:
        pass

    # 실제로 존재하는 파일들만 대상
    candidates = [p for p in candidates if p.exists()]

    if not candidates:
        return []

    normed_set = set()

    for p in candidates:
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            continue

        if "normalized_affiliation" not in df.columns:
            continue

        aff_list = (
            df["normalized_affiliation"]
            .dropna()
            .astype(str)
            .tolist()
        )

        for s in aff_list:
            s = s.strip().rstrip(" .,\t")
            s = " ".join(s.split())
            if s:
                normed_set.add(s)

    return sorted(normed_set)


def _compute_inst_missing_and_recovered(final_csv_path: str):
    """
    반환:
      기관정보 기준 결측 수, 기관정보 보완 수

    - 기관정보 기준 결측 수:
        authorships 안에 ROR URL이 한 개도 없는 논문 수
    - 기관정보 보완 수:
        그 결측 논문들 중에서 mapped affiliation(기관명)이
        authorships 텍스트 안에 한 개라도 등장하는 논문 수
    """
    import re
    from pathlib import Path
    import pandas as pd

    df = pd.read_csv(Path(final_csv_path), dtype={"authorships": str}, encoding="utf-8-sig")

    # 1) 기관정보 기준 결측 수: ROR URL이 하나도 없는 행
    rx_ror = re.compile(r"https?://ror\.org/[0-9a-z]+", re.I)
    has_ror = df["authorships"].astype(str).apply(lambda s: bool(rx_ror.search(s)))
    mask_missing = ~has_ror
    df_missing = df[mask_missing].copy()
    inst_missing = int(mask_missing.sum())

    # 2) 기관정보 보완 수: 결측행들 중 mapped affiliation이 한 개라도 들어 있는 행
    mapped_affs = _load_mapped_affiliations_for_final(final_csv_path)
    if not mapped_affs or inst_missing == 0:
        return inst_missing, 0

    def _has_any_mapped(cell: str) -> bool:
        if not isinstance(cell, str):
            cell = str(cell)
        for aff in mapped_affs:
            if aff and aff in cell:
                return True
        return False

    recovered_mask = df_missing["authorships"].apply(_has_any_mapped)
    inst_recovered = int(recovered_mask.sum())

    return inst_missing, inst_recovered


# === 새 헬퍼: 공통 요약표 ===
def _summary_metrics_common(final_csv_path: str):

    # ---- 0) 경로/메타 파싱 (먼저 해야 함)
    final_csv = Path(final_csv_path)
    base_path = final_csv.with_suffix("")
    base = str(base_path)

    # 접미사 제거해 루트베이스 추출
    root_base = re.sub(r'_(?:ror(?:_extract(?:_name)?)?)(?:_summary)?$', '', base)
    json_merged = Path(f"{root_base}.json")
    csv_file    = final_csv

    # export/summary 파일이면 CSV 길이 폴백 금지
    is_export = final_csv.name.endswith("_export.csv") or final_csv.name.endswith("_summary.csv")

    # 접두사/연도 추출
    m = re.search(r"_(\d{4})_(\d{4})$", root_base)
    y0 = y1 = None
    prefix_all = ""
    if m:
        y0, y1 = int(m.group(1)), int(m.group(2))
        prefix_all = Path(root_base).name[:-(len(m.group(0)))]

    # 검색 디렉터리
    root_dir = Path(root_base).parent
    search_dirs = [root_dir, root_dir / "workdir_tmp"]

    def sum_metrics(keys, search_dirs, allowed_names=None):


        acc = {k: 0 for k in keys if not k.endswith("_rate")}
        rate_keys = [k for k in keys if k.endswith("_rate")]
        found = False

        # 중복 차단: 경로 + 파일명 + (선택) triplet
        seen_paths = set()   # 경로 기준(안전망)
        seen_names = set()   # 파일명 기준 (경로 달라도 1회)

        # "xxx (1).csv" → "xxx.csv"
        def _strip_copy_suffix(name: str) -> str:
            return re.sub(r' \(\d+\)\.csv$', '.csv', name, flags=re.IGNORECASE)

        def _to_num(s):
            try:
                if s is None or s == "":
                    return None
                f = float(s)
                return int(f) if f.is_integer() else f
            except Exception:
                return None

        for base in search_dirs:
            base = Path(base)
            if not base.exists():
                continue
            for mf in base.rglob("*_metrics.csv"):
                # 1) 이번 실행 대상 파일만 (복사본 접미사 제거 후 비교)
                base_name = _strip_copy_suffix(mf.name)
                if allowed_names and (base_name not in allowed_names):
                    continue

                # 2) 파일명 중복 차단 (경로 달라도 같은 이름이면 1회만)
                if base_name in seen_names:
                    continue
                seen_names.add(base_name)

                # 3) 경로 중복 차단(안전망)
                rp = mf.resolve()
                if rp in seen_paths:
                    continue
                seen_paths.add(rp)

                # 4) 파일 내부 중복 key는 '마지막 값'만 사용(현재 정책 유지)
                try:
                    last_by_key = {}
                    with mf.open("r", encoding="utf-8", newline="") as f:
                        rdr = csv.reader(f)
                        _ = next(rdr, None)  # header skip
                        for row in rdr:
                            if len(row) < 2:
                                continue
                            k = (row[0] or "").strip()
                            if not k:
                                continue
                            v = _to_num(row[1])
                            if v is None or k.endswith("_rate"):
                                continue
                            last_by_key[k] = v  # 동일 key 재등장 시 마지막 값으로 갱신

                    for k, v in last_by_key.items():
                        if k in acc:
                            acc[k] += v
                    found = True
                except Exception:
                    # 개별 파일 실패는 전체 실패로 보지 않음
                    pass

        # ---- 비율 재계산 (기존 로직 유지) ----
        if "doi_enriched" in acc and "doi_missing" in acc and "doi_enrich_rate" in rate_keys:
            denom = (acc["doi_missing"] or 0) + (acc["doi_enriched"] or 0)
            acc["doi_enrich_rate"] = round(100.0 * (acc["doi_enriched"] or 0) / denom, 2) if denom else 0.0
        if "ror_enriched" in acc and "ror_missing" in acc and "ror_enrich_rate" in rate_keys:
            denom = (acc["ror_missing"] or 0) + (acc["ror_enriched"] or 0)
            acc["ror_enrich_rate"] = round(100.0 * (acc["ror_enriched"] or 0) / denom, 2) if denom else 0.0

        return acc, found

    # ---- 2) metrics.csv 합산 (이번 실행의 파일명만 허용)
    allowed_names = set()
    if prefix_all and (y0 is not None) and (y1 is not None):
        # 멀티저널 프리픽스("a-b-c")이면 저널별 파일명을 모두 허용
        keys = [k for k in prefix_all.split("-") if k.strip()]
        if not keys:
            keys = [prefix_all]
        years = range(int(y0), int(y1) + 1)
        allowed_names = {f"{k}_{yy}_{yy}_metrics.csv" for k in keys for yy in years}
    acc, found = sum_metrics([
        "total_collected","final_csv_rows",
        "authorships_removed_empty_list", "authorships_removed",
        "col_mismatch_removed", "id_pattern_removed",
        "doi_missing","doi_enriched","doi_enrich_rate",
        "ror_missing","ror_enriched","ror_enrich_rate",
        "ror_missing_before_extract",
    ], search_dirs, allowed_names=allowed_names)

    # ---- 3) 합산 결과 변수들
    total_collected = acc.get("total_collected") if found else None
    removed_authorships_empty = acc.get("authorships_removed_empty_list") if found else None
    removed_authorships = acc.get("authorships_removed") if found else None
    doi_missing_sum   = acc.get("doi_missing") if found else None
    doi_enriched_sum  = acc.get("doi_enriched") if found else None
    doi_rate_from_acc = acc.get("doi_enrich_rate") if found else None
    ror_missing_sum   = acc.get("ror_missing") if found else None
    ror_enriched_sum  = acc.get("ror_enriched") if found else None
    ror_rate_from_acc = acc.get("ror_enrich_rate") if found else None

    editorial_total = (removed_authorships_empty or 0) + (removed_authorships or 0)
    id_pattern_removed_sum = acc.get("id_pattern_removed") if found else None

    # ---- 4) JSON 폴백 (metrics가 없을 때만)
    def _try_load_json(p: Path):
        try:
            return json.loads(p.read_text(encoding="utf-8")) or []
        except Exception:
            return None

    works = None
    if total_collected is None:
        if json_merged.exists():
            works = _try_load_json(json_merged)
        if works is None and prefix_all and (y0 is not None) and (y1 is not None):
            fname_json = f"{prefix_all}_{y0}_{y1}.json"
            for d in search_dirs:
                cand = Path(d) / fname_json
                if cand.exists():
                    works = _try_load_json(cand)
                    if works is not None:
                        break
        if works is not None:
            total_collected = len(works)
            removed_authorships_empty = sum(
                1 for w in works
                if not isinstance(w.get("authorships"), list) or len(w.get("authorships") or []) == 0
            )

    # ---- 5) 최후 폴백: CSV 길이 (export/summary면 금지)
    if total_collected is None and not is_export:
        try:
            if csv_file.exists():
                total_collected = len(pd.read_csv(csv_file))
        except Exception:
            pass

    # ---- 6) 최종 CSV에서 직접 센 값(참고/폴백용)
    doi_missing_final = None
    if csv_file.exists():
        try:
            df_csv = pd.read_csv(csv_file, dtype={"doi": str})
            doi_missing_final = df_csv["doi"].isna().sum() + (df_csv["doi"].astype(str).str.strip() == "").sum()
        except Exception:
            pass

    # ROR 보강 전 결측(최종 CSV의 authorships에서 추정)
    ror_missing_before = None
    try:
        if csv_file.exists():
            tmp = pd.read_csv(csv_file, dtype={"authorships": str})
            if "authorships" in tmp.columns:
                rx = re.compile(r'https?://ror\.org/[0-9a-z]+', re.I)
                ror_before = tmp["authorships"].astype(str).apply(
                    lambda s: list(dict.fromkeys(rx.findall(s)))
                )
                ror_missing_before = int((ror_before.str.len() == 0).sum())
    except Exception:
        pass

    # ---- 7) DOI 결측 표시: metrics가 있으면 그 값, 없으면 최종 CSV
    if doi_missing_sum is None:
        doi_missing_show = doi_missing_final   # 파일을 못 읽었을 때만 CSV로 대체
    else:
        doi_missing_show = doi_missing_sum

    # ---- 8) DOI 보강률 계산(일관화): (enriched / (missing + enriched)) × 100
    doi_rate_calc = None
    if (doi_missing_sum is not None) and (doi_enriched_sum is not None):
        denom = (doi_missing_sum or 0) + (doi_enriched_sum or 0)
        doi_rate_calc = round(100.0 * (doi_enriched_sum or 0) / denom, 2) if denom else 0.0

    # ---- 9) 노드·엣지 수 (중앙성 CSV 있으면 그 노드 집합 우선)
    cent_csv = _find_centrality_csvs(final_csv_path)
    def _to_pairs(names):
        arr = []
        if isinstance(names, str):
            s = names.strip()
            if s:
                try:
                    v = ast.literal_eval(s)
                    arr = list(v) if isinstance(v, (list, tuple)) else [v]
                except Exception:
                    try:
                        v = json.loads(s)
                        arr = v if isinstance(v, list) else [v]
                    except Exception:
                        for sep in [";", "|", ","]:
                            if sep in s:
                                arr = [t.strip() for t in s.split(sep) if t.strip()]
                                break
                        if not arr:
                            arr = [s]
        elif isinstance(names, list):
            arr = names
        arr = [str(a) for a in arr if a]
        pairs = []
        for i in range(len(arr)):
            for j in range(i+1, len(arr)):
                a,b = sorted((arr[i], arr[j]))
                pairs.append((a,b))
        return pairs

    node_count = edge_count = None
    try:
        df_final = pd.read_csv(final_csv)
        all_pairs=[]
        if "org_names" in df_final.columns:
            for pr in df_final["org_names"].apply(_to_pairs):
                all_pairs.extend(pr)
        # 중앙성 CSV가 있어도, 요약표의 '노드/엣지 수'는 최종 CSV 전체 기준으로 계산
        nodes = {n for p in all_pairs for n in p}
        node_count = len(nodes)
        edge_count = len(set(all_pairs))
    except Exception:
        node_count = edge_count = None

    # ---- 10) 출력 테이블 구성 + 포맷 ----
    # 최초 수집 논문 수(first_rows)는 이제 표에서 사용하지 않지만,
    # 필요하면 나중에 다시 쓸 수 있으니 그대로 둠
    first_rows = (0 if total_collected is None else total_collected)
    final_rows = (len(df_final) if 'df_final' in locals() else 0)  # 최종 CSV 행수 = 문헌 총 수

    # 10-1) 기관정보 기준 결측 수 & 기관정보 보완 수 계산
    inst_missing, inst_recovered = _compute_inst_missing_and_recovered(str(csv_file))

    # metrics에서 ror_enriched가 제공되면, 기관정보 보완 수로 우선 사용
    if ror_enriched_sum is not None:
        try:
            inst_recovered = int(ror_enriched_sum)
        except Exception:
            pass

    # 10-2) 보강 전/후 기관정보 활용가능 비율
    if final_rows > 0:
        # 보강 전: (문헌 총 수 - 기관정보 기준 결측 수) / 문헌 총 수 * 100
        pre_util_rate  = (final_rows - inst_missing) / final_rows * 100.0
        # 보강 후: (문헌 총 수 - 기관정보 기준 결측 수 + 기관정보 보완 수) / 문헌 총 수 * 100
        post_util_rate = (final_rows - inst_missing + inst_recovered) / final_rows * 100.0
    else:
        pre_util_rate = post_util_rate = 0.0

    def _fmt_pct(v):
        try:
            return f"{float(v):.2f}%"
        except Exception:
            return "0.00%"

    # 10-3) 요약표 DataFrame (컬럼 이름 전부 새로 정리)
    df_out = pd.DataFrame([{
        # 문헌/비논문
        "문헌 총 수": final_rows,
        "비논문 문헌 수": editorial_total,

        # 기관정보
        "기관정보 기준 결측 수": inst_missing,
        "보강 전 기관정보 활용가능 비율": _fmt_pct(pre_util_rate),
        "기관정보 보완 수": inst_recovered,
        "보강 후 기관정보 활용가능 비율": _fmt_pct(post_util_rate),

        # DOI (이름만 변경)
        "DOI 결측 수": doi_missing_show,
        "DOI 보강 수": (0 if doi_enriched_sum is None else doi_enriched_sum),
        "DOI 보강률": _fmt_pct(
            doi_rate_calc if doi_rate_calc is not None
            else (0.0 if doi_rate_from_acc is None else float(doi_rate_from_acc))
        ),

        # 네트워크 요약
        "노드 수": node_count,
        "엣지 수": edge_count,
    }])

    # 숫자 컬럼 NaN → 0 정리
    for col in [
        "문헌 총 수", "비논문 문헌 수",
        "기관정보 기준 결측 수", "기관정보 보완 수",
        "DOI 결측 수", "DOI 보강 수",
        "노드 수", "엣지 수",
    ]:
        if col in df_out.columns:
            df_out[col] = df_out[col].apply(lambda v: 0 if v is None else v).astype(int)

    return df_out


# === 새 헬퍼: 중심성별 Top 노드 & 엣지 표 ===
# === 중심성별 Top 10 노드 & 엣지 ===
def _summary_top_nodes_and_edges(final_csv_path: str):
    """
    - 중앙성 CSV(…_centrality.csv)에서 각 centrality 상위 10 노드
    - 최종 CSV(org_names)에서 동시출현수 상위 10 엣지
    반환: (df_nodes_wide, df_edges_wide)
      - df_nodes_wide: columns = ['degree: 노드','eigenvector: 노드','betweenness: 노드'], index=Top1..Top10
      - df_edges_wide: columns = ['degree: 엣지','eigenvector: 엣지','betweenness: 엣지'], index=Top1..Top10
    """
    import itertools
    from collections import Counter
    # --- 중앙성 CSV 찾기 ---
    cent_csv = _find_centrality_csvs(final_csv_path)
    if cent_csv is None:
        # 없으면 한번 생성 시도(실패해도 조용히 패스)
        try:
            from new import make_html_from_csv
            make_html_from_csv(final_csv_path)
        except Exception:
            pass
        cent_csv = _find_centrality_csvs(final_csv_path)
    if cent_csv is None:
        return pd.DataFrame(), pd.DataFrame()

    # --- 중앙성 로드 ---
    cent = pd.read_csv(cent_csv, dtype={"org": str})
    if "org" not in cent.columns:
        return pd.DataFrame(), pd.DataFrame()
    # closeness까지 자동 포함 (있을 때만)
    candidates = [c for c in ["degree","eigenvector","betweenness","closeness"] if c in cent.columns]
    if not candidates:
        return pd.DataFrame(), pd.DataFrame()

    # 노드 Top10(각 centrality 별)
    nodes_cols = {}
    for c in candidates:
        tmp = cent[["org", c]].dropna()
        tmp = tmp.sort_values(c, ascending=False)
        names = tmp["org"].astype(str).head(10).tolist()
        nodes_cols[f"{c}: 노드"] = names + [""]*(10-len(names))
    df_nodes = pd.DataFrame(nodes_cols, index=[f"Top {i}" for i in range(1,11)])

    # --- 최종 CSV에서 엣지(동시출현) 계산 ---
    final_df = pd.read_csv(final_csv_path, dtype=str, encoding="utf-8-sig")
    if "org_names" not in final_df.columns:
        return df_nodes, pd.DataFrame()

    def _as_list(s):
        if s is None or str(s).strip()=="":
            return []
        if isinstance(s, list):
            return [str(x).strip() for x in s if str(x).strip()]
        txt = str(s).strip()
        # 리스트 리터럴 우선
        try:
            v = ast.literal_eval(txt)
            if isinstance(v, list):
                return [str(x).strip() for x in v if str(x).strip()]
        except Exception:
            pass
        # 구분자
        for sep in (";", "|", ","):
            if sep in txt:
                return [t.strip() for t in txt.split(sep) if t.strip()]
        return [txt] if txt else []

    edge_counter = Counter()
    for orgs in final_df["org_names"].apply(_as_list):
        # 같은 논문 내 중복 제거
        uniq = sorted(set([o for o in orgs if o]))
        if len(uniq) < 2:
            continue
        for a, b in itertools.combinations(uniq, 2):
            pair = f"{a} — {b}" if a <= b else f"{b} — {a}"
            edge_counter[pair] += 1

    top_edges = [k for k, _ in edge_counter.most_common(10)]
    # 엣지는 centrality와 무관하게 “동시출현수” 기준으로 동일 목록을 각 col에 사용
    edges_cols = {f"{c}: 엣지": top_edges + [""]*(10-len(top_edges)) for c in candidates}
    df_edges = pd.DataFrame(edges_cols, index=[f"Top {i}" for i in range(1,11)])

    # candidates가 2개/1개인 경우도 열 정렬 보정
    order = []
    for c in ["degree","eigenvector","betweenness", "closeness"]:
        if f"{c}: 노드" in df_nodes.columns:
            order.append(f"{c}: 노드")
        if f"{c}: 엣지" in df_edges.columns:
            order.append(f"{c}: 엣지")
    df_nodes = df_nodes[[c for c in [f"{m}: 노드" for m in ["degree","eigenvector","betweenness", "closeness"]] if c in df_nodes.columns]]
    df_edges = df_edges[[c for c in [f"{m}: 엣지" for m in ["degree","eigenvector","betweenness", "closeness"]] if c in df_edges.columns]]
    return df_nodes, df_edges


def sidebar_controls():
    st.sidebar.header("설정")
    issns_text = st.sidebar.text_area(
        "ISSN 리스트 (줄바꿈으로 구분)",
        value="0043-1354\n0011-9164\n0733-9429",
        height=100,
    )
    issns = [s.strip() for s in issns_text.splitlines() if s.strip()]

    year_col1, year_col2 = st.sidebar.columns(2)
    year_start = year_col1.number_input("시작 연도", value=2015, step=1)
    year_end = year_col2.number_input("종료 연도", value=2024, step=1)

    email = st.sidebar.text_input("OpenAlex 이메일", value="s0124kw@gmail.com")
    run_btn = st.sidebar.button("🚀 파이프라인 실행", width="stretch")
    return issns, int(year_start), int(year_end), email, run_btn


def app():
    # st.title("Paper Metadata Collection Pipeline")
    st.title("Paper-Based Inter-Institutional Connectivity Analysis Pipeline")
    if "runs" not in st.session_state:
        st.session_state.runs = []
    # --- NEW: 실행 상태 기본값 ---
    if "_job_state" not in st.session_state:
        st.session_state._job_state = "idle"   # idle | running | done
    if "_job" not in st.session_state:
        st.session_state._job = None
    if "view" not in st.session_state:
        st.session_state.view = "Preview"

    issns, year_start, year_end, email, run_btn = sidebar_controls()



    # 진행률/메시지 슬롯 -> 진행중인 상황 출력
    progress_bar = st.progress(0, text="대기 중")
    progress_text = st.empty()

    # Preview 행 수(고정)
    PREVIEW_N = 1000
    # CSV 다운로드 임계값(MB)
    THRESHOLD_MB = 150

    # === 공통 Summary 렌더링 함수 ===
    def _render_summary_block(sel_csv_or_list):
        """
        - 단일 파일(str)  : _summary_metrics_common() 사용
        - 다중 파일(list): _summary_metrics_common_multi() 사용
        - 공통요약지표 아래에 Top10 노드/엣지 표를 단일/멀티 모두에서 렌더
        """

        with st.expander("Summary", expanded=True):
            st.subheader("공통 요약 지표")

            if isinstance(sel_csv_or_list, (list, tuple)):
                csv_list = list(sel_csv_or_list)
                if len(csv_list) == 0:
                    st.info("최종 CSV를 하나 이상 선택하세요.")
                    return
                summary_df = _summary_metrics_common_multi(csv_list)
                download_name = "multi_selection_summary.csv"
            else:
                sel_csv = sel_csv_or_list
                if not sel_csv:
                    st.info("최종 CSV를 선택하세요.")
                    return
                summary_df = _summary_metrics_common(sel_csv)
                download_name = Path(sel_csv).stem + "_summary.csv"

            if summary_df is None or summary_df.empty:
                st.warning("요약할 데이터가 없습니다.")
                return

            st.dataframe(summary_df, use_container_width=True)

            # 다운로드(메모리 직행, 저장 X)
            csv_bytes = summary_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "요약 CSV 다운로드",
                data=csv_bytes,
                file_name=download_name,
                mime="text/csv",
            )

            # ---- 중심성별 Top10 노드&엣지 ----
            # 단일 CSV 선택시에만 표시 (멀티 선택은 합산 기준이 애매하므로 숨김)
            if not isinstance(sel_csv_or_list, (list, tuple)):
                sel_csv = sel_csv_or_list
                nodes_wide, edges_wide = _summary_top_nodes_and_edges(sel_csv)
                st.markdown("### 중심성별 Top 10 노드&엣지")
                colA, colB = st.columns(2)
                with colA:
                    if not nodes_wide.empty:
                        st.dataframe(nodes_wide, use_container_width=True)
                    else:
                        st.info("중앙성 CSV를 찾을 수 없거나 컬럼 누락으로 노드 Top10을 만들 수 없습니다.")
                with colB:
                    if not edges_wide.empty:
                        st.dataframe(edges_wide, use_container_width=True)
                    else:
                        st.info("최종 CSV의 org_names를 찾을 수 없어 엣지 Top10을 만들 수 없습니다.")

    def _render_summary_unified(sel):
        """
        항상 '선택된 실행'만 요약합니다.
        절대로 여러 실행을 합산하지 않습니다.
        """
        _render_summary_block(sel["csv"])


    # === 공통 다운로드 블록(시각화 포함) ===
    def _render_download_block(sel_csv: str, sel_html: str):
        csv_path = Path(sel_csv)
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)

        col_csv, col_html = st.columns(2)
        with col_csv:
            if file_size_mb <= THRESHOLD_MB:
                st.download_button(
                    label=f"CSV 다운로드 ({file_size_mb:.1f} MB)",
                    data=csv_path.read_bytes(),
                    file_name=csv_path.name,
                    mime="text/csv",
                    width="stretch",
                )
            else:
                buf = BytesIO()
                with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(csv_path.name, csv_path.read_bytes())
                zip_bytes = buf.getvalue()
                st.download_button(
                    label=f"CSV 다운로드 (ZIP, 원본 {file_size_mb:.1f} MB)",
                    data=zip_bytes,
                    file_name=csv_path.stem + ".zip",
                    mime="application/zip",
                    width="stretch",
                )

        with col_html:
            # 네 가지 중심성 지표별 네트워크 HTML을 각각 다운로드
            items = [
                ("degree",      "Degree-based",      "_degree_network.html"),
                ("eigenvector", "Eigenvector-based", "_eigenvector_network.html"),
                ("betweenness", "Betweenness-based", "_betweenness_network.html"),
                ("closeness",   "Closeness-based",   "_closeness_network.html"),
            ]

            for metric, label, suffix in items:
                html_str = make_html_string_from_csv(str(csv_path), size_by=metric, color_by=metric)
                st.download_button(
                    label=f"Visualization 다운로드 ({label})",
                    data=html_str.encode("utf-8"),
                    file_name=csv_path.stem + suffix,
                    mime="text/html",
                    width = "stretch",
                )


    # === 공통 Preview(상위 1,000행) 블록 ===
    def _render_preview_block(sel_csv: str):
        df = pd.read_csv(sel_csv)
        total_rows = len(df)
        st.caption(
            f"⚠️ 아래 표는 총 {total_rows:,}행 중 "
            f"상위 {min(PREVIEW_N, total_rows):,}행만 Preview입니다. "
            "'다운로드' 탭에서 파일을 내려받을 수 있습니다."
        )
        st.dataframe(df.head(PREVIEW_N), width="stretch")

        # ▼ 네트워크 Preview(임베드) — degree / eigenvector
        with st.expander("네트워크 Preview (Degree / Eigenvector / Betweenness / Closeness)", expanded=False):
            st.caption("※ 안내: 'Summary' 탭의 공통 요약 표에 표시되는 '노드/엣지 수'는 **최종 CSV 전체**를 기준으로 계산됩니다. "
                        "아래 미리보기 네트워크는 **Centrality 기준으로 상위 노드들을 강조/필터링**하여 "
                        "요약의 수치와 다를 수 있습니다. 네트워크는 '다운로드' 탭에서 HTML로 다운로드 받으실 수 있습니다.")
            tabs = st.tabs(["Degree 네트워크", "Eigenvector 네트워크", "Betweenness 네트워크", "Closeness 네트워크"])

            # HTML 문자열 생성 (기존 헬퍼 재사용)
            deg_html = make_html_string_from_csv(str(Path(sel_csv)), size_by="degree",       color_by="degree")
            eig_html = make_html_string_from_csv(str(Path(sel_csv)), size_by="eigenvector",  color_by="eigenvector")
            bet_html = make_html_string_from_csv(str(Path(sel_csv)), size_by="betweenness",  color_by="betweenness")
            clo_html = make_html_string_from_csv(str(Path(sel_csv)), size_by="closeness",    color_by="closeness")


            with tabs[0]:
                components.html(deg_html, height=800, scrolling=True)
            with tabs[1]:
                components.html(eig_html, height=800, scrolling=True)
            with tabs[2]:
                components.html(bet_html, height=800, scrolling=True)
            with tabs[3]:
                components.html(clo_html, height=800, scrolling=True)

    # === 실행 여부에 따라 결과 렌더 ===

    # A) 버튼 클릭 시: 상태만 세팅하고 즉시 재실행 → 화면을 '실행 중' 상태로 고정
    if run_btn:
        st.session_state._job = {
            "issns": issns,
            "year_start": int(year_start),
            "year_end": int(year_end),
            "email": email,
        }
        st.session_state._job_state = "running"
        
        #무한 리런 방지
        if not st.session_state.get("_reran_once"):
            st.session_state["_reran_once"] = True
            st.rerun()

    # B) 재실행 후: 상태를 보고 실제 작업 수행 (중간에 앱이 재실행돼도 계속 '실행 중' 유지)
    if st.session_state._job_state == "running" and st.session_state._job:
        params = st.session_state._job
        start_time = datetime.datetime.now(ZoneInfo("Asia/Seoul"))

        with st.spinner("파이프라인 실행 중... (시간이 걸릴 수 있어요)"):
            progress_text.info("준비 중..")
            final_csv, final_html = cached_run(
                params["issns"], params["year_start"], params["year_end"], params["email"],
                progress_bar=progress_bar, progress_text=progress_text
            )

        # 종료 시간/라벨 기록
        end_time = datetime.datetime.now(ZoneInfo("Asia/Seoul"))
        duration = end_time - start_time
        prefix = Path(final_csv).stem.replace(
            f"_{params['year_start']}_{params['year_end']}_ror_extract_name", ""
        )
        label = f"{prefix} | {params['year_start']}-{params['year_end']} | {start_time.strftime('%m/%d %H:%M')}"

        st.session_state.runs.append({
            "label": label, "csv": final_csv, "html": final_html,
            "start": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "end": end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "duration": str(duration)
        })

        # 실행 종료 표시 및 상태 초기화
        st.session_state._job_state = "done"
        st.session_state._job = None

        # 다음 실행을 위해 가드 해제
        st.session_state.pop("_reran_once", None)

        # 방금 실행 결과를 즉시 렌더(기존 UI 패턴 유지)
        sel = st.session_state.runs[-1]
        st.success(f"실행 완료: {sel['label']}")

        st.sidebar.markdown("---")
        st.sidebar.subheader("실행 시간")
        st.sidebar.write(f" 시작 {sel['start']}")
        st.sidebar.write(f" 종료 {sel['end']}")
        st.sidebar.write(f" 소요 {sel['duration']}")

        # ✅ 라디오 박스 복원: Preview / 다운로드 / Summary
        view = st.radio(
            "보기 선택",
            ["Preview", "다운로드", "Summary"],
            index=["Preview","다운로드","Summary"].index(st.session_state.view),
            key="_view_radio",
            horizontal=True,
            on_change=_on_change_view,
        )
        view = st.session_state.view

        if view == "Preview":
            _render_preview_block(sel["csv"])
        if view == "다운로드":
            _render_download_block(sel["csv"], sel["html"])
        if view == "Summary":
            _render_summary_unified(sel)

        # 최근 선택 CSV를 세션에 보관(선택 유지용)
        st.session_state["last_csv"] = sel["csv"]


    else:
        # 이전 실행 이력 선택/표시
        if st.session_state.runs:
            st.markdown("### 이전 실행 결과 보기")
            options = [r["label"] for r in st.session_state.runs]
            selected = st.selectbox("실행 결과 선택", options, index=len(options)-1)
            sel = next(r for r in st.session_state.runs if r["label"] == selected)

            # 사이드바에 실행 시간 표시
            st.sidebar.markdown("---")
            st.sidebar.subheader("실행 시간")
            st.sidebar.write(f" 시작 {sel['start']}")
            st.sidebar.write(f" 종료 {sel['end']}")
            st.sidebar.write(f" 소요 {sel['duration']}")

            # ✅ 라디오 박스 복원: Preview / 다운로드 / Summary
            view = st.radio(
                "보기 선택",
                ["Preview", "다운로드", "Summary"],
                index=["Preview","다운로드","Summary"].index(st.session_state.view),
                key="_view_radio",
                horizontal=True,
                on_change=_on_change_view,
            )
            view = st.session_state.view

            if view == "Preview":
                _render_preview_block(sel["csv"])
            if view == "다운로드":
                _render_download_block(sel["csv"], sel["html"])
            if view == "Summary":
                _render_summary_unified(sel)

            # 최근 선택 CSV를 세션에 보관(선택 유지용)
            st.session_state["last_csv"] = sel["csv"]
        else:
            st.info("사이드바에서 설정 후 🚀 파이프라인 실행 버튼을 눌러주세요.")



if __name__ == "__main__":
    app()
