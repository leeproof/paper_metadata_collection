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
    from pathlib import Path
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
    여러 실행(여러 저널 × 여러 연도)의 최종 CSV 목록을 '단일 Summary와 완전히 동일한 계산 로직'으로
    합산한 1행 DataFrame을 반환합니다.
    - metrics 합산: 각 실행 폴더(root_dir, workdir_tmp)만 제한적으로 스캔 + 파일명 '완전일치' 필터
    - DOI/ROR 보강률: 단일과 동일하게 '합산된 분자/분모' 기준으로 재계산하여 %.2f%% 포맷
    - 노드/엣지: org_names 기반으로 유니온 / 중앙성 CSV 있으면 그 노드 집합 우선 적용 (단일과 동일)
    - 반환 컬럼/순서/형식: 단일 Summary 테이블과 동일
    """
    import pandas as pd

    # 0) 헬퍼
    def _to_num(s):
        try:
            if s is None or s == "":
                return None
            f = float(s)
            return int(f) if f.is_integer() else f
        except Exception:
            return None

    def _fmt_pct(v):
        try:
            return f"{float(v):.2f}%"
        except Exception:
            return "0.00%"

    # 1) 각 실행의 (root_dir, prefix_all, y0, y1) 메타 추출
    roots_with_meta = []
    csv_paths = [Path(p) for p in sel_csv_list]
    for final_csv in csv_paths:
        base_path = final_csv.with_suffix("")  # ..._final.csv → ..._final
        base = str(base_path)
        # *_final[_ror/_ror_extract(_name)][_summary]? 접미사 제거
        root_base = re.sub(r'_(?:ror(?:_extract(?:_name)?)?)(?:_summary)?$', '', base)
        m = re.search(r"_(\d{4})_(\d{4})$", root_base)
        if not m:
            continue
        y0, y1 = int(m.group(1)), int(m.group(2))
        prefix_all = Path(root_base).name[:-(len(m.group(0)))]
        root_dir = Path(root_base).parent
        roots_with_meta.append((root_dir, prefix_all, y0, y1))

    # 2) metrics 합산(정확 일치 파일만, 현재 실행 폴더만 스캔)
    keys = [
        "total_collected", "final_csv_rows", "authorships_removed_empty_list",
        "doi_missing", "doi_enriched", "doi_enrich_rate",
        "ror_missing", "ror_enriched", "ror_enrich_rate",
        "ror_missing_after_extract",
    ]
    acc = {k: 0 for k in keys if not k.endswith("_rate")}
    pats = [_mk_metric_pattern(prefix, y0, y1) for (root, prefix, y0, y1) in roots_with_meta]
    search_dirs = []
    for (root, *_meta) in roots_with_meta:
        root = Path(root)
        search_dirs += [root, root / "workdir_tmp"]

    seen = set()
    for base in search_dirs:
        base = Path(base)
        if not base.exists():
            continue
        for mf in base.rglob("*_metrics.csv"):
            rp = mf.resolve()
            if rp in seen:
                continue
            name = mf.name
            # ★ 선택된 실행들의 metrics만: 파일명 '완전일치'
            if not any(pat.match(name) for pat in pats):
                continue
            seen.add(rp)
            try:
                import csv
                with mf.open("r", encoding="utf-8", newline="") as f:
                    rdr = csv.reader(f)
                    _ = next(rdr, None)  # header skip
                    for row in rdr:
                        if len(row) < 2:
                            continue
                        k, v = row[0], _to_num(row[1])
                        if k in acc and v is not None and (not k.endswith("_rate")):
                            acc[k] += v
            except Exception:
                pass

    # 3) ROR 결측(보강 전) 합산 — 최종 CSV들의 authorships에서 직접 계산 (단일과 동일 로직)
    ror_missing_before_total = None
    try:
        rx = re.compile(r'https?://ror\.org/[0-9a-z]+', re.I)
        total = 0
        for csv_file in csv_paths:
            try:
                tmp = pd.read_csv(csv_file, dtype={"authorships": str})
                if "authorships" in tmp.columns:
                    before = tmp["authorships"].astype(str).apply(
                        lambda s: list(dict.fromkeys(rx.findall(s)))
                    )
                    total += int((before.str.len() == 0).sum())
            except Exception:
                pass
        ror_missing_before_total = total
    except Exception:
        pass

    # 4) 노드/엣지 — org_names 유니온 + 중앙성 CSV 우선 (단일과 동일 로직)
    node_count = edge_count = None
    try:
        all_pairs = []
        node_universe = set()
        cent_nodes = set()
        have_cent_nodes = False

        def _to_pairs(names):
            arr=[]
            if isinstance(names, str):
                s = names.strip()
                if s:
                    # literal → json → 구분자 순으로 파싱
                    try:
                        v = ast.literal_eval(s)
                        arr = list(v) if isinstance(v, (list, tuple)) else [v]
                    except Exception:
                        try:
                            v = json.loads(s)
                            arr = v if isinstance(v, list) else [v]
                        except Exception:
                            for sep in [";","|",","]:
                                if sep in s:
                                    arr = [x.strip() for x in s.split(sep) if x.strip()]
                                    break
                            if not arr:
                                arr=[s]
            elif isinstance(names, list):
                arr = names

            # 무향 쌍(중복/순서 제거)
            pairs=[]
            arr=[x for x in arr if x]
            for i in range(len(arr)):
                for j in range(i+1, len(arr)):
                    a,b = sorted((arr[i], arr[j]))
                    pairs.append((a,b))
            return pairs

        for csv_file in csv_paths:
            # org_names → 쌍/노드
            try:
                df_final = pd.read_csv(csv_file)
                if "org_names" in df_final.columns:
                    for pr in df_final["org_names"].apply(_to_pairs):
                        all_pairs.extend(pr)
                # 현재까지 쌓인 쌍에서 노드 유니온
                node_universe.update({n for p in all_pairs for n in p})
            except Exception:
                pass

            # 중앙성 CSV 노드 집합
            try:
                cent_csv = _find_centrality_csvs(str(csv_file))  # 단일과 동일한 헬퍼 사용
                if cent_csv:
                    cent = pd.read_csv(cent_csv)
                    node_col = "node" if "node" in cent.columns else cent.columns[0]
                    cent_nodes.update(set(cent[node_col].astype(str).tolist()))
                    have_cent_nodes = True
            except Exception:
                pass

        # 다중 요약도 항상 '최종 CSV 전체' 기준으로 노드/엣지 수 계산
        node_set = node_universe
        edge_count = len(set(all_pairs))
        node_count = len(node_set)

    except Exception:
        node_count = edge_count = None

    # 5) DOI/ROR 보강률 재계산(합산 기준) — 단일과 동일 포맷('%.2f%%')
    doi_missing_sum  = acc.get("doi_missing")
    doi_enriched_sum = acc.get("doi_enriched")
    ror_missing_after = acc.get("ror_missing_after_extract")
    ror_enriched_sum  = acc.get("ror_enriched")

    # (A) DOI 보강률: 합산된 분자/분모 기준 (단일과 동일한 방식으로 사용 중인 분모에 맞춰 두세요)
    doi_rate = None
    try:
        # 보편적으로는 (enriched / (missing + enriched)) 또는 (enriched / missing) 중 한 방식을 씁니다.
        # 단일 함수가 사용 중인 분모에 맞춰 아래 한 줄을 선택하세요.
        # 1) enriched / (missing + enriched)
        denom = (doi_missing_sum or 0) + (doi_enriched_sum or 0)
        # 2) (단일이 'missing'만을 분모로 쓰는 경우)
        # denom = (doi_missing_sum or 0)
        doi_rate = (100.0 * (doi_enriched_sum or 0) / denom) if denom else 0.0
    except Exception:
        doi_rate = None

    # (B) ROR 보강률: 합산된 분자/분모 기준
    #    - 분자: ror_enriched_sum 이 있으면 사용
    #    - 없으면 (보강 전 결측 - 보강 후 결측)을 사용 (단일과 동일한 fallback)
    ror_enriched_calc = ror_enriched_sum
    if ror_enriched_calc in (None, 0) and (ror_missing_before_total is not None) and (ror_missing_after is not None):
        ror_enriched_calc = max(0, ror_missing_before_total - ror_missing_after)

    ror_rate = None
    try:
        # 단일 계산과 동일한 분모를 사용하세요.
        # 1) enriched / (missing + enriched)
        denom_r = (acc.get("ror_missing") or 0) + (ror_enriched_calc or 0)
        # 2) (단일이 'missing'만을 분모로 쓰는 경우)
        # denom_r = (acc.get("ror_missing") or 0)
        ror_rate = (100.0 * (ror_enriched_calc or 0) / denom_r) if denom_r else 0.0
    except Exception:
        ror_rate = None

    # 6) 단일 Summary와 '동일' 컬럼/순서/포맷으로 행 구성
    data = {
        "전체 수집 논문 수": acc.get("total_collected"),
        "Editorial Material 삭제 수": acc.get("authorships_removed_empty_list"),
        "DOI 결측 수": doi_missing_sum,
        "DOI 보강 수(합산)": (doi_enriched_sum or 0) if doi_enriched_sum is not None else None,
        "DOI 보강률": (None if doi_rate is None else _fmt_pct(doi_rate)),
        "ROR ID 결측 수": ror_missing_before_total,
        "ROR ID 보강 수": ror_enriched_calc,
        "ROR ID 보강률": (None if ror_rate is None else _fmt_pct(ror_rate)),
        "노드 수": node_count,
        "엣지 수": edge_count,
    }
    df_out = pd.DataFrame([data])

    return df_out


# === 새 헬퍼: 공통 요약표 ===
def _summary_metrics_common(final_csv_path: str):
    import csv, json, re
    from pathlib import Path
    import pandas as pd

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

    # ---- 1) metrics.csv 합산 함수 (이제 정의 → 이후 호출)
    def sum_metrics(keys, search_dirs, allowed_names=None):
        acc = {k: 0 for k in keys if not k.endswith("_rate")}
        rate_keys = [k for k in keys if k.endswith("_rate")]
        found = False
        seen = set()

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
                if allowed_names and (mf.name not in allowed_names):
                    continue
                rp = mf.resolve()
                if rp in seen:
                    continue
                seen.add(rp)
                try:
                    with mf.open("r", encoding="utf-8", newline="") as f:
                        rdr = csv.reader(f)
                        _ = next(rdr, None)  # ["key","value"] 헤더 스킵 허용
                        for row in rdr:
                            if len(row) < 2:
                                continue
                            k, v = row[0], _to_num(row[1])
                            if (k in acc) and (v is not None) and (not k.endswith("_rate")):
                                acc[k] += v
                    found = True
                except Exception:
                    pass

        # 합계로 비율 재계산: (enriched / (missing + enriched)) × 100
        if "doi_enriched" in acc and "doi_missing" in acc and "doi_enrich_rate" in rate_keys:
            denom = (acc["doi_missing"] or 0) + (acc["doi_enriched"] or 0)
            acc["doi_enrich_rate"] = round(100.0 * (acc["doi_enriched"] or 0) / denom, 2) if denom else 0.0

        if "ror_enriched" in acc and "ror_missing" in acc and "ror_enrich_rate" in rate_keys:
            denom = (acc["ror_missing"] or 0) + (acc["ror_enriched"] or 0)
            acc["ror_enrich_rate"] = round(100.0 * (acc["ror_enriched"] or 0) / denom, 2) if denom else 0.0

        # real4 전/후 결측이 있으면 파생치로 ror_enriched 보정
        if "ror_missing_before_extract" in acc and "ror_missing_after_extract" in acc:
            fixed = max(0, (acc["ror_missing_before_extract"] or 0) - (acc["ror_missing_after_extract"] or 0))
            acc.setdefault("ror_enriched", 0)
            acc["ror_enriched"] += fixed
            acc.setdefault("ror_missing", 0)
            acc["ror_missing"] = max(0, (acc["ror_missing"] or 0) - fixed)
            denom = (acc["ror_missing"] or 0) + (acc["ror_enriched"] or 0)
            if "ror_enrich_rate" in rate_keys:
                acc["ror_enrich_rate"] = round(100.0 * (acc["ror_enriched"] or 0) / denom, 2) if denom else 0.0

        return acc, found

    # ---- 2) metrics.csv 합산 (이번 실행의 파일명만 허용)
    allowed_names = set()
    if prefix_all and (y0 is not None) and (y1 is not None):
        allowed_names = {f"{prefix_all}_{yy}_{yy}_metrics.csv" for yy in range(int(y0), int(y1) + 1)}

    acc, found = sum_metrics([
        "total_collected","final_csv_rows","authorships_removed_empty_list",
        "doi_missing","doi_enriched","doi_enrich_rate",
        "ror_missing","ror_enriched","ror_enrich_rate",
        "ror_missing_after_extract",
    ], search_dirs, allowed_names=allowed_names)

    # ---- 3) 합산 결과 변수들
    total_collected = acc.get("total_collected") if found else None
    removed_authorships_empty = acc.get("authorships_removed_empty_list") if found else None
    doi_missing_sum   = acc.get("doi_missing") if found else None
    doi_enriched_sum  = acc.get("doi_enriched") if found else None
    doi_rate_from_acc = acc.get("doi_enrich_rate") if found else None
    ror_missing_sum   = acc.get("ror_missing") if found else None
    ror_enriched_sum  = acc.get("ror_enriched") if found else None
    ror_rate_from_acc = acc.get("ror_enrich_rate") if found else None
    ror_missing_after = acc.get("ror_missing_after_extract") if found else None

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

    # ROR 보강 수/보강률 폴백 계산
    _ror_enriched_fallback = (None if (ror_missing_before is None or ror_missing_after is None)
                              else max(0, ror_missing_before - ror_missing_after))
    _ror_enrich_rate_fallback = (None if (ror_missing_before in (None, 0)) else
                                 ((_ror_enriched_fallback or 0) / ror_missing_before * 100))

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

    # ---- 10) 출력 테이블 구성 + 포맷
    df_out = pd.DataFrame([{
        "전체 수집 논문 수": total_collected,
        "Editorial Material 삭제 수": removed_authorships_empty,
        "DOI 결측 수": doi_missing_show,
        "DOI 보강 수(합산)": (0 if doi_enriched_sum is None else doi_enriched_sum),
        "DOI 보강률": (
            doi_rate_calc if doi_rate_calc is not None
            else (None if doi_rate_from_acc is None else round(float(doi_rate_from_acc), 2))
        ),
        "ROR ID 결측 수": ror_missing_before,
        "ROR ID 보강 수(합산)": (ror_enriched_sum if ror_enriched_sum is not None else _ror_enriched_fallback),
        "ROR ID 보강률": (None if (ror_rate_from_acc is None and _ror_enrich_rate_fallback is None)
                          else round(float(ror_rate_from_acc if ror_rate_from_acc is not None else _ror_enrich_rate_fallback), 2)),
        "노드 수": node_count,
        "엣지 수": edge_count,
    }])

    def _fmt_pct(v):
        try:
            return f"{float(v):.2f}%"
        except Exception:
            return "0.00%"

    for col in ["DOI 보강률", "ROR ID 보강률"]:
        if col in df_out.columns:
            df_out[col] = df_out[col].apply(_fmt_pct)

    for col in ["전체 수집 논문 수","Editorial Material 삭제 수","DOI 결측 수","DOI 보강 수(합산)",
                "ROR ID 결측 수","ROR ID 보강 수(합산)","노드 수","엣지 수"]:
        if col in df_out.columns:
            df_out[col] = df_out[col].apply(lambda v: 0 if v is None else v)

    return df_out


# === 새 헬퍼: 중심성별 Top 노드 & 엣지 표 ===
def _summary_top_nodes_and_edges(final_csv_path: str):

    cent_csv = _find_centrality_csvs(final_csv_path)
    if cent_csv is None:
        # 중앙성 CSV가 없으면 한번 생성 시도 (실패해도 조용히 패스)
        try:
            from new import make_html_from_csv
            make_html_from_csv(final_csv_path)
        except Exception:
            pass
        cent_csv = _find_centrality_csvs(final_csv_path)
    if cent_csv is None:
        return pd.DataFrame(), pd.DataFrame()


    cent = pd.read_csv(cent_csv)
    node_col = "node" if "node" in cent.columns else cent.columns[0]
    candidates = [c for c in ["degree","eigenvector","betweenness","closeness"] if c in cent.columns]
    if not candidates:
        # 노드명만 있는 경우
        return pd.DataFrame(), pd.DataFrame()

    # Top 노드 표 만들기
    top_nodes = {}
    for c in candidates:
        tmp = cent[[node_col, c]].dropna()
        tmp = tmp.sort_values(c, ascending=False)
        names = tmp[node_col].astype(str).head(10).tolist()
        # 이름만 보여달라는 요구에 맞춰 점수는 제외
        top_nodes[c] = [*names, *([""]*(10-len(names)))]

    df_nodes = pd.DataFrame(top_nodes, index=[f"Top {i}" for i in range(1,11)])


    def _pairs_from_org_names(s):
        import ast
        arr = []
        if isinstance(s, str):
            t = s.strip()
            if t:
                try:
                    v = ast.literal_eval(t)
                    arr = list(v) if isinstance(v, (list, tuple)) else [v]
                except Exception:
                    try:
                        v = json.loads(t)
                        arr = v if isinstance(v, list) else [v]
                    except Exception:
                        for sep in [";", "|", ","]:
                            if sep in t:
                                arr = [x.strip() for x in t.split(sep) if x.strip()]
                                break
                        if not arr:
                            arr = [t]
        elif isinstance(s, list):
            arr = s

        arr = [str(a) for a in arr if a]
        pairs=[]
        for i in range(len(arr)):
            for j in range(i+1,len(arr)):
                a,b = sorted((arr[i],arr[j]))
                pairs.append((a,b))
        return pairs


    final_df = pd.read_csv(final_csv_path)
    all_pairs=[]
    if "org_names" in final_df.columns:
        final_df["org_names"].astype(str).apply(lambda s: all_pairs.extend(_pairs_from_org_names(s)))

    # 노드 점수 map
    score_map = {c: dict(zip(cent[node_col].astype(str), cent[c])) for c in candidates}

    # 중심성별로 엣지 점수 = 노드 점수 합(간단 근사)
    top_edges = {}
    for c in candidates:
        sm = score_map[c]
        scored=[]
        for a,b in set(all_pairs):
            s = float(sm.get(a,0)) + float(sm.get(b,0))
            scored.append((s, f"{a} - {b}"))
        scored.sort(key=lambda x:x[0], reverse=True)
        names = [name for _,name in scored[:10]]
        top_edges[c] = [*names, *([""]*(10-len(names)))]
    df_edges = pd.DataFrame(top_edges, index=[f"Top {i}" for i in range(1,11)])
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
    def _render_summary_block(sel_csv: str):
        with st.expander("Summary", expanded=True):
            # 1) 공통 요약표
            st.subheader("공통 요약 지표")
            summary_df = _summary_metrics_common(sel_csv)
            st.dataframe(summary_df, width="stretch")

            # 2) Summary CSV 다운로드 (index=False, UTF-8-SIG)
            csv_bytes = summary_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "다운로드 Summary CSV",
                data=csv_bytes,
                file_name=Path(sel_csv).stem + "_summary.csv",
                mime="text/csv",
            )

            # 3) 중심성별 Top 10 노드&엣지 (결합 테이블)
            st.subheader("중심성별 Top 10 노드&엣지")
            top_nodes, top_edges = _summary_top_nodes_and_edges(sel_csv)
            if not top_nodes.empty:
                combined = pd.DataFrame(index=top_nodes.index)
                for c in top_nodes.columns:
                    combined[(c, "노드")] = top_nodes[c]
                    combined[(c, "엣지")] = top_edges[c] if c in top_edges.columns else ""
                combined.columns = [f"{m} · {sub}" for m, sub in combined.columns]
                st.dataframe(combined, width="stretch")
            else:
                st.info("중심성 CSV를 찾지 못해 Top 노드/엣지를 표시할 수 없습니다.")

    def _render_summary_unified(sel):
        # sel: 현재 사이드바에서 선택된 실행 dict(기존 코드와 동일)
        all_csvs = [r["csv"] for r in st.session_state.runs] if "runs" in st.session_state else [sel["csv"]]
        if len(all_csvs) > 1:
            # 여러 실행 로드 → 합산 Summary를 '기존 테이블 스키마 그대로' 출력
            df = _summary_metrics_common_multi(all_csvs)  # 이전에 추가한 합산 함수
            st.subheader("Summary")
            st.dataframe(df, use_container_width=True)
            st.download_button(
                "다운로드 Summary CSV",
                data=df.to_csv(index=False).encode("utf-8-sig"),
                file_name="summary.csv",
                mime="text/csv",
            )
        else:
            # 단일 실행 → 기존 단일 Summary 그대로
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
