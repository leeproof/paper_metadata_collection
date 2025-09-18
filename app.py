import os
os.environ["STREAMLIT_SERVER_FILE_WATCHER_TYPE"] = "none"

import streamlit as st
import nest_asyncio
import pandas as pd
import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from io import BytesIO
import zipfile
import json
import re

from new import run_pipeline, make_html_from_csv, run_pipeline_cached, make_html_string_from_csv

# 한국 시간대 설정
now_kst = datetime.datetime.now(ZoneInfo("Asia/Seoul"))

# 이벤트 루프 충돌 방지 (async 사용 시 필수)
nest_asyncio.apply()


st.set_page_config(page_title="OpenAlex Paper Metadata Pipeline", layout="wide")




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

# === 새 헬퍼: 공통 요약표 ===
def _summary_metrics_common(final_csv_path: str):

    final_csv = Path(final_csv_path)
    base_path = final_csv.with_suffix("")
    base = str(base_path)

    root_base = re.sub(r'_(?:ror(?:_extract(?:_name)?)?)(?:_summary)?$', '', base)


    json_merged = Path(f"{root_base}.json")
    csv_file    = Path(final_csv)

    # export 파일일 경우, 수집량을 폴백하지 않도록 설정
    is_export = final_csv.name.endswith("_export.csv") or final_csv.name.endswith("_summary.csv")

    # 반드시 root_base로 연/접두사 추출(접미사 섞임 방지)
    m = re.search(r"_(\d{4})_(\d{4})$", root_base)
    y0 = y1 = None
    prefixes = []
    prefix_all = ""
    if m:
        y0, y1 = int(m.group(1)), int(m.group(2))
        prefix_all = Path(root_base).name[:-(len(m.group(0)))]
        prefixes = prefix_all.split("-") if prefix_all else []

    def sum_metrics(keys):
        acc = {k: 0 for k in keys}; found = False
        if y0 is None or y1 is None:
            return acc, found
        
        #metrics -> csv와 같은 폴더에 존재, 그 기준으로 탐색
        root_dir = Path(root_base).parent
        search_dirs = [root_dir, root_dir / "workdir_tmp"]

        # 0) 기간 통합 metrics — 성공하면 즉시 반환(중복 합산 방지)
        if prefix_all:
            for d in search_dirs:
                merged = d / f"{Path(prefix_all).name}_{y0}_{y1}_metrics.json"
                if merged.exists():
                    try:
                        mm = json.loads(merged.read_text(encoding="utf-8")) or {}
                        for k in keys:
                            v = mm.get(k)
                            if v is None:
                                continue
                            if k.endswith("_rate"):
                                acc[k] = float(v)        # 퍼센트는 합산 금지
                            else:
                                if isinstance(v, (int, float)):
                                    acc[k] += int(v)      # 카운트만 합산
                        return acc, True
                    except Exception:
                        pass

        # 1) 연도별 합산 — prefix 중복 제외(px == prefix_all는 건너뜀)
        for y in range(y0, y1 + 1):
            for d in search_dirs:
                candidates = []
                if prefix_all:
                    candidates.append(d / f"{Path(prefix_all).name}_{y}_{y}_metrics.json")
                for px in prefixes:
                    if px and px != prefix_all:
                        candidates.append(d / f"{Path(px).name}_{y}_{y}_metrics.json")

                for mf in candidates:
                    if mf.exists():
                        try:
                            mm = json.loads(mf.read_text(encoding="utf-8")) or {}
                            for k in keys:
                                v = mm.get(k)
                                if v is None:
                                    continue
                                if k.endswith("_rate"):
                                    acc[k] = float(v)
                                else:
                                    if isinstance(v, (int, float)):
                                        acc[k] += int(v)
                            found = True
                        except Exception:
                            pass
        return acc, found

    
    # 1) 전체 수집 수 & authorships=[] 삭제 수 —— metrics → JSON → (비-export/summary일 때만) CSV
    doi_missing = doi_enriched = doi_enrich_rate = None
    ror_missing = ror_enriched = ror_enrich_rate = None

    total_collected = None
    removed_authorships_empty = None

    # (A) metrics 우선
    acc, found = sum_metrics([
        "total_collected",
        "final_csv_rows",
        "authorships_removed_empty_list",
        "doi_missing", "doi_enriched", "doi_enrich_rate",
        "ror_missing", "ror_enriched", "ror_enrich_rate",
        "ror_missing_after_extract",
    ])
    if found:
        total_collected = acc.get("total_collected")
        removed_authorships_empty = acc.get("authorships_removed_empty_list", 0)
        doi_missing = acc.get("doi_missing", None)
        doi_enriched = acc.get("doi_enriched", None)
        doi_enrich_rate = acc.get("doi_enrich_rate", None)
        ror_missing = acc.get("ror_missing", None)
        ror_enriched = acc.get("ror_enriched", None)
        ror_enrich_rate = acc.get("ror_enrich_rate", None)
        ror_missing_after = acc.get("ror_missing_after_extract", None)

    # (B) metrics 없으면 JSON 합본에서 추론
    if total_collected is None and json_merged.exists():
        try:
            works = json.loads(json_merged.read_text(encoding="utf-8")) or []
            total_collected = len(works)
            if removed_authorships_empty is None:
                removed_authorships_empty = sum(
                    1 for w in works
                    if not isinstance(w.get("authorships"), list) or len(w.get("authorships") or []) == 0
                )
        except Exception:
            pass

    # (C) 최후 폴백: CSV 길이 — 단, export/summary 파일은 금지
    if total_collected is None and not is_export:
        try:
            if csv_file.exists():
                total_collected = len(pd.read_csv(csv_file))
        except Exception:
            pass

    # 2) DOI 결측(최종) + 보강(합산) + 보강률
    doi_missing_final = None
    if csv_file.exists():
        try:
            df_csv = pd.read_csv(csv_file, dtype={"doi": str})
            doi_missing_final = df_csv["doi"].isna().sum() + (df_csv["doi"].astype(str).str.strip() == "").sum()
        except Exception:
            pass

        
    # 3) ROR 결측(전/후) + 보강 + 보강률

    # 3-1) '매핑 전 결측'(before)은 CSV에서 authorships의 ROR URL 유무로 집계
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

    # 3-2) 보강 수/률 계산
    ror_augmented_rows = ror_augment_rate = None
    if (ror_missing_before is not None) and (ror_missing_after is not None):
        ror_augmented_rows = max(0, ror_missing_before - ror_missing_after)
        ror_augment_rate = (
            (ror_augmented_rows / ror_missing_before) if ror_missing_before > 0 else None
        )
    # 4) 노드·엣지 수 (공통값) — centrality 무관
    #    org_names에서 기관쌍 생성, 중앙성 CSV 있으면 해당 노드 집합으로 필터
    cent_csv = _find_centrality_csvs(final_csv_path)
    def _to_pairs(names):
        import ast
        arr = []
        if isinstance(names, str):
            s = names.strip()
            if s:
                try:
                    v = ast.literal_eval(s)
                    if isinstance(v, (list, tuple)):
                        arr = list(v)
                    else:
                        arr = [v]
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
        if cent_csv is not None:
            cent = pd.read_csv(cent_csv)
            node_col = "node" if "node" in cent.columns else cent.columns[0]
            node_set = set(cent[node_col].astype(str).tolist())
            node_count = len(node_set)
            edge_count = len({(a,b) for (a,b) in all_pairs if a in node_set and b in node_set})
        else:
            nodes = {n for p in all_pairs for n in p}
            node_count = len(nodes)
            edge_count = len(set(all_pairs))
    except Exception:
        node_count = edge_count = None

    _ror_enriched_fallback = (None if (ror_missing_before is None or ror_missing_after is None)
                          else max(0, ror_missing_before - ror_missing_after))
    _ror_enrich_rate_fallback = (None if (ror_missing_before in (None, 0)) else
                             ((_ror_enriched_fallback or 0) / ror_missing_before * 100))


    # 표 구성
    df_out = pd.DataFrame([{
        # ← 요구한 10개 항목 “정확히” 매핑
        "전체 수집 논문 수": total_collected,
        "authorships==[] 삭제 수": removed_authorships_empty,
        "doi 결측 수": doi_missing_final,  # CSV 기준 최종 결측
        "doi 보강 수(합산)": (0 if doi_enriched is None else doi_enriched),
        "doi 보강률": (None if doi_enrich_rate is None else round(float(doi_enrich_rate), 2)),

        "ror 결측 수": ror_missing_before,  # 매핑 전 결측(보고 지표로 일관)
        "ror 보강 수(합산)": (ror_enriched if ror_enriched is not None else _ror_enriched_fallback),
        "ror 보강률": (None if (ror_enrich_rate is None and _ror_enrich_rate_fallback is None)
                    else round(float(ror_enrich_rate if ror_enrich_rate is not None else _ror_enrich_rate_fallback), 2)),

        "노드 수": node_count,
        "엣지 수": edge_count,
    }])

    def _fmt_pct(v):
        try:
            return f"{float(v):.2f}%"
        except Exception:
            return "0.00%"

    # 퍼센트 표기 컬럼 포맷 — “정확히 같은 라벨”만
    for col in ["doi 보강률", "ror 보강률"]:
        if col in df_out.columns:
            df_out[col] = df_out[col].apply(_fmt_pct)

    # 숫자 기본값 보정 — “정확히 같은 라벨”만
    for col in ["전체 수집 논문 수","authorships==[] 삭제 수","doi 결측 수","doi 보강 수(합산)",
                "ror 결측 수","ror 보강 수(합산)","노드 수","엣지 수"]:
        if col in df_out.columns:
            df_out[col] = df_out[col].apply(lambda v: 0 if v is None else v)
    return df_out

# === 새 헬퍼: 중심성별 Top 노드 & 엣지 표 ===
def _summary_top_nodes_and_edges(final_csv_path: str):

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
    st.title("Paper Metadata Collection Pipeline")

    if "runs" not in st.session_state:
        st.session_state.runs = []

    issns, year_start, year_end, email, run_btn = sidebar_controls()

    # 진행률/메시지 슬롯 -> 진행중인 상황 출력
    progress_bar = st.progress(0, text="대기 중")
    progress_text = st.empty()

    # 미리보기 행 수(고정)
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
                "Download Summary CSV",
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

    if run_btn:
        # 실행 시간 기록
        start_time = datetime.datetime.now(ZoneInfo("Asia/Seoul"))

        with st.spinner("파이프라인 실행 중... (시간이 걸릴 수 있어요)"):
            progress_text.info("준비 중..")
            final_csv, final_html = cached_run(issns, year_start, year_end, email,
                                               progress_bar=progress_bar, progress_text=progress_text
                                               )
        
        # 종료 시간 기록
        end_time = datetime.datetime.now(ZoneInfo("Asia/Seoul"))
        duration = end_time - start_time
        
        # 세션 이력에 기록 (라벨: issn|연도|시각)
        prefix = Path(final_csv).stem.replace(f"_{year_start}_{year_end}_ror_extract_name", "")
        label = f"{prefix} | {year_start}-{year_end} | {start_time.strftime('%m/%d %H:%M')}"
        st.session_state.runs.append({"label": label, "csv": final_csv, "html": final_html,
                                      "start": start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                      "end": end_time.strftime('%Y-%m-%d %H:%M:%S'),
                                      "duration": str(duration)})
        
        # 가장 최근 실행 선택
        sel = st.session_state.runs[-1]
        st.success(f"실행 완료: {sel['label']}")
        st.sidebar.markdown("---")
        st.sidebar.subheader("실행 시간")
        st.sidebar.write(f" 시작 {sel['start']}")
        st.sidebar.write(f" 종료 {sel['end']}")
        st.sidebar.write(f" 소요 {sel['duration']}")

        # ✅ CSV 다운로드: 임계값 초과 시 ZIP
        csv_path = Path(sel["csv"])
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)

        # html 경로
        html_path = Path(sel["html"])

        col_csv, col_html = st.columns(2)
        with col_csv:
            if file_size_mb <= THRESHOLD_MB:
                st.download_button(
                    label=f"CSV Download ({file_size_mb:.1f} MB)",
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
                    label=f"CSV Download (ZIP, 원본 {file_size_mb:.1f} MB)",
                    data=zip_bytes,
                    file_name=csv_path.stem + ".zip",
                    mime="application/zip",
                    width="stretch",
                )

        with col_html:
            # Degree 기반
            deg_html = make_html_string_from_csv(str(csv_path), size_by="degree", color_by="degree")
            st.download_button(
                label="Visualization Download (Degree-based)",
                data=deg_html.encode("utf-8"),
                file_name=csv_path.stem + "_degree_network.html",
                mime="text/html",
                width="stretch",
            )

            # Eigenvector 기반
            eig_html = make_html_string_from_csv(str(csv_path), size_by="eigenvector", color_by="eigenvector")
            st.download_button(
                label="Visualization Download (Eigenvector-based)",
                data=eig_html.encode("utf-8"),
                file_name=csv_path.stem + "_eigenvector_network.html",
                mime="text/html",
                width="stretch",
            )

        # ✅ 여기서 공통 Summary 블록 렌더링 (다운로드 후 rerun 되어도 항상 붙음)
        _render_summary_block(sel["csv"])

    else:
        # 이전 실행 이력 선택/표시
        if st.session_state.runs:
            st.markdown("### 이전 실행 결과 보기")
            options = [r["label"] for r in st.session_state.runs]
            selected = st.selectbox("실행 결과 선택", options, index=len(options)-1)
            sel = next(r for r in st.session_state.runs if r["label"] == selected)

            # ✅ 선택된 실행의 시간 로그 -> 사이드바에 표시 (드롭다운 변경 시에도 유지)
            st.sidebar.markdown("---")
            st.sidebar.subheader("실행 시간")
            st.sidebar.write(f" 시작 {sel['start']}")
            st.sidebar.write(f" 종료 {sel['end']}")
            st.sidebar.write(f" 소요 {sel['duration']}")

            df = pd.read_csv(sel["csv"])

            # ✅ 상위 1000행만 화면에 프리뷰
            total_rows = len(df)
            st.caption(
                f"⚠️ 아래 표는 총 {total_rows:,}행 중 "
                f"상위 {min(PREVIEW_N, total_rows):,}행만 미리보기입니다. "
                "전체는 아래에서 다운로드하세요."
            )
            st.dataframe(df.head(PREVIEW_N), width="stretch")

            # ✅ CSV 다운로드: 임계값 초과 시 ZIP
            csv_path = Path(sel["csv"])
            file_size_mb = csv_path.stat().st_size / (1024 * 1024)

            # html 경로
            html_path = Path(sel["html"])

            col_csv, col_html = st.columns(2)
            with col_csv:
                if file_size_mb <= THRESHOLD_MB:
                    st.download_button(
                        label=f"CSV Download ({file_size_mb:.1f} MB)",
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
                        label=f"CSV Download (ZIP, 원본 {file_size_mb:.1f} MB)",
                        data=zip_bytes,
                        file_name=csv_path.stem + ".zip",
                        mime="application/zip",
                        width="stretch",
                    )

            with col_html:
                #Degree 기반
                deg_html = make_html_string_from_csv(str(csv_path), size_by="degree", color_by="degree")
                st.download_button(
                    label="Visualization Download (Degree-based)",
                    data=deg_html.encode("utf-8"),
                    file_name=csv_path.stem + "_degree_network.html",
                    mime="text/html",
                    width="stretch",
                )

                #Eigenvector 기반
                eig_html = make_html_string_from_csv(str(csv_path), size_by="eigenvector", color_by="eigenvector")
                st.download_button(
                    label="Visualization Download (Eigenvector-based)",
                    data=eig_html.encode("utf-8"),
                    file_name=csv_path.stem + "_eigenvector_network.html",
                    mime="text/html",
                    width="stretch",
                )

            # ✅ 이전 실행 보기에서도 공통 Summary 블록을 동일하게 렌더링
            _render_summary_block(sel["csv"])
        else:
            st.info("사이드바에서 설정 후 🚀 파이프라인 실행 버튼을 눌러주세요.")


if __name__ == "__main__":
    app()
