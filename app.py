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
import re
import ast


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

# === 새 헬퍼: 공통 요약표 ===
# app.py
def _summary_metrics_common(final_csv_path: str):
    final_csv = Path(final_csv_path)
    root_base = re.sub(r'_(?:ror(?:_extract(?:_name)?)?)(?:_summary)?$', '', final_csv.stem)

    # 1. 모든 metrics.json 파일에서 통계 수치 집계
    def sum_metrics(keys):
        acc = {k: 0 for k in keys}
        # 탐색 경로를 단순화하고 명확하게 설정
        search_dirs = [final_csv.parent, final_csv.parent / "workdir_tmp"]
        candidates = []
        for d in search_dirs:
            if d.exists():
                candidates.extend(d.rglob(f"{root_base}_metrics.json")) # 파일명을 더 정확하게 지정

        # 중복 방지
        candidates = sorted(list(set(candidates)))

        for mf in candidates:
            try:
                data = json.loads(mf.read_text(encoding="utf-8")) or {}
                for k in keys:
                    v = data.get(k)
                    if v is not None:
                        # 비율(rate)은 마지막 값으로 덮어쓰고, 나머지는 합산
                        if k.endswith("_rate"):
                            acc[k] = float(v)
                        elif isinstance(v, (int, float)):
                            acc[k] += int(v)
            except Exception:
                continue
        return acc

    metric_keys = [
        "total_collected", "authorships_removed_empty_list",
        "doi_missing", "doi_enriched", "doi_enrich_rate",
        "ror_missing", "ror_enriched", "ror_enrich_rate",
        "ror_missing_after_extract"
    ]
    metrics = sum_metrics(metric_keys)

    # 2. CSV에서 직접 계산해야 하는 보조 지표
    node_count = edge_count = ror_missing_before = 0
    try:
        df_final = pd.read_csv(final_csv_path, dtype={'authorships': str, 'org_names': str})

        # ROR ID 결측 수 (매핑 시도 전 기준)
        if "authorships" in df_final.columns:
            rx = re.compile(r'https?://ror\.org/[0-9a-z]+', re.I)
            ror_urls = df_final["authorships"].fillna('').apply(lambda s: rx.findall(s))
            ror_missing_before = int((ror_urls.str.len() == 0).sum())

        # 노드 및 엣지 수
        cent_csv = _find_centrality_csvs(final_csv_path)
        if cent_csv and cent_csv.exists():
            cent_df = pd.read_csv(cent_csv)
            node_col = "org" if "org" in cent_df.columns else cent_df.columns[0]
            node_count = cent_df[node_col].nunique()

            if "org_names" in df_final.columns:
                from itertools import combinations
                node_set = set(cent_df[node_col].astype(str))
                all_edges = set()
                for orgs_str in df_final["org_names"].dropna():
                    try:
                        orgs = ast.literal_eval(orgs_str)
                        # 필터링된 노드만 포함하여 엣지 계산
                        valid_orgs = [o for o in orgs if o in node_set]
                        if len(valid_orgs) >= 2:
                            for u, v in combinations(sorted(list(set(valid_orgs))), 2):
                                all_edges.add((u, v))
                    except Exception:
                        continue
                edge_count = len(all_edges)
    except Exception as e:
        st.warning(f"CSV 기반 지표 계산 중 오류 발생: {e}")

    # 3. 최종 값 결정 (Metrics 우선, 없으면 계산 값으로 대체)
    total_papers = metrics.get("total_collected", 0)
    editorial_removed = metrics.get("authorships_removed_empty_list", 0)
    doi_missing = metrics.get("doi_missing", 0)
    doi_enriched = metrics.get("doi_enriched", 0)
    doi_enrich_rate = metrics.get("doi_enrich_rate", 0.0)
    ror_missing = metrics.get("ror_missing", ror_missing_before) # metrics.json 우선
    ror_enriched = metrics.get("ror_enriched", 0)
    ror_enrich_rate = metrics.get("ror_enrich_rate", 0.0)

    # 보강률 재계산 (합산된 값을 기준으로 하는 것이 더 정확)
    if doi_missing > 0:
        doi_enrich_rate = (doi_enriched / doi_missing) * 100
    if ror_missing > 0:
        ror_enrich_rate = (ror_enriched / ror_missing) * 100

    # 4. 요약 데이터프레임 생성
    summary_data = {
        "전체 수집 논문 수": int(total_papers),
        "Editorial Material 삭제 수": int(editorial_removed),
        "DOI 결측 수": int(doi_missing),
        "DOI 보강 수(합산)": int(doi_enriched),
        "DOI 보강률": f"{doi_enrich_rate:.2f}%",
        "ROR ID 결측 수": int(ror_missing),
        "ROR ID 보강 수(합산)": int(ror_enriched),
        "ROR ID 보강률": f"{ror_enrich_rate:.2f}%",
        "노드 수": int(node_count),
        "엣지 수": int(edge_count),
    }

    df_summary = pd.DataFrame([summary_data])
    return df_summary

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
            # Degree 기반
            deg_html = make_html_string_from_csv(str(csv_path), size_by="degree", color_by="degree")
            st.download_button(
                label="Visualization 다운로드 (Degree-based)",
                data=deg_html.encode("utf-8"),
                file_name=csv_path.stem + "_degree_network.html",
                mime="text/html",
                width="stretch",
            )
            # Eigenvector 기반
            eig_html = make_html_string_from_csv(str(csv_path), size_by="eigenvector", color_by="eigenvector")
            st.download_button(
                label="Visualization 다운로드 (Eigenvector-based)",
                data=eig_html.encode("utf-8"),
                file_name=csv_path.stem + "_eigenvector_network.html",
                mime="text/html",
                width="stretch",
            )

    # === 공통 Preview(상위 1,000행) 블록 ===
    def _render_preview_block(sel_csv: str):
        df = pd.read_csv(sel_csv)
        total_rows = len(df)
        st.caption(
            f"⚠️ 아래 표는 총 {total_rows:,}행 중 "
            f"상위 {min(PREVIEW_N, total_rows):,}행만 Preview입니다. "
            "전체는 아래에서 다운로드하세요."
        )
        st.dataframe(df.head(PREVIEW_N), width="stretch")

        # ▼ 네트워크 Preview(임베드) — degree / eigenvector
        with st.expander("네트워크 Preview (Degree / Eigenvector)", expanded=False):
            tabs = st.tabs(["Degree 네트워크", "Eigenvector 네트워크"])

            # HTML 문자열 생성 (기존 헬퍼 재사용)
            deg_html = make_html_string_from_csv(str(Path(sel_csv)), size_by="degree",     color_by="degree")
            eig_html = make_html_string_from_csv(str(Path(sel_csv)), size_by="eigenvector", color_by="eigenvector")

            with tabs[0]:
                components.html(deg_html, height=800, scrolling=True)

            with tabs[1]:
                components.html(eig_html, height=800, scrolling=True)

    # === 실행 여부에 따라 결과 렌더 ===
    if run_btn:
        # 실행 시간 기록
        start_time = datetime.datetime.now(ZoneInfo("Asia/Seoul"))

        with st.spinner("파이프라인 실행 중... (시간이 걸릴 수 있어요)"):
            progress_text.info("준비 중..")
            final_csv, final_html = cached_run(
                issns, year_start, year_end, email,
                progress_bar=progress_bar, progress_text=progress_text
            )

        # 종료 시간 기록
        end_time = datetime.datetime.now(ZoneInfo("Asia/Seoul"))
        duration = end_time - start_time

        # 세션 이력에 기록 (라벨: issn|연도|시각)
        prefix = Path(final_csv).stem.replace(f"_{year_start}_{year_end}_ror_extract_name", "")
        label = f"{prefix} | {year_start}-{year_end} | {start_time.strftime('%m/%d %H:%M')}"
        st.session_state.runs.append({
            "label": label, "csv": final_csv, "html": final_html,
            "start": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "end": end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "duration": str(duration)
        })

        # 가장 최근 실행 선택
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
            _render_summary_block(sel["csv"])

        # ✅ Summary가 항상 붙도록(다운로드 후 rerun되어도)
        # 보조로 최근 선택 CSV를 세션에 보관
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
                _render_summary_block(sel["csv"])

            # 최근 선택 CSV를 세션에 보관(선택 유지용)
            st.session_state["last_csv"] = sel["csv"]
        else:
            st.info("사이드바에서 설정 후 🚀 파이프라인 실행 버튼을 눌러주세요.")



if __name__ == "__main__":
    app()
