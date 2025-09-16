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
    import pandas as pd, json, re
    from pathlib import Path
    final_csv = Path(final_csv_path)
    base_path = final_csv.with_suffix("")
    base = str(base_path)

    json_merged = Path(f"{base}.json")
    csv_file    = Path(f"{base}.csv")
    ror_ex_csv  = Path(f"{base}_ror_extract.csv")

    # 연도·접두사 추출 (병합 케이스 대비)
    m = re.search(r"_(\d{4})_(\d{4})$", base)
    y0 = y1 = None
    prefixes = []
    if m:
        y0, y1 = int(m.group(1)), int(m.group(2))
        prefix_all = Path(base).name[:-(len(m.group(0)))]
        prefixes = prefix_all.split("-") if prefix_all else []

    def sum_metrics(keys):
        acc = {k: 0 for k in keys}; found = False
        if y0 is None or y1 is None or not prefixes:
            return acc, found
        for y in range(y0, y1+1):
            for px in prefixes:
                mf = Path(f"{px}_{y}_{y}_metrics.json")
                if mf.exists():
                    try:
                        mm = json.loads(mf.read_text(encoding="utf-8")) or {}
                        for k in keys:
                            v = mm.get(k)
                            if v is not None:
                                acc[k] += int(v) if isinstance(v,(int,float)) else 0
                        found = True
                    except Exception:
                        pass
        return acc, found

    # 1) 전체 수집 수 & authorships=[] 삭제 수
    total_collected = None
    removed_authorships_empty = None
    if json_merged.exists():
        try:
            works = json.loads(json_merged.read_text(encoding="utf-8"))
            total_collected = len(works)
            removed_authorships_empty = sum(
                1 for w in works
                if not isinstance(w.get("authorships"), list) or len(w.get("authorships") or []) == 0
            )
        except Exception:
            pass
    else:
        acc, found = sum_metrics(["json_rows", "authorships_removed_empty_list"])
        if found:
            total_collected = acc["json_rows"]
            removed_authorships_empty = acc["authorships_removed_empty_list"]

    # 2) DOI 결측(최종) + 보강(합산) + 보강률
    doi_missing_final = None
    if csv_file.exists():
        try:
            df_csv = pd.read_csv(csv_file, dtype={"doi": str})
            doi_missing_final = df_csv["doi"].isna().sum() + (df_csv["doi"].astype(str).str.strip() == "").sum()
        except Exception:
            pass
    acc, found = sum_metrics(["doi_missing_initial", "doi_enriched_from_empty"])
    doi_missing_initial = acc["doi_missing_initial"] if found else None
    doi_enriched = acc["doi_enriched_from_empty"] if found else None
    doi_enrich_rate = (doi_enriched / doi_missing_initial) if (doi_missing_initial and doi_missing_initial>0) else None

    # 3) ROR 결측(전/후) + 보강 + 보강률
    rx = re.compile(r'https?://ror\.org/[0-9a-z]+', re.IGNORECASE)
    def _extract_ror_list(s):
        if not isinstance(s,str):
            return []
        return list(dict.fromkeys(rx.findall(s)))

    ror_missing_before = None
    if csv_file.exists():
        try:
            tmp = pd.read_csv(csv_file, dtype={"authorships": str})
            if "authorships" in tmp.columns:
                ror_before = tmp["authorships"].apply(_extract_ror_list)
                ror_missing_before = int((ror_before.str.len() == 0).sum())
        except Exception:
            pass

    ror_missing_after = None
    acc, found = sum_metrics(["ror_missing_after_extract"])
    if found:
        ror_missing_after = acc["ror_missing_after_extract"]

    ror_augmented_rows = None
    ror_augment_rate = None
    if (ror_missing_before is not None) and (ror_missing_after is not None):
        ror_augmented_rows = max(0, ror_missing_before - ror_missing_after)
        ror_augment_rate = (ror_augmented_rows / ror_missing_before) if ror_missing_before>0 else None

    # 4) 노드·엣지 수 (공통값) — centrality 무관
    #    org_names에서 기관쌍 생성, 중앙성 CSV 있으면 해당 노드 집합으로 필터
    cent_csv = _find_centrality_csvs(final_csv_path)
    def _to_pairs(names):
        if not isinstance(names, str):
            return []
        try:
            arr = json.loads(names)
            if not isinstance(arr, list):
                return []
        except Exception:
            return []
        arr = [str(a) for a in arr if a]
        pairs=[]
        for i in range(len(arr)):
            for j in range(i+1,len(arr)):
                a,b = sorted((arr[i],arr[j]))
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

    # 표 구성
    df_out = pd.DataFrame([{
        "전체 수집 논문 수": total_collected,
        "authorships==[] 삭제 수": removed_authorships_empty,
        "DOI 결측 수(최종)": doi_missing_final,
        "DOI 보강 수(합산)": doi_enriched,
        "DOI 보강률(결측 대비)": (None if doi_enrich_rate is None else round(doi_enrich_rate*100,2)),
        "ROR 결측 수(매핑 전)": ror_missing_before,
        "ROR 결측 수(매핑 후)": ror_missing_after,
        "ROR 보강 수(행 기준)": ror_augmented_rows,
        "ROR 보강률(결측 대비)": (None if ror_augment_rate is None else round(ror_augment_rate*100,2)),
        "노드 수(기관 수)": node_count,
        "엣지 수(협업 수)": edge_count,
    }])
    # 포맷
    for col in ["DOI 보강률(결측 대비)","ROR 보강률(결측 대비)"]:
        if col in df_out.columns:
            df_out[col] = df_out[col].apply(lambda v: "-" if v is None else f"{v:.2f}%")
    for col in ["전체 수집 논문 수","authorships==[] 삭제 수","DOI 결측 수(최종)","DOI 보강 수(합산)",
                "ROR 결측 수(매핑 전)","ROR 결측 수(매핑 후)","ROR 보강 수(행 기준)",
                "노드 수(기관 수)","엣지 수(협업 수)"]:
        if col in df_out.columns:
            df_out[col] = df_out[col].apply(lambda v: 0 if v is None else v)
    return df_out

# === 새 헬퍼: 중심성별 Top 노드 & 엣지 표 ===
def _summary_top_nodes_and_edges(final_csv_path: str):
    import pandas as pd, json
    from pathlib import Path
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

    # 엣지 후보: 최종 CSV org_names에서 모든 쌍 만들기
    def _pairs_from_org_names(s):
        try:
            arr = json.loads(s) if isinstance(s,str) else []
            arr = [str(a) for a in arr if a]
        except Exception:
            arr = []
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
                                      "duration": str(duration)
                                      })

        # ✅ 이력 드롭다운(기본값=방금 실행한 항목)
        options = [r["label"] for r in st.session_state.runs]
        selected = st.selectbox("실행 결과 선택", options, index=len(options)-1)
        sel = next(r for r in st.session_state.runs if r["label"] == selected)

        df = pd.read_csv(sel["csv"])
        st.success("파이프라인 완료! 선택한 실행 결과를 아래에서 확인하세요.")

        # ✅ 상위 1000행만 화면에 프리뷰
        total_rows = len(df)
        st.caption(
            f"⚠️ 아래 표는 총 {total_rows:,}행 중 "
            f"상위 {min(PREVIEW_N, total_rows):,}행만 미리보기입니다. "
            "전체는 아래에서 다운로드하세요."
        )
        st.dataframe(df.head(PREVIEW_N), width="stretch")

        # 사이드바에 실행 시간 표시
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
            # Degree 기반 (크기=degree, 색=eigenvector)
            deg_html = make_html_string_from_csv(str(csv_path), size_by="degree", color_by="degree")
            st.download_button(
                label="Visualization Download (Degree-based)",
                data=deg_html.encode("utf-8"),
                file_name=csv_path.stem + "_degree_network.html",
                mime="text/html",
                width="stretch",
            )

            # Eigenvector 기반 (크기=eigenvector, 색=degree)
            eig_html = make_html_string_from_csv(str(csv_path), size_by="eigenvector", color_by="eigenvector")
            st.download_button(
                label="Visualization Download (Eigenvector-based)",
                data=eig_html.encode("utf-8"),
                file_name=csv_path.stem + "_eigenvector_network.html",
                mime="text/html",
                width="stretch",
            )

        # 실행 끝나면 요약 지표 테이블 출력
        with st.expander("Summary", expanded=True):
            # 1) 공통 요약표 (중심성과 무관 — 1개 표)
            st.subheader("공통 요약 지표")
            st.dataframe(_summary_metrics_common(sel["csv"]), width="stretch")

            # 2) 중심성별 Top 10 노드 + 3) 중심성별 Top 10 엣지
            st.subheader("중심성별 Top 10 노드")
            top_nodes, top_edges = _summary_top_nodes_and_edges(sel["csv"])
            if not top_nodes.empty:
                st.dataframe(top_nodes, width="stretch")
            else:
                st.info("중심성 CSV를 찾지 못해 Top 노드를 표시할 수 없습니다.")

            st.subheader("중심성별 Top 10 엣지")
            if not top_edges.empty:
                st.dataframe(top_edges, width="stretch")
            else:
                st.info("중심성 CSV를 찾지 못해 Top 엣지를 표시할 수 없습니다.")




    else:
        if st.session_state.runs:
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
                deg_html = make_html_string_from_csv(str(csv_path), size_by="degree", color_by="eigenvector")
                st.download_button(
                    label="Visualization Download (Degree-based)",
                    data=deg_html.encode("utf-8"),
                    file_name=csv_path.stem + "_degree_network.html",
                    mime="text/html",
                    width="stretch",
                )

                #Eigenvector 기반
                eig_html = make_html_string_from_csv(str(csv_path), size_by="eigenvector", color_by="degree")
                st.download_button(
                    label="Visualization Download (Eigenvector-based)",
                    data=eig_html.encode("utf-8"),
                    file_name=csv_path.stem + "_eigenvector_network.html",
                    mime="text/html",
                    width="stretch",
                )
        else:
            st.info("사이드바에서 설정 후 🚀 파이프라인 실행 버튼을 눌러주세요.")

if __name__ == "__main__":
    app()
