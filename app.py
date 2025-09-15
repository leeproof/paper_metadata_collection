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

def _summary_metrics_table(final_csv_path: str, centrality: str = "eigenvector"):
    """파이프라인 산출물들을 가볍게 읽어 요약지표 테이블(1행 DataFrame) 생성"""
    import pandas as pd
    from pathlib import Path
    import json

    final_csv = Path(final_csv_path)  # ..._ror_extract_name.csv
    base = str(final_csv).replace("_ror_extract_name.csv", "")

    json_merged = Path(f"{base}.json")                  # 수집+DOI보강 후 합본 JSON
    csv_file    = Path(f"{base}.csv")                   # JSON→CSV 변환 (authorships 필터 적용)
    ror_ex_csv  = Path(f"{base}_ror_extract.csv")       # ROR 추출 결과
    cent_csv    = Path(f"{base}_centrality_{centrality}_{centrality}.csv")  # real6가 저장

    # 1) 전체 수집 논문 수 (중복 제거 후)
    total_collected = None
    if json_merged.exists():
        with open(json_merged, "r", encoding="utf-8") as f:
            works = json.load(f)
        total_collected = len(works)

    # 2) authorships == [] 이었던 문헌 삭제 수 (JSON 기준 빠르게 집계)
    removed_authorships_empty = None
    if json_merged.exists():
        removed_authorships_empty = sum(
            1 for w in works
            if not isinstance(w.get("authorships"), list) or len(w.get("authorships") or []) == 0
        )

    # 3) DOI 결측 수(최종) — CSV 기준
    doi_missing_final = None
    if csv_file.exists():
        df_csv = pd.read_csv(csv_file, dtype={"doi": str})
        doi_missing_final = df_csv["doi"].isna().sum() + (df_csv["doi"].astype(str).str.strip() == "").sum()

    # 4) ROR 결측/보강: (매핑 전) csv_file에서 정규식으로 ROR 추출 → (매핑 후) ror_extract.csv와 비교
    ror_missing_before = ror_missing_after = ror_augmented_rows = ror_augment_rate = None
    rx = re.compile(r'https?://ror\.org/[0-9a-z]+', re.IGNORECASE)

    def _extract_ror_list(s):
        if not isinstance(s, str):
            return []
        return list(dict.fromkeys(rx.findall(s)))

    if csv_file.exists():
        tmp = pd.read_csv(csv_file, dtype={"authorships": str})
        ror_before = tmp["authorships"].apply(_extract_ror_list)
        ror_missing_before = (ror_before.str.len() == 0).sum()

    if ror_ex_csv.exists():
        df_ror = pd.read_csv(ror_ex_csv)
        # real4에서 이미 동일 정규식으로 추출해둠
        # (참고: real4는 len==0 개수 로그만 출력하지만 우리는 CSV로 직접 집계)
        ror_missing_after = (df_ror["ror"].apply(lambda x: eval(x) if isinstance(x, str) and x.startswith('[') else (x if isinstance(x, list) else [])).apply(len) == 0).sum()
        if ror_missing_before is not None:
            ror_augmented_rows = max(0, ror_missing_before - ror_missing_after)
            ror_augment_rate = (ror_augmented_rows / ror_missing_before) if ror_missing_before > 0 else None

    # 5) 노드/엣지 수: centrality CSV(노드 목록) + 최종 CSV(org_names)로 가볍게 재계산
    node_count = edge_count = None
    if cent_csv.exists():
        cent = pd.read_csv(cent_csv)
        node_keep = set(cent["node"].astype(str).tolist())
        node_count = len(node_keep)

        # 최종 CSV에서 협업 edge 생성 → centrality에 남은 노드만 필터 → unique edge 수 계산
        df_final = pd.read_csv(final_csv)
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
            pairs = []
            for i in range(len(arr)):
                for j in range(i+1, len(arr)):
                    a, b = sorted((arr[i], arr[j]))
                    pairs.append((a, b))
            return pairs

        all_pairs = []
        if "org_names" in df_final.columns:
            for pairs in df_final["org_names"].apply(_to_pairs):
                all_pairs.extend(pairs)

        # centrality에 남아있는 노드들만으로 edge 필터
        kept_edges = {(a, b) for (a, b) in all_pairs if a in node_keep and b in node_keep}
        edge_count = len(kept_edges)

    # 6) DOI 보강 관련(결측 대비 보강 수/율): per-year 메트릭 파일이 있으면 합산 사용(있으면 자동 집계)
    doi_missing_initial = doi_enriched = doi_enrich_rate = None
    # 연도 범위 추출
    m = re.search(r"_(\d{4})_(\d{4})$", base)
    if m:
        y0, y1 = int(m.group(1)), int(m.group(2))
        # (issn 다중일 수 있으므로 prefix만 추출)
        prefix = Path(base).name[:-(len(m.group(0)))]
        acc_missing, acc_enriched = 0, 0
        found_any = False
        for y in range(y0, y1+1):
            metrics_file = Path(f"{prefix}_{y}_{y}_metrics.json")
            if metrics_file.exists():
                try:
                    mm = json.loads(metrics_file.read_text(encoding="utf-8"))
                    if "doi_missing_initial" in mm:
                        acc_missing += int(mm.get("doi_missing_initial", 0))
                        acc_enriched += int(mm.get("doi_enriched_from_empty", 0))
                        found_any = True
                except Exception:
                    pass
        if found_any:
            doi_missing_initial = acc_missing
            doi_enriched = acc_enriched
            doi_enrich_rate = (acc_enriched / acc_missing) if acc_missing > 0 else None


    # ✅ 메트릭 JSON 우선 반영
    metrics_path = Path(f"{base}_metrics.json")
    if metrics_path.exists():
        try:
            mm = json.loads(metrics_path.read_text(encoding="utf-8"))
            total_collected = mm.get("total_collected", total_collected)
            doi_missing_initial = mm.get("doi_missing_initial", doi_missing_initial)
            doi_enriched = mm.get("doi_enriched_from_empty", doi_enriched)
            doi_enrich_rate = mm.get("doi_enrich_rate", doi_enrich_rate)
            removed_authorships_empty = mm.get("authorships_removed_empty_list", removed_authorships_empty)
            ror_missing_after = mm.get("ror_missing_after_extract", ror_missing_after)
        except Exception:
            pass

    # 테이블(1행) 생성
    data = [{
        "중심성 지표": centrality,
        "전체 수집 논문 수": total_collected,
        "authorships==[] 삭제 수": removed_authorships_empty,
        "DOI 결측 수(최종)": doi_missing_final,
        "DOI 보강 수(합산)": doi_enriched,
        "DOI 보강률(결측 대비)": (None if doi_enrich_rate is None else round(doi_enrich_rate*100, 2)),
        "ROR 결측 수(매핑 전)": ror_missing_before,
        "ROR 결측 수(매핑 후)": ror_missing_after,
        "ROR 보강 수(행 기준)": ror_augmented_rows,
        "ROR 보강률(결측 대비)": (None if ror_augment_rate is None else round(ror_augment_rate*100, 2)),
        "노드 수(기관 수)": node_count,
        "엣지 수(협업 수)": edge_count,
    }]
    df = pd.DataFrame(data)
    return df


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
            st.subheader("Degree 기반")
            st.dataframe(_summary_metrics_table(sel["csv"], centrality="degree"), use_container_width=True)

            st.subheader("Eigenvector 기반")
            st.dataframe(_summary_metrics_table(sel["csv"], centrality="eigenvector"), use_container_width=True)



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
