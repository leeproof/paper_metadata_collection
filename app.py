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

from new import run_pipeline, make_html_from_csv, run_pipeline_cached, make_html_string_from_csv

# ──────────────────────────────────────────────────────────────────────────────
# 기본 설정
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="OpenAlex Paper Metadata Pipeline", layout="wide")

# 한국 시간대 설정
now_kst = datetime.datetime.now(ZoneInfo("Asia/Seoul"))

# 이벤트 루프 충돌 방지 (async 사용 시 필수)
nest_asyncio.apply()


# ──────────────────────────────────────────────────────────────────────────────
# 공통 유틸/요약 테이블/공통 렌더
# ──────────────────────────────────────────────────────────────────────────────
def _safe_int(x):
    try:
        return int(x)
    except Exception:
        return None


def load_metrics_if_any(csv_path: Path) -> dict:
    """최종 CSV와 같은 폴더의 metrics.json을 읽어 dict 반환(없으면 {})."""
    mfile = csv_path.parent / "metrics.json"
    if mfile.exists():
        try:
            return json.loads(mfile.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def summarize_lightweight_from_csv(csv_path: Path) -> dict:
    """metrics.json이 없을 때, 엣지 CSV만으로 노드/엣지 수 집계."""
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        return {}
    cols = {c.lower(): c for c in df.columns}
    src = cols.get("source") or cols.get("src") or "source"
    tgt = cols.get("target") or cols.get("dst") or "target"
    node_count = pd.Index(
        pd.concat([df[src], df[tgt]], ignore_index=True).dropna().unique()
    ).size
    edge_count = len(df)
    return {"node_count": int(node_count), "edge_count": int(edge_count)}


def render_summary_table(metrics: dict):
    rows = [
        ("총 수집 논문 수",           metrics.get("total_papers")),
        ("authorships 비어있는 논문 수", metrics.get("authorships_empty")),
        ("삭제한 문헌 수",            metrics.get("deleted_docs")),
        ("DOI 결측 수",              metrics.get("doi_missing")),
        ("DOI 보강 수",              metrics.get("doi_filled")),
        ("DOI 보강률(%)",            metrics.get("doi_fill_rate")),
        ("ROR ID 결측 수",           metrics.get("ror_missing")),
        ("ROR ID 보강 수",           metrics.get("ror_filled")),
        ("ROR ID 보강률(%)",         metrics.get("ror_fill_rate")),
        ("노드 수(기관 수)",          metrics.get("node_count")),
        ("엣지 수(협업 수)",          metrics.get("edge_count")),
    ]
    df_show = pd.DataFrame(rows, columns=["지표", "값"])
    st.subheader("📊 수집/보강/네트워크 요약")
    st.dataframe(df_show, use_container_width=True)


def render_outputs(col_html, csv_path: Path):
    """공통 출력: Degree/Eigenvector HTML 다운로드 버튼 + 요약 테이블."""
    with col_html:
        # (1) Degree 기반 (크기·색 = degree)
        deg_html = make_html_string_from_csv(str(csv_path), size_by="degree", color_by="degree")
        st.download_button(
            label="Visualization Download (Degree-based)",
            data=deg_html.encode("utf-8"),
            file_name=csv_path.stem + "_degree_network.html",
            mime="text/html",
            use_container_width=True,
        )

        # (2) Eigenvector 기반 (크기·색 = eigenvector)
        eig_html = make_html_string_from_csv(str(csv_path), size_by="eigenvector", color_by="eigenvector")
        st.download_button(
            label="Visualization Download (Eigenvector-based)",
            data=eig_html.encode("utf-8"),
            file_name=csv_path.stem + "_eigenvector_network.html",
            mime="text/html",
            use_container_width=True,
        )

        # (3) 요약 테이블
        metrics = load_metrics_if_any(csv_path)
        if not metrics:
            metrics = summarize_lightweight_from_csv(csv_path)

        for k_rate, (filled_key, missing_key) in {
            "doi_fill_rate": ("doi_filled", "doi_missing"),
            "ror_fill_rate": ("ror_filled", "ror_missing"),
        }.items():
            filled = _safe_int(metrics.get(filled_key))
            missing = _safe_int(metrics.get(missing_key))
            if filled is not None and missing not in (None, 0):
                metrics[k_rate] = round(100.0 * filled / missing, 2)

        render_summary_table(metrics)


# ──────────────────────────────────────────────────────────────────────────────
# 파이프라인 실행/사이드바
# ──────────────────────────────────────────────────────────────────────────────
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

    out_path = Path(final_csv_path)
    final_html = make_html_from_csv(str(out_path))

    if progress_bar:
        progress_bar.progress(1.0)
    if progress_text:
        progress_text.success("모든 구간 처리 및 최종 파일 생성 완료")

    st.sidebar.caption(f"마지막 업데이트 · {now_kst:%H:%M:%S}")

    return str(out_path), final_html


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
    run_btn = st.sidebar.button("🚀 파이프라인 실행", use_container_width=True)
    return issns, int(year_start), int(year_end), email, run_btn


# ──────────────────────────────────────────────────────────────────────────────
# 메인 앱
# ──────────────────────────────────────────────────────────────────────────────
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

        # ✅ 이력 드롭다운(기본값=방금 실행한 항목)
        options = [r["label"] for r in st.session_state.runs]
        selected = st.selectbox("실행 결과 선택", options, index=len(options) - 1)
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
        st.dataframe(df.head(PREVIEW_N), use_container_width=True)

        # 사이드바에 실행 시간 표시
        st.sidebar.markdown("---")
        st.sidebar.subheader("실행 시간")
        st.sidebar.write(f" 시작 {sel['start']}")
        st.sidebar.write(f" 종료 {sel['end']}")
        st.sidebar.write(f" 소요 {sel['duration']}")

        # ✅ CSV 다운로드: 임계값 초과 시 ZIP
        csv_path = Path(sel["csv"])
        file_size_mb = csv_path.stat().st_size / (1024 * 1024)

        col_csv, col_html = st.columns(2)
        with col_csv:
            if file_size_mb <= THRESHOLD_MB:
                st.download_button(
                    label=f"CSV Download ({file_size_mb:.1f} MB)",
                    data=csv_path.read_bytes(),
                    file_name=csv_path.name,
                    mime="text/csv",
                    use_container_width=True,
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
                    use_container_width=True,
                )

        # 시각화/요약 (공통 렌더)
        render_outputs(col_html, csv_path)

    else:
        if st.session_state.runs:
            options = [r["label"] for r in st.session_state.runs]
            selected = st.selectbox("실행 결과 선택", options, index=len(options) - 1)
            sel = next(r for r in st.session_state.runs if r["label"] == selected)

            # ✅ 선택된 실행의 시간 로그 -> 사이드바에 표시
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
            st.dataframe(df.head(PREVIEW_N), use_container_width=True)

            # ✅ CSV 다운로드: 임계값 초과 시 ZIP
            csv_path = Path(sel["csv"])
            file_size_mb = csv_path.stat().st_size / (1024 * 1024)

            col_csv, col_html = st.columns(2)
            with col_csv:
                if file_size_mb <= THRESHOLD_MB:
                    st.download_button(
                        label=f"CSV Download ({file_size_mb:.1f} MB)",
                        data=csv_path.read_bytes(),
                        file_name=csv_path.name,
                        mime="text/csv",
                        use_container_width=True,
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
                        use_container_width=True,
                    )

            # 시각화/요약 (공통 렌더)
            render_outputs(col_html, csv_path)
        else:
            st.info("사이드바에서 설정 후 🚀 파이프라인 실행 버튼을 눌러주세요.")


if __name__ == "__main__":
    app()
