import streamlit as st
import nest_asyncio
import pandas as pd
import datetime
from pathlib import Path
import math
from zoneinfo import ZoneInfo

# 한국 시간대 설정

now_kst = datetime.datetime.now(ZoneInfo("Asia/Seoul"))

# 이벤트 루프 충돌 방지 (async 사용 시 필수)
nest_asyncio.apply()

from new import run_pipeline, make_html_from_csv, run_pipeline_cached

# ▼ 추가: 대용량 다운로드 ZIP 압축용
from io import BytesIO
import zipfile

st.set_page_config(page_title="OpenAlex Paper Metadata Pipeline", layout="wide")

# 캐시 데코레이터 제거 (파일 생성/외부 I/O는 캐시 비권장)
def cached_run(issns, year_start, year_end, email,
               progress_bar=None, progress_text=None):
    # results = []
    # step = 5  # 5년 단위로 분할 실행

    # # 전체 스텝 수 계산 -> 진행률 표시
    # total_steps = math.ceil((year_end - year_start + 1) / step)

    # for i, y0 in enumerate(range(year_start, year_end + 1, step), start=1):
    #     y1 = min(y0 + step - 1, year_end)

    #     # 시작 전에 안내
    #     if progress_text:
    #         progress_text.info(f"처리 중: {y0}-{y1} 구간 (#{i}/{total_steps})")

    #     csv_path, _ = run_pipeline(
    #         issns=issns, year_start=y0, year_end=y1, email=email, make_html=False
    #     )
    #     results.append(csv_path)

    #     # 중간 상태를 ui 주기적으로 표시 -> 세션 타임아웃 방지
    #     if progress_bar:
    #         progress_bar.progress(min(i / total_steps, 1.0))
    #     if progress_text:
    #         progress_text.info(f"완료: {y0}-{y1} 구간 ({i}/{total_steps})")

    # # 사이드바에도 최근 업데이트 표시
    # st.sidebar.caption(f"마지막 업데이트 · {now_kst:%H:%M:%S}")

    # # 결과 CSV 합치기
    # df_all = pd.concat([pd.read_csv(f) for f in results], ignore_index=True)
    
    # first_chunk = Path(results[0]).name
    # first_suffix = f"_{year_start}_{min(year_start+step-1, year_end)}_ror_extract_name.csv"
    # prefix = first_chunk.replace(first_suffix, "")

    # # ✅ 최종 파일명도 같은 접두(prefix)에 전체 연도만 붙임 → '저널명 기반' 파일명 완성
    # out_name = f"{prefix}_{year_start}_{year_end}_ror_extract_name.csv"

    # df_all.to_csv(out_name, index=False, encoding="utf-8-sig")

    # # 최종 CSV로만 HTML 1개 생성
    # final_html = make_html_from_csv(out_name)

    # # 완료 표시
    # if progress_text:
    #     progress_text.success("모든 구간 처리 및 최종 파일 생성 완료")

    # return out_name, final_html

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

        with col_html:
            if html_path.exists():
                html_bytes = html_path.read_bytes()
                st.download_button(
                    label="Visualization Download",
                    data=html_bytes,
                    file_name=html_path.name,
                    mime="text/html",
                    use_container_width=True,
                )
            else:
                st.warning("연결된 HTML 파일이 없습니다. (파이프라인 실행 후 생성됩니다)")
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
            st.dataframe(df.head(PREVIEW_N), use_container_width=True)

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

            with col_html:
                if html_path.exists():
                    st.download_button(
                        label="Visualization Download",
                        data=html_path.read_bytes(),
                        file_name=html_path.name,
                        mime="text/html",
                        use_container_width=True,
                    )
                else:
                    st.warning("연결된 HTML 파일이 없습니다. (파이프라인 실행 후 생성됩니다)")
        else:
            st.info("사이드바에서 설정 후 🚀 파이프라인 실행 버튼을 눌러주세요.")

if __name__ == "__main__":
    app()
