import streamlit as st
import nest_asyncio
import pandas as pd
import glob

# asyncio 이벤트 루프 패치
nest_asyncio.apply()

from new import run_pipeline

@st.cache_data(show_spinner=False)
def cached_run(issns, year_start, year_end, email):
    return run_pipeline(
        issns=issns,
        year_start=year_start,
        year_end=year_end,
        email=email
    )


def main():
    st.title("🔄 Paper Metadata Collection")

    # 사이드바: 사용자 입력
    with st.sidebar:
        st.header("▶ 입력 설정")
        issn_input = st.text_input("ISSN 리스트 (콤마로 구분)", value="0043-1354,0011-9164,0733-9429")
        year_start  = st.number_input("시작 연도", min_value=1900, max_value=2100, value=2015)
        year_end    = st.number_input("종료 연도", min_value=1900, max_value=2100, value=2024)
        email       = st.text_input("결과 보고 이메일", value="s0124kw@gmail.com")

        if st.button("🚀 실행"):
            issns = [s.strip() for s in issn_input.split(',') if s.strip()]
            with st.spinner("파이프라인 실행 중입니다..."):
                try:
                    cached_run(issns, year_start, year_end, email)
                    st.success("파이프라인이 성공적으로 완료되었습니다.")
                    # 세션 상태에 실행 완료 플래그 설정
                    st.session_state["pipeline_done"] = True
                except Exception as e:
                    with st.expander("오류 세부 정보 보기"):
                        st.error(f"실행 중 오류가 발생했습니다: {e}")

    # 파이프라인이 한 번이라도 성공 실행된 후라면 CSV 선택 UI를 항상 보여준다
    if st.session_state.get("pipeline_done", False):
        csv_files = [f for f in glob.glob("*.csv") if f.endswith("_ror_extract_name.csv")]
        if not csv_files:
            st.error("출력된 CSV 파일을 찾을 수 없습니다.")
            return

        selected = st.selectbox("다운로드할 파일", csv_files, key="csv_selector")
        df = pd.read_csv(selected)
        st.dataframe(df)

        csv_data = df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "CSV 다운로드",
            data=csv_data,
            file_name=selected,
            mime="text/csv"
        )
    else:
        st.info("사이드바에서 설정 후 🚀 실행 버튼을 눌러주세요.")


if __name__ == "__main__":
    main()
