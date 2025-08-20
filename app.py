import streamlit as st
import nest_asyncio
import pandas as pd
import glob
from pathlib import Path

# 이벤트 루프 충돌 방지 (async 사용 시 필수)
nest_asyncio.apply()

from new import run_pipeline

st.set_page_config(page_title="OpenAlex Paper Metadata Pipeline", layout="wide")

# 캐시 데코레이터 제거 (파일 생성/외부 I/O는 캐시 비권장)
def cached_run(issns, year_start, year_end, email):
    results = []
    step = 5  # 5년 단위로 분할 실행
    for y0 in range(year_start, year_end+1, step):
        y1 = min(y0+step-1, year_end)
        run_pipeline(issns=issns, year_start=y0, year_end=y1, email=email)
        results.append(f"{'_'.join(issns)}_{y0}_{y1}_ror_extract_name.csv")

    # 결과 CSV 합치기
    df_all = pd.concat([pd.read_csv(f) for f in results], ignore_index=True)
    out_name = f"{'_'.join(issns)}_{year_start}_{year_end}_ror_extract_name.csv"
    df_all.to_csv(out_name, index=False, encoding="utf-8-sig")
    return True

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

def list_csv_outputs():
    # real5의 출력 규칙에 맞춘 CSV만 노출
    return sorted([f for f in glob.glob("*.csv") if f.endswith("_ror_extract_name.csv")])

def app():
    st.title("Paper Metadata Collection Pipeline")

    issns, year_start, year_end, email, run_btn = sidebar_controls()

    if run_btn:
        with st.spinner("파이프라인 실행 중... (시간이 걸릴 수 있어요)"):
            cached_run(issns, year_start, year_end, email)
        st.success("파이프라인 완료! 아래에서 파일을 선택해 다운로드하세요.")

    csv_files = list_csv_outputs()
    if not csv_files:
        st.info("아직 생성된 파일이 없습니다. 사이드바 설정 후 🚀 실행 버튼을 눌러주세요.")
        return

    selected = st.selectbox("CSV 선택", csv_files, index=0)
    df = pd.read_csv(selected)
    st.dataframe(df, use_container_width=True)

    # CSV 직렬화
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig")

    # 선택 CSV → 짝 HTML 파일명 계산
    # 예: ABC.csv → ABC_network.html
    html_candidate = selected.replace("_ror_extract_name.csv", "_ror_extract_name_network.html")
    html_path = Path(html_candidate)

    col_csv, col_html = st.columns(2)
    with col_csv:
        st.download_button(
            label="CSV Download",
            data=csv_bytes,
            file_name=Path(selected).name,
            mime="text/csv",
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

if __name__ == "__main__":
    app()
