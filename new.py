import os, json, pandas as pd, asyncio
from pathlib import Path
from typing import List
from pyalex import Sources, config
import re

# 각 단계별 기존 스크립트 import (함수화)
from libs import real1_test_openalex_sequential_optimized_0722 as real1
from libs import real2_test_openalex_to_csv as real2
from libs import real3_ror_mapping_processor_copy as real3
from libs import real4_ror_extract as real4
from libs import real5_name_mapping as real5
from libs import real6_visualize as real6


def run_pipeline(issns: List[str], year_start: int, year_end: int, email: str = 's0124kw@gmail.com'):

    """
    전체 파이프라인 실행 함수
    issns: 저널 ISSN 리스트
    year_start, year_end: 연도 범위
    email: OpenAlex API용 이메일
    """

    """
    파일명 prefix 계산
    """
    config.email = email
    journal_prefixes = []
    for issn in issns:
        src = next(
            Sources().filter(issn=[issn])
                     .select(['display_name'])
                     .paginate(per_page=1)
        )
        display = src[0]['display_name'] if isinstance(src, list) else src['display_name']
        safe = re.sub(r'\W+', '_', display).strip('_')
        journal_prefixes.append(safe)
    prefix = "-".join(journal_prefixes)


    # 1️⃣ 논문 메타데이터 수집 및 DOI 보강 (real1)
    json_base = f"{prefix}_{year_start}_{year_end}"
    json_parts = [f"{json_base}_part{i+1}.json" for i in range(3)]
    json_merged = f"{json_base}.json"
    csv_file = f"{json_base}.csv"
    csv_ror = f"{json_base}_ror.csv"
    csv_ror_extract = f"{json_base}_ror_extract.csv"
    csv_ror_extract_name = f"{json_base}_ror_extract_name.csv"
    html_ror_extract_name_network = f"{json_base}_ror_extract_name_network.html"
    cache_file = Path("ror_cache.pkl")

    # 1. real1: 논문 수집 및 보강
    print("[1/7] 논문 메타데이터 수집 및 DOI 보강...")
    real1.main(issns=issns, year_start=year_start, year_end=year_end, email=email, prefix=prefix)

    # 2. real2: JSON → CSV 변환
    print("[2/7] JSON → CSV 변환...")
    real2.main(input_json=json_merged, output_csv=csv_file, prefix=prefix, year_start=year_start, year_end=year_end)

    # 3. real3: ROR 매핑
    print("[3/7] ROR 매핑...")
    asyncio.run(real3.process(
        input_csv=Path(csv_file),
        output_csv=Path(csv_ror),
        cache_file=cache_file,
        concurrency=20
    ))

    # 4. real4: ROR 추출
    print("[4/7] ROR 추출 및 통계...")
    real4.main(input_csv=csv_ror, output_csv=csv_ror_extract, prefix=prefix, year_start=year_start, year_end=year_end)

    # 5. real5: ROR ID → 기관명 매핑
    print("[5/7] ROR ID → 기관명 매핑...")
    real5.main(input_csv=csv_ror_extract, output_csv=csv_ror_extract_name, prefix=prefix, year_start=year_start, year_end=year_end)

    # 6. real6: 협력 네트워크 시각화 (HTML 생성)
    print("[6/7] 협력 네트워크 시각화...")
    html_path = real6.main(
        input_csv=csv_ror_extract_name,
        output_html=html_ror_extract_name_network # 추가: 원하는 파일명으로 고정
        )
    if html_path:
        print(f"→ 시각화 파일 생성: {html_path}")


    print("[7/7] 전체 파이프라인 완료!")

    return html_path

if __name__ == "__main__":
    run_pipeline(issns=['0043-1354','0011-9164','0733-9429'], year_start=2015, year_end=2024, email='s0124kw@gmail.com')
    
# real1, real2, real3, real4의 main 함수가 하이퍼파라미터를 받을 수 있도록 각각 수정 필요