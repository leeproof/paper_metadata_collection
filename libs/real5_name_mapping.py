import pandas as pd
import asyncio
import aiohttp
import ast
import time
from pathlib import Path

def main(input_csv, output_csv, prefix, year_start, year_end):
    # 실행 시간 측정 시작
    t0 = time.perf_counter()

    # CSV 불러오기
    df = pd.read_csv(Path(input_csv), dtype={'ror': str})

    # ror 컬럼에서 ID 추출 함수
    def extract_ids(ror_cell):
        if pd.isna(ror_cell) or not isinstance(ror_cell, str):
            return []
        try:
            urls = ast.literal_eval(ror_cell)
        except Exception:
            return []
        return [u.rstrip('/').rsplit('/', 1)[-1] for u in urls if isinstance(u, str)]

    # 비동기 ROR ID → 기관명 매핑 함수
    def load_names_concurrently(ids, concurrency=20):
        async def fetch(session, rid, sem, cache):
            if rid in cache:
                return rid, cache[rid]

            url = f'https://api.ror.org/v2/organizations/{rid}'
            async with sem:
                try:
                    async with session.get(url, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()

                            # 1) 공식 full name 우선
                            full_name = data.get('name')

                            # 2) names 리스트에서 lang=='en'인 영어 이름
                            names = data.get('names', [])
                            eng_name = next(
                                (n.get('value') for n in names if n.get('lang') == 'en'),
                                None
                            )

                            # 3) 최종 display_name 결정
                            display_name = eng_name or full_name or (names[0].get('value') if names else None)

                            cache[rid] = display_name
                            return rid, display_name
                except Exception:
                    # 네트워크 에러 등 무시하고 None 처리
                    pass

            # 실패했거나 names가 없으면 None
            cache[rid] = None
            return rid, None

        async def runner():
            sem = asyncio.Semaphore(concurrency)
            timeout = aiohttp.ClientTimeout(total=60)
            conn = aiohttp.TCPConnector(limit_per_host=concurrency)
            cache = {}
            async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
                tasks = [fetch(session, rid, sem, cache) for rid in ids]
                results = await asyncio.gather(*tasks)
            return dict(results)

        return asyncio.run(runner())

    # 고유 ID 집합 수집
    all_ids = set()
    for cell in df['ror']:
        all_ids.update(extract_ids(cell))

    # 매핑 수행
    dict_id_to_name = load_names_concurrently(all_ids, concurrency=50)

    # org_names 컬럼 생성
    def map_row_to_names(ror_cell):
        ids = extract_ids(ror_cell)
        return [dict_id_to_name.get(r) for r in ids]

    df['org_names'] = df['ror'].apply(map_row_to_names)

    # 결과 저장 및 시간 출력
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    total_time = time.perf_counter() - t0
    print(f"매핑 완료: {len(all_ids)}개 ID → 기관명, 총 소요시간: {total_time:.2f}초")

if __name__ == "__main__":
    main(
        input_csv='0723_2015_2024_optimized_ror_extract.csv',
        output_csv='0723_2015_2024_optimized_ror_extract_name.csv',
        prefix='', year_start=2015, year_end=2024
    )
