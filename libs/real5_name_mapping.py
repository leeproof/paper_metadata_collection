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
        return [
                (u.rstrip('/').rsplit('/',1)[-1]).strip().lower()
                for u in urls if isinstance(u, str)]

    # 비동기 ROR ID → 기관명 매핑 함수
    def load_names_concurrently(ids, concurrency=20):
        async def fetch(session, rid, sem, cache):
            rid = (rid or "").strip().lower()

            if rid in cache:
                return rid, cache[rid]
            
            async with sem:
                attempt = 0
                while attempt < 4:
                    url = f'https://api.ror.org/v2/organizations/{rid}'
                    try:
                        resp = await session.get(url, timeout=20)

                        if resp.status in (301,302,307,308):
                            loc = resp.headers.get('Location') or resp.headers.get('location')
                            if loc:
                                rid = (loc.rstrip('/').rsplit('/',1)[-1]).strip().lower()
                                attempt += 1
                                continue

                        if resp.status == 200:
                            data = await resp.json()
                            full_name = data.get('name')
                            names = data.get('names', [])
                            eng_name = next((n.get('value') for n in names if n.get('lang') == 'en'), None)
                            display_name = eng_name or full_name or (names[0].get('value') if names else None)
                            cache[rid] = display_name
                            return rid, display_name

                        if resp.status == 429:
                            ra =resp.headers.get('Retry-After')
                            delay = float(ra) if ra else (0.5 * (2 ** attempt))
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue

                        if resp.status in (500,502,503,504):
                            await asyncio.sleep(0.5 * (2 ** attempt))
                            attempt += 1
                            continue

                        break

                    except Exception:
                        await asyncio.sleep(0.5 * (2 ** attempt))
                        attempt += 1
                        continue

            # 실패했거나 names가 없으면 None
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
    all_ids = []
    for cell in df['ror']:
        all_ids.extend(extract_ids(cell))

    unique_ids = sorted(set(all_ids))
    dict_id_to_name = load_names_concurrently(unique_ids, concurrency=10)

    missing_ids = [rid for rid in unique_ids if dict_id_to_name.get(rid) in (None, '', [])]
    if missing_ids:
        await_sleep = 3
        time.sleep(await_sleep)
        second = load_names_concurrently(missing_ids, concurrency=5)
        dict_id_to_name.update({k: v for k, v in second.items() if v})

    # org_names 컬럼 생성
    def map_row_to_names(ror_cell):
        ids = extract_ids(ror_cell)
        names = [dict_id_to_name.get(r) for r in ids]
        names = [n for n in names if n]
        return names or None

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
