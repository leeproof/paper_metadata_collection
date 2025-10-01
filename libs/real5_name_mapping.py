import pandas as pd
import asyncio
import aiohttp
import ast
import time
from pathlib import Path

import json, os
from pathlib import Path as _Path

_CACHE_PATH = _Path("ror_name_cache.json")

def _load_cache():
    if _CACHE_PATH.exists():
        try:
            return json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_cache(cache: dict):
    try:
        _CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


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
            rid = (rid or "").strip()
            if rid.startswith("http"):
                rid = rid.rstrip("/").rsplit("/", 1)[-1]
                rid = rid.lower()

            if rid in cache:
                return rid, cache[rid]
            
            async with sem:
                attempt = 0
                while attempt < 6:
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
                            #성공값만 캐시에 저장
                            cache[rid] = display_name
                            return rid, display_name
                        
                        #최종 실패로 봐도 되는 코드들
                        if resp.status in (400,404):
                            return rid, None
                        
                        #레이트리밋: Retry-After 존중
                        if resp.status == 429:
                            ra =resp.headers.get('Retry-After')
                            delay = None
                            if ra:
                                try:
                                    delay = float(ra)
                                except Exception:
                                    try:
                                        from email.utils import parsedate_to_datetime
                                        import time as _time
                                        dt = parsedate_to_datetime(ra)
                                        delay = max(0.0, dt.timestamp() - _time.time())
                                    except Exception:
                                        delay = None
                            if delay is None:
                                delay = 0.5 * (2 ** attempt)
                            try:
                                import random
                                delay += random.uniform(0, 0.3)
                            except Exception:
                                pass
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue
                        
                        #서버측 일시 오류
                        if resp.status in (500,502,503,504):
                            await asyncio.sleep(0.5 * (2 ** attempt))
                            attempt += 1
                            continue

                        #그 외는 시도X, 종료
                        return rid, None

                    except Exception:
                        # 네트워크 등 예외: 지수 백오프 후 재시도
                        await asyncio.sleep(0.5 * (2 ** attempt))
                        attempt += 1
                        continue

                

            # 실패했거나 names가 없으면 None
            return rid, None

        async def runner():
            sem = asyncio.Semaphore(concurrency)
            timeout = aiohttp.ClientTimeout(total=90)
            conn = aiohttp.TCPConnector(limit_per_host=concurrency)
            #기존 캐시 로드(과거 성공 결과 재사용)
            cache = _load_cache()

            async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
                tasks = [fetch(session, rid, sem, cache) for rid in ids]
                results = await asyncio.gather(*tasks)

            result_dict = dict(results)
            # 성공 값만 캐시에 병합 저장
            changed = False
            for k, v in result_dict.items():
                if v and cache.get(k) != v:
                    cache[k] = v
                    changed = True
            if changed:
                _save_cache(cache)
                
            return result_dict

        return asyncio.run(runner())

    # 고유 ID 집합 수집
    all_ids = []
    for cell in df['ror']:
        all_ids.extend(extract_ids(cell))

    unique_ids = sorted(set(all_ids))
    if not unique_ids:
        df['org_names'] = None
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        return
    
    dict_id_to_name = load_names_concurrently(unique_ids, concurrency=10)

    missing_ids = [rid for rid in unique_ids if dict_id_to_name.get(rid) in (None, '', [])]
    if missing_ids:
        time.sleep(8)
        second = load_names_concurrently(missing_ids, concurrency=3)
        dict_id_to_name.update({k: v for k, v in second.items() if v})

        missing_ids = [rid for rid in unique_ids if dict_id_to_name.get(rid) in (None, '', [])]
        if missing_ids:
            time.sleep(15)
            third = load_names_concurrently(missing_ids, concurrency=2)
            dict_id_to_name.update({k: v for k, v in third.items() if v})

        missing_ids = [rid for rid in unique_ids if dict_id_to_name.get(rid) in (None, '', [])]
        if missing_ids:
            time.sleep(25)
            fourth = load_names_concurrently(missing_ids, concurrency=1)
            dict_id_to_name.update({k: v for k, v in fourth.items() if v}) 

    # org_names 컬럼 생성
    def map_row_to_names(ror_cell):
        ids = extract_ids(ror_cell)
        names = [dict_id_to_name.get(r) for r in ids]
        names = [n for n in names if n]
        return names or None

    df['org_names'] = df['ror'].apply(map_row_to_names)

    
    outp = Path(output_csv)

    # 이미 결과가 있으면 스킵 (환경변수로 강제 덮어쓰기 허용)
    if outp.exists() and not bool(os.environ.get("OVERWRITE_NAME_MAPPING")):
        total_time = time.perf_counter() - t0
        print(f"매핑 스킵(기존 산출물 존재): 총 소요시간: {total_time:.2f}초")
        return

    # 결과 저장 및 시간 출력
    df.to_csv(outp, index=False, encoding='utf-8-sig')
    total_time = time.perf_counter() - t0
    print(f"매핑 완료: {len(all_ids)}개 ID → 기관명, 총 소요시간: {total_time:.2f}초")

if __name__ == "__main__":
    main(
        input_csv='0723_2015_2024_optimized_ror_extract.csv',
        output_csv='0723_2015_2024_optimized_ror_extract_name.csv',
        prefix='', year_start=2015, year_end=2024
    )
