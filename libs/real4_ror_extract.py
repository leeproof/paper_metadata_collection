import pandas as pd
import re
from pathlib import Path


# === ADD (공통 헬퍼) ==========================================
def _update_metrics(prefix, year_start, year_end, **updates):
    import os, json, math
    from numbers import Integral, Real

    try:
        import numpy as np
        _HAS_NP = True
    except Exception:
        _HAS_NP = False

    def _jsonable(v):
        # None 그대로
        if v is None:
            return None
        # 파이썬 기본 타입
        if isinstance(v, (bool, int, float, str)):
            return v
        # 넘파이/넘버스 계열 수치 → 파이썬 기본형으로
        if _HAS_NP:
            import numpy as np
            if isinstance(v, (np.integer,)):
                return int(v)
            if isinstance(v, (np.floating,)):
                # NaN 방지
                return None if (np.isnan(v)) else float(v)
            if isinstance(v, (np.bool_,)):
                return bool(v)
        # numbers 모듈로 한 번 더 안전망
        if isinstance(v, Integral):
            return int(v)
        if isinstance(v, Real):
            # NaN 방지
            return None if (isinstance(v, float) and math.isnan(v)) else float(v)
        # 리스트/튜플은 재귀 처리
        if isinstance(v, (list, tuple)):
            return [_jsonable(x) for x in v]
        # dict는 값만 변환
        if isinstance(v, dict):
            return {str(k): _jsonable(val) for k, val in v.items()}
        # 그 외는 문자열화(최후 안전망)
        return str(v)

    path = f"{prefix}_{year_start}_{year_end}_metrics.json"
    data = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as rf:
                data = json.load(rf) or {}
        except Exception:
            data = {}

    # 업데이트(기존 값 덮어씀)
    for k, v in updates.items():
        jv = _jsonable(v)
        if jv is not None:
            data[str(k)] = jv

    with open(path, "w", encoding="utf-8") as wf:
        json.dump(data, wf, ensure_ascii=False, indent=2)

# =============================================================



def extract_rors_via_regex(s):
    if not isinstance(s, str):
        return []
    ror_pattern = re.compile(r'https?://ror\.org/[0-9a-z]+', re.IGNORECASE)
    found = ror_pattern.findall(s)
    return list(dict.fromkeys(found))  # 순서 유지하면서 중복 제거

def main(input_csv, output_csv, prefix, year_start, year_end):
    import pandas as pd
    from pathlib import Path
 
    df = pd.read_csv(Path(input_csv), dtype={'authorships': str})
    df['ror'] = df['authorships'].apply(extract_rors_via_regex)

    # ORCID ROR값만 제거 - 다른 기관 ROR값은 보존
    _BAD_ROR_SUFFIX = "/04fa4r544"
    def _drop_orcid(lst):
        out = []
        for r in (lst or []):
            try:
                s = str(r).strip().rstrip('/').lower()
                if not s.endswith(_BAD_ROR_SUFFIX):
                    out.append(r)
            except Exception:
                pass
        return out
    df['ror'] = df['ror'].apply(_drop_orcid)

    total = len(df)
    missing = (df['ror'].str.len() == 0).sum()
    print(f"총 행 수: {total}, ROR 미추출 행 수: {missing}")

    # ✅ metrics.json 업데이트
    _update_metrics(
        prefix, year_start, year_end,
        ror_missing_after_extract=missing,
        rows_total_after_extract=total
    )

    df.to_csv(output_csv, index=False, encoding='utf-8')


if __name__ == "__main__":
    main('0723_2015_2024_optimized_ror.csv', '0723_2015_2024_optimized_ror_extract.csv')