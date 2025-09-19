import pandas as pd
import re
from pathlib import Path


# === ADD (공통 헬퍼) ==========================================

from pathlib import Path

def _update_metrics(prefix, year_start, year_end, **updates):
    import os, json, math
    from numbers import Integral, Real
    try:
        import numpy as np
        _HAS_NP = True
    except Exception:
        _HAS_NP = False

    def _jsonable(v):
        if v is None: return None
        if isinstance(v, (bool, int, float, str)): return v
        if _HAS_NP:
            if isinstance(v, np.integer):  return int(v)
            if isinstance(v, np.floating): return None if (isinstance(v, float) and math.isnan(v)) else float(v)
            if isinstance(v, np.bool_):    return bool(v)
        if isinstance(v, Integral): return int(v)
        if isinstance(v, Real):     return float(v)
        if isinstance(v, (list, tuple)): return [_jsonable(x) for x in v]
        if isinstance(v, dict):          return {str(k): _jsonable(val) for k, val in v.items()}
        return str(v)

    # ★ 앵커 파일과 같은 폴더에 저장
    anchor = updates.pop("__anchor", None)
    out_dir = Path(anchor).parent if anchor else Path(".")
    path = out_dir / f"{prefix}_{year_start}_{year_end}_metrics.json"

    data = {}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            data = {}

    for k, v in updates.items():
        jv = _jsonable(v)
        if jv is not None:
            data[str(k)] = jv

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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

    # (1) 추출 직후(드롭 전) 상태 기록
    ror_missing_before = sum(1 for r in df['ror'] if not r)
    _update_metrics(prefix, year_start, year_end,
        ror_missing_before_extract=ror_missing_before,
        __anchor=output_csv
    )

    # (2) ORCID(기관) ROR 컷 — 안전망
    _BAD_ROR_SUFFIX = "/04fa4r544"
    def _drop_orcid(lst):
        out = []
        for r in (lst or []):
            s = str(r).strip().rstrip('/').lower()
            if not s.endswith(_BAD_ROR_SUFFIX):
                out.append(r)
        return out

    df['ror'] = df['ror'].apply(_drop_orcid)

    # (3) 드롭 후 상태 기록
    ror_missing_after = int((df['ror'].str.len() == 0).sum())

    _update_metrics(
        prefix, year_start, year_end,
        ror_missing_after_extract=ror_missing_after,
        rows_total_after_extract=len(df),
        __anchor=output_csv
    )

    df.to_csv(output_csv, index=False, encoding='utf-8')


if __name__ == "__main__":
    main('0723_2015_2024_optimized_ror.csv', '0723_2015_2024_optimized_ror_extract.csv')