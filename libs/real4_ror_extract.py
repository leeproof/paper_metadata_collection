import pandas as pd
import re
from pathlib import Path


# === ADD (공통 헬퍼) ==========================================

from pathlib import Path
import csv
from numbers import Integral, Real

def _update_metrics(prefix, year_start, year_end, __anchor=None, **updates):
    """
    메트릭을 CSV 한 장(key,value)으로만 기록.
    __anchor 폴더가 없으면 조용히 스킵(기존 정책 유지)
    """
    from pathlib import Path
    import csv
    from numbers import Integral, Real

    def _norm(v):
        try:
            import numpy as np
            if isinstance(v, (np.integer,)): return int(v)
            if isinstance(v, (np.floating,)): return float(v)
        except Exception:
            pass
        if isinstance(v, Integral): return int(v)
        if isinstance(v, Real):     return float(v)
        return v

    updates = {k: _norm(v) for k, v in updates.items()}
    if __anchor is None:  # 기록 위치가 없으면 건너뜀
        return updates

    p = Path(__anchor)
    out_dir = p if p.is_dir() else p.parent
    if not out_dir.exists():
        return updates

    out_csv = out_dir / f"{prefix}_{year_start}_{year_end}_metrics.csv"
    existing = {}
    if out_csv.exists():
        try:
            with out_csv.open("r", encoding="utf-8", newline="") as f:
                rdr = csv.reader(f)
                header = next(rdr, None)
                if header and header[:2] == ["key", "value"]:
                    for row in rdr:
                        if len(row) >= 2:
                            existing[row[0]] = row[1]
        except Exception:
            pass

    for k, v in updates.items():
        existing[k] = "" if v is None else str(v)

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["key", "value"])
        for k in sorted(existing.keys()):
            wr.writerow([k, existing[k]])

    return updates

# =============================================================



def extract_rors_via_regex(s):
    if not isinstance(s, str):
        return []
    ror_pattern = re.compile(r'https?://ror\.org/[0-9a-z]+', re.IGNORECASE)
    found = ror_pattern.findall(s)
    return list(dict.fromkeys(found))  # 순서 유지하면서 중복 제거

def main(input_csv, output_csv, prefix, year_start, year_end,
         anchor_path: str | None = None):
    import pandas as pd, re
    from pathlib import Path

    df = pd.read_csv(Path(input_csv), dtype={'authorships': str})
    df['ror'] = df['authorships'].apply(extract_rors_via_regex)

    _BAD_ROR_SUFFIX = "/04fa4r544"
    def _drop_orcid(lst):
        out = []
        for r in (lst or []):
            s = str(r).strip().rstrip('/').lower()
            if not s.endswith(_BAD_ROR_SUFFIX):
                out.append(r)
        return out
    df['ror'] = df['ror'].apply(_drop_orcid)

    # ★ metrics.csv는 절대 건드리지 않음
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding='utf-8')



if __name__ == "__main__":
    main('0723_2015_2024_optimized_ror.csv', '0723_2015_2024_optimized_ror_extract.csv')