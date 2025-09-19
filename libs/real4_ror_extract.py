import pandas as pd
import re
from pathlib import Path


# === ADD (공통 헬퍼) ==========================================

from pathlib import Path
import csv
from numbers import Integral, Real

def _update_metrics(prefix, year_start, year_end, __anchor=None, **updates):
    """
    외부저장소에는 CSV만 기록. __anchor가 파일이면 그 부모 폴더에,
    디렉터리면 그 디렉터리에 '<prefix>_<ys>_<ye>_metrics.csv'를 저장.
    - 새 폴더 생성 절대 안함
    - 같은 키는 덮어쓰기(병합 업데이트)
    """
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

    if __anchor is None:
        # 외부로 아예 안 쓰고 끝낼 수도 있음(로컬만 쓸 경우)
        return updates

    p = Path(__anchor)
    out_dir = p if p.is_dir() else p.parent
    # 새 폴더 절대 생성하지 않음
    if not out_dir.exists():
        # 폴더 없으면 기록 스킵(정책 위반 방지)
        return updates

    out_csv = out_dir / f"{prefix}_{year_start}_{year_end}_metrics.csv"

    # 기존 값 로드(key,value) 형태
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

    # 병합(덮어쓰기)
    for k, v in updates.items():
        existing[k] = "" if v is None else str(v)

    # 저장
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
    import pandas as pd
    from pathlib import Path
 
    df = pd.read_csv(Path(input_csv), dtype={'authorships': str})
    df['ror'] = df['authorships'].apply(extract_rors_via_regex)

    # (1) 추출 직후(드롭 전) 상태 기록
    ror_missing_before = sum(1 for r in df['ror'] if not r)
    _update_metrics(prefix, year_start, year_end,
        ror_missing_before_extract=ror_missing_before,
        __anchor=(anchor_path or output_csv)
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
        __anchor=(anchor_path or output_csv)
    )

    df.to_csv(output_csv, index=False, encoding='utf-8')


if __name__ == "__main__":
    main('0723_2015_2024_optimized_ror.csv', '0723_2015_2024_optimized_ror_extract.csv')