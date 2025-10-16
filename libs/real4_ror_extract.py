import pandas as pd
import re
from pathlib import Path
import os
import csv


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

    try:
        df = pd.read_csv(
            Path(input_csv),
            dtype={'authorships': str},
            engine='python',
            encoding='utf-8-sig',
            quoting=csv.QUOTE_ALL,
            quotechar='"',
            escapechar='\\',
            low_memory=False
        )
    except pd.errors.ParserError:
        # 매우 드문 경우를 대비한 안전한 fallback: csv.DictReader로 무조건 읽어서 DataFrame으로 변환
        # -> 보수적으로 "누락 없이" 모든 행을 가져옵니다.
        rows = []
        with open(Path(input_csv), 'r', encoding='utf-8-sig', newline='') as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                rows.append(r)
        df = pd.DataFrame(rows)
        # authorships 컬럼이 문자열이길 기대하므로 보장
        if 'authorships' in df.columns:
            df['authorships'] = df['authorships'].astype(str)
            
    df['ror'] = df['authorships'].apply(extract_rors_via_regex)

    def _drop_orcid(lst):
        out = []
        for r in (lst or []):
            s = str(r).strip().lower()
            # orcid.org 등은 제거, ror.org만 유지
            if "orcid.org" in s:
                continue
            if "ror.org" in s:
                out.append(r)
        return out
    df['ror'] = df['ror'].apply(_drop_orcid)

    outp = Path(output_csv)
    outp.parent.mkdir(parents=True, exist_ok=True)

    # 이미 결과가 있으면 스킵 (환경변수로 강제 덮어쓰기 허용)
    if outp.exists() and not bool(os.environ.get("OVERWRITE_ROR_EXTRACT")):
        # 기존 산출물 유지, 아무 작업도 안 함
        return
    df.to_csv(output_csv, index=False, encoding='utf-8')



if __name__ == "__main__":
    main('0723_2015_2024_optimized_ror.csv', '0723_2015_2024_optimized_ror_extract.csv')