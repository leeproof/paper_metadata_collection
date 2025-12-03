import pandas as pd
import re
from pathlib import Path
import os
import csv
from pandas.errors import ParserError


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



# === PATCH: robust ROR extraction (regex + JSON) ===
import json, re

_ROR_URL_RX = re.compile(r'https?://ror\.org/[0-9a-z]+', re.I)

def _to_ror_url(rid: str) -> str:
    rid = (rid or "").strip()
    if not rid:
        return ""
    return rid if rid.startswith("http") else f"https://ror.org/{rid}"

def _collect_rors_from_json(authorships_text: str) -> list[str]:
    try:
        obj = json.loads(authorships_text)
    except Exception:
        return []
    found, stack = [], [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if 'ror' in cur:
                val = cur.get('ror')
                if isinstance(val, str) and val.strip():
                    found.append(_to_ror_url(val))
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    # URL로 정규화 + 중복 제거
    return sorted({_to_ror_url(u) for u in found if u})

def extract_rors_via_regex_or_json(s: str) -> list[str]:
    s = s if isinstance(s, str) else ""
    urls = set(_ROR_URL_RX.findall(s))          # 1) 텍스트 내 URL
    urls |= set(_collect_rors_from_json(s))     # 2) JSON의 'ror' 키(맨 ID 포함) → URL화
    # 노이즈 제거(예: ORCID 등)
    return sorted(u for u in urls if "orcid.org" not in u.lower())
# === /PATCH ===    

def main(input_csv, output_csv, prefix, year_start, year_end,
         anchor_path: str | None = None):

    input_path = Path(input_csv)
    df = pd.DataFrame()
    if not input_path.exists() or input_path.stat().st_size == 0:
        print(f"[real4] 입력 CSV가 존재하지 않거나 빈 파일입니다: {input_path}")
        df = pd.DataFrame()
    else:
        # 1) C 엔진 우선 시도 (빠르고 효율적)
        try:
            df = pd.read_csv(
                input_path,
                dtype={'authorships': str},
                encoding='utf-8-sig',
                low_memory=False
            )
        except ParserError as e:
            print(f"[real4] ParserError (C engine) 발생: {e}. python 엔진으로 재시도합니다.")
            # 2) python 엔진으로 재시도
            try:
                df = pd.read_csv(
                    input_path,
                    dtype={'authorships': str},
                    engine='python',
                    encoding='utf-8-sig',
                    on_bad_lines='error'
                )
            except Exception as e2:
                # 3) 최후 fallback: csv.DictReader 로 모든 행을 확보
                print(f"[real4] python 엔진 재시도 실패: {e2}. csv.DictReader로 최후 재시도합니다.")
                rows = []
                with open(input_path, 'r', encoding='utf-8-sig', newline='') as fh:
                    rdr = csv.DictReader(fh)
                    for row in rdr:
                        rows.append(row)
                df = pd.DataFrame(rows)
                # authorships 컬럼이 문자열로 기대되므로 강제 변환(있을 경우)
                if 'authorships' in df.columns:
                    df['authorships'] = df['authorships'].astype(str)
        except Exception as e:
            # 일반 예외(예: 인코딩 문제 등): python 엔진으로 재시도
            print(f"[real4] read_csv 일반 예외: {e}. python 엔진으로 재시도합니다.")
            try:
                df = pd.read_csv(
                    input_path,
                    dtype={'authorships': str},
                    engine='python',
                    encoding='utf-8-sig',
                    on_bad_lines='error'
                )
            except Exception as e2:
                print(f"[real4] python 엔진 재시도 실패: {e2}. csv.DictReader로 최후 재시도합니다.")
                rows = []
                with open(input_path, 'r', encoding='utf-8-sig', newline='') as fh:
                    rdr = csv.DictReader(fh)
                    for row in rdr:
                        rows.append(row)
                df = pd.DataFrame(rows)
                if 'authorships' in df.columns:
                    df['authorships'] = df['authorships'].astype(str)

    # authorships 컬럼 방어: 없으면 빈 문자열 컬럼 추가, 있으면 NaN -> '' 처리 후 문자열로 변환
    if 'authorships' not in df.columns:
        df['authorships'] = ""
    else:
        df['authorships'] = df['authorships'].fillna('').astype(str)

    # now safe to extract RORs
    df['ror'] = df['authorships'].apply(extract_rors_via_regex_or_json)

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
        return

    # before: authorships에서 (regex+JSON 기준) ROR을 하나도 못 찾은 행 수
    ror_missing_before = int(
        df['authorships'].apply(lambda s: len(extract_rors_via_regex_or_json(s)) == 0).sum()
    )

    # after: 추출된 ror 리스트가 비어 있는 행 수 (동일 기준)
    ror_missing_after = int(
        df['ror'].apply(lambda v: (len(v) if isinstance(v, list) else 0) == 0).sum()
    )

    # 파일명 규칙에 맞춰 metrics 업데이트 (anchor_path가 있으면 같은 폴더에 기록)
    _update_metrics(
        prefix, year_start, year_end,
        ror_missing_before_extract=int(ror_missing_before),
        ror_missing_after_extract=int(ror_missing_after),
        __anchor=(anchor_path or output_csv),
    )

    print(f"[real4] WROTE rows: {len(df)} to {output_csv}")
    df.to_csv(output_csv, index=False, encoding='utf-8')

    print(f"[real4] ROR missing: before={ror_missing_before}, after={ror_missing_after}, delta={ror_missing_before - ror_missing_after}")




if __name__ == "__main__":
    main('0723_2015_2024_optimized_ror.csv', '0723_2015_2024_optimized_ror_extract.csv')