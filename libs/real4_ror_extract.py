import pandas as pd
import re
from pathlib import Path


# === ADD (공통 헬퍼) ==========================================
def _update_metrics(prefix, year_start, year_end, **updates):
    import os, json
    path = f"{prefix}_{year_start}_{year_end}_metrics.json"
    data = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as rf:
                data = json.load(rf) or {}
        except Exception:
            data = {}
    for k, v in updates.items():
        if v is None: 
            continue
        # 숫자는 int로 정규화
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            v = int(v)
        data[k] = v
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