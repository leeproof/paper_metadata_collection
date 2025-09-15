import pandas as pd
import re
from pathlib import Path

def extract_rors_via_regex(s):
    if not isinstance(s, str):
        return []
    ror_pattern = re.compile(r'https?://ror\.org/[0-9a-z]+', re.IGNORECASE)
    found = ror_pattern.findall(s)
    return list(dict.fromkeys(found))  # 순서 유지하면서 중복 제거

def main(input_csv, output_csv, prefix, year_start, year_end):
    import pandas as pd
    import re
    from pathlib import Path
    def extract_rors_via_regex(s):
        if not isinstance(s, str):
            return []
        ror_pattern = re.compile(r'https?://ror\.org/[0-9a-z]+', re.IGNORECASE)
        found = ror_pattern.findall(s)
        return list(dict.fromkeys(found))
    df = pd.read_csv(Path(input_csv), dtype={'authorships': str})
    df['ror'] = df['authorships'].apply(extract_rors_via_regex)
    total = len(df)
    missing = (df['ror'].str.len() == 0).sum()
    print(f"총 행 수: {total}, ROR 미추출 행 수: {missing}")
    df.to_csv(output_csv, index=False, encoding='utf-8')

    # ✅ 메트릭 JSON 업데이트
    import json, os
    metrics_path = f"{prefix}_{year_start}_{year_end}_metrics.json"
    metrics = {}
    if os.path.exists(metrics_path):
        try:
            metrics = json.loads(open(metrics_path, "r", encoding="utf-8").read())
        except Exception:
            metrics = {}
    metrics["ror_missing_after_extract"] = int(missing)
    metrics["rows_total_after_extract"] = int(total)
    with open(metrics_path, "w", encoding="utf-8") as mf:
        json.dump(metrics, mf, ensure_ascii=False)

    # ✅ 메트릭 JSON 누적 업데이트
    import json, os
    metrics_path = f"{prefix}_{year_start}_{year_end}_metrics.json"
    metrics = {}
    if os.path.exists(metrics_path):
        try:
            metrics = json.loads(open(metrics_path, "r", encoding="utf-8").read())
        except Exception:
            metrics = {}

    metrics["ror_missing_after_extract"] = int(missing)   # 추출 후 결측 행 수
    metrics["rows_total_after_extract"] = int(total)

    with open(metrics_path, "w", encoding="utf-8") as mf:
        json.dump(metrics, mf, ensure_ascii=False)

if __name__ == "__main__":
    main('0723_2015_2024_optimized_ror.csv', '0723_2015_2024_optimized_ror_extract.csv')
