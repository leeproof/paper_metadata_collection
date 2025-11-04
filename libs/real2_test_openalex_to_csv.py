import pandas as pd
import json
import csv
import re


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



def main(input_json, output_csv, prefix, year_start, year_end,
         anchor_path: str | None = None):
    # 1️⃣ JSON 로드
    with open(input_json, 'r', encoding='utf-8') as f:
        works = json.load(f)

    # 2️⃣ authorships 직렬화
    def clean_authorships(auths):
        if not isinstance(auths, list):
            return '[]'
        # JSON은 JSON답게: 쌍따옴표 유지 (csv.QUOTE_ALL로 저장하므로 안전)
        s = json.dumps(auths, ensure_ascii=False, separators=(',', ':'))
        s = re.sub(r'[\n\r\t]+', ' ', s)
        s = re.sub(r' +', ' ', s)
        return s.strip()

    
    def reconstruct_abstract(inverted_index: dict) -> str:
        if not isinstance(inverted_index, dict) or not inverted_index:
            return ""
        pairs = []
        for word, positions in inverted_index.items():
            if isinstance(positions, list):
                for pos in positions:
                    if isinstance(pos, int):
                        pairs.append((pos, word))
        pairs.sort(key=lambda x: x[0])
        return " ".join(w for _, w in pairs)

    for w in works:
        w['authorships'] = clean_authorships(w.get('authorships', []))

    # ✅ abstract 보강: real1에서 없었거나 비어 있으면 여기서 복원
    for w in works:
        if not w.get('abstract'):
            aii = w.get('abstract_inverted_index', {})
            if isinstance(aii, dict) and aii:
                w['abstract'] = reconstruct_abstract(aii)

        if 'abstract_inverted_index' in w:
            w.pop('abstract_inverted_index', None)

    leaks = [i for i, w in enumerate(works) if isinstance(w.get('abstract_inverted_index'), dict)]
    print("abstract_inverted_index dict가 남아있는 행 수:", len(leaks))

    # 3️⃣ 평탄화
    df = pd.json_normalize(works, sep='_')

    # ✅ abstract_inverted_index 관련 컬럼 방어적으로 삭제
    drop_cols = [c for c in df.columns if 'abstract_inverted_index' in c]
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True)
        print(f"abstract_inverted_index 관련 컬럼 삭제: {len(drop_cols)}개")
    else:
        [print("abstract_inverted_index 관련 컬럼 없음.")]

    removed_authorships_empty_list = 0

    # 4️⃣ authorships == '[]' 인 행만 삭제
    if 'authorships' in df.columns:
        before = len(df)
        df = df[df['authorships'].str.strip() != '[]']
        after = len(df)
        removed_authorships_empty_list = before - after
        print(f"→ authorships '[]' 필터: {removed_authorships_empty_list} 행 삭제")
                                     

    # 5️⃣ 주요 필드 검증 (ror: 로 시작하는 id/doi/title 은 없다고 가정)

    # 6️⃣ 문자열 필드 정제 (줄바꿈·탭·쌍따옴표)
    def clean_string(x):
        if isinstance(x, str):
            x = x.replace('"', "'")
            x = re.sub(r'[\n\r\t]+', ' ', x)
            x = re.sub(r' +', ' ', x)
            return x.strip()
        return x
 
    for col in df.select_dtypes(include='object').columns:
        if col != 'authorships':
            df[col] = df[col].apply(clean_string)

    # 7️⃣ 컬럼 수 불일치 행 제거 (여전히 안전장치)
    expected_cols = len(df.columns)
    _before_cols = len(df)
    df = df[df.apply(lambda r: len(r) == expected_cols, axis=1)]
    _after_cols = len(df)
    col_mismatch_removed = _before_cols - _after_cols
    print(f"-> 컬럼 수 불일치 필터: {col_mismatch_removed} 행 삭제")

    id_pattern_removed = 0

    # 8️⃣ id 패턴 필터
    _before_id = len(df)
    df = df[df['id'].str.match(r'^https?://openalex\.org/W\d+', na=False)]
    _after_id =  len(df)
    id_pattern_removed = _before_id - _after_id
    print(f"-> id 패턴 필터: {id_pattern_removed} 행 삭제")


    # 9️⃣ journal 컬럼 추출
    if 'primary_location_source_display_name' in df.columns:
        df['journal'] = df['primary_location_source_display_name']
        print(f"→ journal 컬럼 생성: {df['journal'].nunique()}개 고유 저널")
    else:
        df['journal'] = None
        print("→ primary_location_source_display_name 컬럼이 없어 journal을 None으로 채웠습니다.")

    # 10 primary_location 필드 내 저널명만 남기고 삭제
    drop_cols = [c for c in df.columns if c.startswith('primary_location')]
    df.drop(columns=drop_cols, inplace=True)
    print(f"primary_location 컬럼 삭제: {drop_cols}")

    # CSV 저장
    df.to_csv(
        output_csv,
        index=False,
        encoding='utf-8',
        quoting=csv.QUOTE_ALL,
        lineterminator='\n'
    )

    print("✅ 최종 CSV:", output_csv)
    print("   JSON 원본:", len(works), "행")
    print("   최종 CSV:", len(df), "행")


    # Editorial Materials 삭제 이후 -> DOI 결측 수 집계
    doi_missing_after = int((df.get("doi").isna() | (df["doi"].astype(str).str.strip() == "")).sum()) if "doi" in df.columns else 0

    # 방금 저장된 CSV 다시 읽기(현재 기준)
    df2 = pd.read_csv(output_csv)

    # real1에서 덤프한 "보강 전 결측 ID" 로드
    sidecar_path = Path(f"{prefix}_{year_start}_{year_end}_missing_doi_ids.json")
    if sidecar_path.exists():
        with open(sidecar_path, "r", encoding="utf-8") as f:
            missing_doi_ids = set(json.load(f))
    else:
        missing_doi_ids = set()

    # 현재 DF의 남아있는 work id 집합
    present_ids = set(df2["id"].astype(str))

    # ① doi_missing = "보강 전 결측 ID" ∩ "편집물 삭제 후 남아있는 ID"
    doi_missing = len(missing_doi_ids & present_ids)

    # ② 현재(보강 후 + 편집물 삭제 후)의 DOI 결측 수
    _cur = df2["doi"].fillna("").astype(str).str.strip().str.lower() if "doi" in df2.columns else pd.Series([], dtype=str)
    doi_missing_after = int(((_cur.eq("")) | (_cur.eq("nan")) | (_cur.eq("null"))).sum())

    # ③ doi_enriched / rate
    doi_enriched = max(0, doi_missing - doi_missing_after)
    doi_enrich_rate = round((doi_enriched / doi_missing) * 100, 6) if doi_missing else 0.0

    _update_metrics(
        prefix, year_start, year_end,
        json_rows=len(works),
        final_csv_rows=len(df2),
        authorships_removed_empty_list=removed_authorships_empty_list,
        id_pattern_removed=locals().get("id_pattern_removed", 0),
        # ★ 스키마 불변: 정확한 의미로 덮어쓰기
        doi_missing=int(doi_missing),
        doi_enriched=int(doi_enriched),
        doi_enrich_rate=doi_enrich_rate,
        __anchor=(anchor_path or output_csv),
    )


if __name__ == "__main__":
    # 0️⃣ 파일 경로
    INPUT_JSON  = '0723_2015_2024_optimized.json'
    OUTPUT_CSV  = '0723_2015_2024_optimized.csv'
    main(INPUT_JSON, OUTPUT_CSV, prefix="0723", year_start=2015, year_end=2024)
