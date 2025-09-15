import pandas as pd
import json
import csv
import re

def main(input_json, output_csv, prefix, year_start, year_end):
    # 1️⃣ JSON 로드
    with open(input_json, 'r', encoding='utf-8') as f:
        works = json.load(f)

    # 2️⃣ authorships 직렬화
    def clean_authorships(auths):
        if not isinstance(auths, list):
            return '[]'
        s = json.dumps(auths, ensure_ascii=False, separators=(',', ':'))
        s = re.sub(r'[\n\r\t]+', ' ', s)
        s = re.sub(r' +', ' ', s)
        return s.replace('"', "'").strip()
    
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

    # 4️⃣ authorships == '[]' 인 행만 삭제 (정확히 132행)
    if 'authorships' in df.columns:
        before = len(df)
        df = df[df['authorships'].str.strip() != '[]']
        after = len(df)
        removed_authorships_empty = before - after
        print(f"→ authorships '[]' 필터: {removed_authorships_empty} 행 삭제")

        # 매트릭 JSON에 반영 (누적 업데이트)
        import json, os
        metrics_path = f"{prefix}_{year_start}_{year_end}_metrics.json"
        metrics = {}
        if os.path.exists(metrics_path):
            try:
                metrics = json.loads(open(metrics_path, "r", encoding="utf-8").read())
            except Exception:
                metrics = {}
        metrics["authorships_removed_empty_list"] = int(removed_authorships_empty)
        with open(metrics_path, "w", encoding="utf-8") as mf:
            json.dump(metrics, mf, ensure_ascii=False)
                                     

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
    df = df[df.apply(lambda r: len(r) == expected_cols, axis=1)]

    # 8️⃣ id 패턴 필터: OpenAlex 워크 ID가 아닌 행만 삭제 (끝에 붙은 2개)
    before = len(df)
    df = df[df['id'].str.match(r'^https?://openalex\.org/W\d+', na=False)]
    after = len(df)
    print(f"→ id 패턴 필터: {before-after} 행 삭제")

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

if __name__ == "__main__":
    # 0️⃣ 파일 경로
    INPUT_JSON  = '0723_2015_2024_optimized.json'
    OUTPUT_CSV  = '0723_2015_2024_optimized.csv'
    main(INPUT_JSON, OUTPUT_CSV)
