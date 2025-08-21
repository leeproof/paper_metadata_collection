# paper_metadata_collection
1. app.py 실행 -> streamlit 어플 구동
2. 구동된 streamlit 어플 내에서 논문 메타데이터 수집 파이프라인 활용

# 내부
1. real1
  OpenAlex API 활용, 논문 메타데이터 수집 (ISSN, 연도 기간 입력)
  Crossref API 활용, 결측된 DOI 보강 작업 수행 (배치 단위로)
  JSON 파일 생성
2. real2
  1을 통해 수집된 메타데이터(JSON파)를 CSV파일로 변환 수행
3. real3
  ROR API 활용, 결측된 ROR ID 보강 작업 수행
4. real4
  Column 'authorships'의 'institutions 리스트 내 딕셔너리 키-값 중, 'ror'값을 추출
  Column 'ror'을 생성하여 추출한 'ror'값 저장
5. real5
  같은 행 내 중복된 ror id 삭제
  ROR API 활용, Column 'ror'의 각 행에 입력된 ror id에 따른 문자열(기관명) 수집
  Column 'org_names'을 생성하여 수집한 기관명 저장
6. real6
Column 'org_names' 활용, 협업 네트워크 생성
  1> 노드 크기: 전체 행에서 기관명 출현 빈도
  2> 엣지 두께: 같은 행에서 기관명 동시 출현 빈도
