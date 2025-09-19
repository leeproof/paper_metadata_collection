import os, json, pandas as pd, asyncio
from pathlib import Path
from typing import List, Optional
from pyalex import Sources, config
import re

# 각 단계별 기존 스크립트 import (함수화)
from libs import real1_test_openalex_sequential_optimized_0722 as real1
from libs import real2_test_openalex_to_csv as real2
from libs import real3_ror_mapping_processor_copy as real3
from libs import real4_ror_extract as real4
from libs import real5_name_mapping as real5
from libs import real6_visualize_fast as real6


# Google Drive adapter
import json as _json
import ast as _ast
from collections.abc import Mapping

try:
    import streamlit as st
except Exception:
    st = None # 로컬 환경에서도 동작하도록 선택적 import
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

def run_pipeline(issns: List[str], year_start: int, year_end: int,
                 email: str = 's0124kw@gmail.com', include_only_with_abstract: bool = False,
                 make_html: bool = False):

    """
    전체 파이프라인 실행 함수
    issns: 저널 ISSN 리스트
    year_start, year_end: 연도 범위
    email: OpenAlex API용 이메일
    """

    """
    파일명 prefix 계산
    """
    config.email = email
    journal_prefixes = []
    for issn in issns:
        try:
            src = next(
                Sources().filter(issn=[issn])
                         .select(['display_name'])
                         .paginate(per_page=1)
            )
            display = src[0]['display_name'] if isinstance(src, list) else src['display_name']
        except Exception:
            display = issn  # fallback: ISSN 그대로 사용
        safe = re.sub(r'\W+', '_', display).strip('_')
        journal_prefixes.append(safe)
    prefix = "-".join(journal_prefixes)


    # 1️⃣ 논문 메타데이터 수집 및 DOI 보강 (real1)
    json_base = f"{prefix}_{year_start}_{year_end}"
    json_parts = [f"{json_base}_part{i+1}.json" for i in range(3)]
    json_merged = f"{json_base}.json"
    csv_file = f"{json_base}.csv"
    csv_ror = f"{json_base}_ror.csv"
    csv_ror_extract = f"{json_base}_ror_extract.csv"
    csv_ror_extract_name = f"{json_base}_ror_extract_name.csv"
    html_ror_extract_name_network = f"{json_base}_ror_extract_name_network.html"
    cache_file = Path("ror_cache.pkl")

    # 1. real1: 논문 수집 및 보강
    print("[1/7] 논문 메타데이터 수집 및 DOI 보강...")
    real1.main(issns=issns, year_start=year_start, year_end=year_end, email=email, prefix=prefix, include_only_with_abstract=include_only_with_abstract)

    # 2. real2: JSON → CSV 변환
    print("[2/7] JSON → CSV 변환...")
    real2.main(input_json=json_merged, output_csv=csv_file, prefix=prefix, year_start=year_start, year_end=year_end)

    # 3. real3: ROR 매핑
    print("[3/7] ROR 매핑...")
    asyncio.run(real3.process(
        input_csv=Path(csv_file),
        output_csv=Path(csv_ror),
        cache_file=cache_file,
        concurrency=20
    ))

    # 4. real4: ROR 추출
    print("[4/7] ROR 추출 및 통계...")
    real4.main(input_csv=csv_ror, output_csv=csv_ror_extract, prefix=prefix, year_start=year_start, year_end=year_end)

    # 5. real5: ROR ID → 기관명 매핑
    print("[5/7] ROR ID → 기관명 매핑...")
    real5.main(input_csv=csv_ror_extract, output_csv=csv_ror_extract_name, prefix=prefix, year_start=year_start, year_end=year_end)

    # 6. real6: 협력 네트워크 시각화 (HTML 생성)
    html_path = None
    if make_html:
        print("[6/7] 협력 네트워크 시각화...")
        html_path = real6.main(
            input_csv=csv_ror_extract_name,
            output_html=html_ror_extract_name_network,
            size_by="eigenvector",
            color_by="eigenvector"
        )
        if html_path:
            print(f"→ 시각화 파일 생성: {html_path}")


    print("[7/7] 전체 파이프라인 완료!")

    return csv_ror_extract_name, html_path


def make_html_from_csv(final_csv_path: str) -> str:
    """
    최종 CSV로 시각화를 2가지 버전(degree / eigenvector)으로 생성한 뒤,
    하나의 HTML에 두 뷰를 iframe으로 나란히 담아 반환(함수 시그니처/리턴타입 변경 없음).
    """
    if not final_csv_path.endswith("_ror_extract_name.csv"):
        raise ValueError("final_csv_path must end with _ror_extract_name.csv")

    base = final_csv_path[:-len("_ror_extract_name.csv")]

    # 1) 개별 시각화 HTML 생성(같은 폴더에 유지)
    html_degree = f"{base}_degree_network.html"
    html_eigen  = f"{base}_eigenvector_network.html"

    # 권장 조합: 크기=degree / 색=eigenvector  &  크기=eigenvector / 색=degree
    real6.main(
        input_csv=final_csv_path,
        output_html=html_degree,
        size_by="degree",
        color_by="eigenvector"
    )
    real6.main(
        input_csv=final_csv_path,
        output_html=html_eigen,
        size_by="eigenvector",
        color_by="degree"
    )

    # 2) 같은 폴더의 두 HTML을 iframe으로 불러오는 래퍼 생성
    deg_name = Path(html_degree).name
    eig_name = Path(html_eigen).name

    output_html = final_csv_path.replace(
        "_ror_extract_name.csv",
        "_ror_extract_name_network.html"
    )

    wrapper = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Collaboration Networks (Degree & Eigenvector)</title>
  <style>
    body {{ background:#111; color:#eaeaea; font-family:system-ui, sans-serif; }}
    .row {{ display:flex; gap:12px; flex-wrap:wrap; padding:12px; }}
    .panel {{ flex:1 1 48%; min-width:380px; border:1px solid #333; border-radius:8px; background:#151515; }}
    .panel h3 {{ margin:8px 12px; font-weight:600; }}
    iframe {{ width:100%; height:820px; border:0; background:#111; border-top:1px solid #333; }}
  </style>
</head>
<body>
  <div class="row">
    <div class="panel">
      <h3>Degree-based view (size=degree, color=eigenvector)</h3>
      <iframe src="{deg_name}"></iframe>
    </div>
    <div class="panel">
      <h3>Eigenvector-based view (size=eigenvector, color=degree)</h3>
      <iframe src="{eig_name}"></iframe>
    </div>
  </div>
</body>
</html>
"""
    Path(output_html).write_text(wrapper, encoding="utf-8")
    return output_html

def make_html_string_from_csv(final_csv_path: str, size_by: str, color_by: str) -> str:
    """
    최종 CSV에서 네트워크 HTML 문자열 생성(파일 저장 없음).
    - size_by: "degree" 또는 "eigenvector"
    - color_by: "eigenvector" 또는 "degree"
    """
    res = real6.main(
        input_csv=final_csv_path,
        output_html=None,      # ← 파일 저장 안 함
        size_by=size_by,
        color_by=color_by
    )

    # 안전장치: 혹시 경로 문자열이 돌아오면 파일을 읽어서 HTML 문자열로 치환
    from pathlib import Path as _Path
    if isinstance(res, str):
        s = res.strip().lower()
        if s.startswith("<!doctype") or s.startswith("<html"):
            return res
        if res.endswith(".html") and _Path(res).exists():
            return _Path(res).read_text(encoding="utf-8")
    raise RuntimeError("HTML 생성 실패: 반환값이 비었거나 HTML이 아닙니다.")


# 저장 전략 설정
USE_JOURNAL_NAME_SLUG: bool = True
LOCAL_WORKDIR = Path("workdir_tmp")
LOCAL_WORKDIR.mkdir(parents=True, exist_ok=True)

_slug_rx = re.compile(r"[^a-zA-Z0-9]+")
def _to_slug(name: str) -> str:
    return _slug_rx.sub("_", name.strip().lower()).strip("_")

_JSLUG_CACHE: dict[str, str] = {}
def _resolve_journal_name_slug(issn: str) -> str:
    """ISSN -> OpenAlex display_name -> 파일시스템 안전 슬러그"""
    if issn in _JSLUG_CACHE:
        return _JSLUG_CACHE[issn]
    try:
        src = next(
            Sources().filter(issn=[issn])
                    .select(['display_name'])
                    .paginate(per_page=1)
        )
        display = src[0]['display_name'] if isinstance(src, list) else src['display_name']
        base_name = display
    except Exception:
        base_name = issn
    slug = _to_slug(base_name)
    _JSLUG_CACHE[issn] = slug
    return slug

def _key_name_for(issn: str) -> str:
    """파일명/폴더명에 쓸 키(저널명 슬러그 또는 ISSN)"""
    return _resolve_journal_name_slug(issn) if USE_JOURNAL_NAME_SLUG else issn

# ----- Secrets/ENV에서 Drive 인증/폴더 정보 읽기 -----

def _get_gdrive_root_folder_id() -> Optional[str]:
    """
    Streamlit Cloud Secrets 또는 환경변수에서 루트 폴더 ID 읽기
    - secrets: GDRIVE_FOLDER_ID
    - env    : GDRIVE_FOLDER_ID
    """
    if st is not None:
        try:
            return st.secrets["GDRIVE_FOLDER_ID"]
        except Exception:
            pass
    return os.getenv("GDRIVE_FOLDER_ID")


def _get_service_account_info() -> Optional[dict]:
    """
    ✅ 오직 Streamlit secrets의 [gcp_service_account]만 지원
       (필요 시 ENV GDRIVE_SA_JSON JSON 문자열은 보조용)
    - 문자열/딕셔너리 모두 허용
    - private_key 의 '\\n' → 실제 개행으로 치환
    """
    raw = None
    # 1) Streamlit secrets
    if st is not None:
        try:
            raw = st.secrets["gcp_service_account"]
        except Exception:
            print("[GDRIVE] gcp_service_account not found in st.secrets")
            raw = None
    # 2) (선택) 환경변수 JSON 보조
    if raw is None:
        raw = os.getenv("GDRIVE_SA_JSON")
        if raw is None:
            print("[GDRIVE] GDRIVE_SA_JSON not found in env")
    if raw is None:
        return None

    # 딕셔너리면 그대로 사용
    if isinstance(raw, Mapping):
        sa = dict(raw)
    else:
        s = str(raw).strip()
        # JSON 먼저 시도
        try:
            sa = _json.loads(s)
        except Exception:
            # 일부 환경에서 단일따옴표 dict 문자열로 들어온 경우
            try:
                sa = _ast.literal_eval(s)
                if not isinstance(sa, Mapping):
                    raise ValueError("literal_eval did not return a mapping")
                sa = dict(sa)
            except Exception as e:
                print("[GDRIVE] Could not parse gcp_service_account string:", repr(e))
                print("[GDRIVE] First 120 chars:", s[:120])
                return None

    # private_key 개행 정규화
    pk = sa.get("private_key")
    if not pk:
        print("[GDRIVE] private_key missing in gcp_service_account")
        return None
    # sa["private_key"] = str(pk).replace("\\n", "\n")
    return sa


def _gdrive_client():
    # ✅ oauth2client 기반으로 변경
    from oauth2client.service_account import ServiceAccountCredentials
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive

    sa_info = _get_service_account_info()
    if not sa_info:
        raise RuntimeError("Google Drive 서비스계정 정보가 없습니다. secrets 또는 ENV를 확인하세요.")

    scopes = ['https://www.googleapis.com/auth/drive']
    # 핵심: google-auth 대신 oauth2client로 크리덴셜 생성
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scopes=scopes)

    gauth = GoogleAuth()
    gauth.credentials = creds
    gauth.Authorize()  # ← oauth2client 크리덴셜은 authorize() 지원
    drive = GoogleDrive(gauth)
    return drive


def _find_folder(drive: GoogleDrive, parent_id: str, name: str) -> Optional[str]:
    q = f"'{parent_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and title='{name}'"
    lst = drive.ListFile({'q': q, 'includeItemsFromAllDrives': True, 'supportsAllDrives': True}).GetList()
    return lst[0]['id'] if lst else None

def _ensure_folder(drive: GoogleDrive, parent_id: str, name: str) -> str:
    fid = _find_folder(drive, parent_id, name)
    if fid:
        return fid
    f = drive.CreateFile({'title': name, 'parents':[{'id': parent_id}],
                          'mimeType': 'application/vnd.google-apps.folder'})
    f.Upload()
    return f['id']

def _find_file(drive: GoogleDrive, parent_id: str, name: str) -> Optional[str]:
    q = f"'{parent_id}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder' and title='{name}'"
    lst = drive.ListFile({'q': q, 'includeItemsFromAllDrives': True, 'supportsAllDrives': True}).GetList()
    return lst[0]['id'] if lst else None

def _download_file(drive: GoogleDrive, file_id: str, local_path: Path):
    f = drive.CreateFile({'id': file_id})
    local_path.parent.mkdir(parents=True, exist_ok=True)
    f.GetContentFile(str(local_path))

def _upload_file(drive: GoogleDrive, parent_id: str, local_path: Path, name: Optional[str] = None):
    name = name or local_path.name
    f = drive.CreateFile({'title': name, 'parents':[{'id': parent_id}]})
    f.SetContentFile(str(local_path))
    f.Upload(param={'supportsAllDrives': True})
    return f['id']

# ----- issn/year 원격 파일 위치 -----
def _gdrive_locate_piece(issn: str, year: int):
    """
    ROOT / <key> / <year> / <key>_<year>_ror_extract_name.csv
    """
    root_id = _get_gdrive_root_folder_id()
    if not root_id:
        raise RuntimeError("GDRIVE_FOLDER_ID가 설정되지 않았습니다.")
    key = _key_name_for(issn)
    drive = _gdrive_client()
    key_folder = _ensure_folder(drive, root_id, key)
    year_folder = _ensure_folder(drive, key_folder, str(year))
    fname = f"{key}_{year}_ror_extract_name.csv"
    file_id = _find_file(drive, year_folder, fname)
    return drive, key, key_folder, year_folder, fname, file_id

def _gdrive_piece_exists(issn: str, year: int) -> bool:
    try:
        _, _, _, _, _, file_id = _gdrive_locate_piece(issn, year)
        return file_id is not None
    except Exception:
        return False

def _gdrive_download_piece(issn: str, year: int, local_path: Path) -> bool:
    try:
        drive, _, _, _, _, file_id = _gdrive_locate_piece(issn, year)
        if not file_id:
            return False
        _download_file(drive, file_id, local_path)
        return True
    except Exception:
        return False

def _gdrive_upload_piece(issn: str, year: int, local_path: Path):
    drive, _, _, year_folder, fname, _ = _gdrive_locate_piece(issn, year)
    _upload_file(drive, year_folder, local_path, fname)


# ======================================================================
# 연·저널 단위 저장/재사용 (Drive 사용)
# ======================================================================

# [# ADDED] ISSN 정규화
_issn_rx = re.compile(r"^\d{4}-\d{3}[\dxX]$")

def _normalize_issn_list(issns: List[str]) -> List[str]:
    norm = []
    for s in issns:
        if not s:
            continue
        s = s.strip()
        if "-" not in s and len(s) == 8:
            s = s[:4] + "-" + s[4:]
        if _issn_rx.match(s):
            norm.append(s.upper())
        else:
            print(f"[WARN] 잘못된 ISSN 형식 건너뜀: {s!r}")
    # 입력 순서 유지 중복 제거
    seen, out = set(), []
    for t in norm:
        if t not in seen:
            out.append(t); seen.add(t)
    return out

# [# ADDED] 로컬 임시(조각 다운로드/생성 위치)
def _local_piece_path(issn: str, year: int) -> Path:
    key = _key_name_for(issn)
    return LOCAL_WORKDIR / key / str(year) / f"{key}_{year}_ror_extract_name.csv"

# [# ADDED] 연·저널 1건 처리 (있으면 다운로드, 없으면 생성 후 업로드)
def _run_one_piece(issn: str, year: int, email: str,
                   include_only_with_abstract: bool = False) -> Path:
    local_out = _local_piece_path(issn, year)

    # 1) 원격 존재 시 → 다운로드
    if _gdrive_piece_exists(issn, year):
        if _gdrive_download_piece(issn, year, local_out):
            return local_out

    # 2) 없으면 생성
    local_out.parent.mkdir(parents=True, exist_ok=True)
    prefix = _key_name_for(issn)

    config.email = email
    real1.main(issns=[issn], year_start=year, year_end=year,
               email=email, prefix=prefix, include_only_with_abstract=include_only_with_abstract,
               anchor_path=str(local_out))

    json_merged   = f"{prefix}_{year}_{year}.json"
    tmp_csv       = f"{prefix}_{year}_{year}.csv"
    tmp_csv_ror   = f"{prefix}_{year}_{year}_ror.csv"
    tmp_csv_ror_ex= f"{prefix}_{year}_{year}_ror_extract.csv"
    tmp_csv_name  = f"{prefix}_{year}_{year}_ror_extract_name.csv"

    real2.main(input_json=json_merged, output_csv=tmp_csv,
               prefix=prefix, year_start=year, year_end=year,
               anchor_path=str(local_out))

    asyncio.run(real3.process(
        input_csv=Path(tmp_csv),
        output_csv=Path(tmp_csv_ror),
        cache_file=Path("ror_cache.pkl"),
        concurrency=20, anchor_path=str(local_out)
    ))

    real4.main(input_csv=tmp_csv_ror, output_csv=tmp_csv_ror_ex,
               prefix=prefix, year_start=year, year_end=year,
               anchor_path=str(local_out))

    real5.main(input_csv=tmp_csv_ror_ex, output_csv=tmp_csv_name,
               prefix=prefix, year_start=year, year_end=year)

    # 최종 조각: 로컬 표준 위치로 이동
    Path(tmp_csv_name).parent.mkdir(parents=True, exist_ok=True)
    Path(tmp_csv_name).replace(local_out)

    # 3) 원격 업로드
    _gdrive_upload_piece(issn, year, local_out)
    #연도 단위 처리 후 잠깐 쉬기(백오프)
    # 429 에러 완화(ROR 쿼리 몰리면 발생)
    import time, random
    time.sleep(5 + random.uniform(0, 0.5))

    return local_out

# [# ADDED] 조각 병합 → 최종 CSV
def _collect_merge(issns: List[str], year_start: int, year_end: int) -> Path:
    issns = _normalize_issn_list(issns)
    piece_paths: List[Path] = []
    keys: List[str] = []

    for issn in issns:
        key = _key_name_for(issn)
        keys.append(key)
        for y in range(year_start, year_end + 1):
            local_piece = _local_piece_path(issn, y)
            if local_piece.exists():
                piece_paths.append(local_piece)
            else:
                if _gdrive_download_piece(issn, y, local_piece):
                    piece_paths.append(local_piece)

    if not piece_paths:
        raise FileNotFoundError("선택 범위의 조각 CSV가 없습니다.")

    dfs = [pd.read_csv(p) for p in piece_paths]
    merged = pd.concat(dfs, ignore_index=True)

    # [# ADDED] 중복 제거(가능하면 DOI 기준)
    if "doi" in merged.columns:
        merged = merged.drop_duplicates(subset=["doi"])
    else:
        merged = merged.drop_duplicates()

    # 보기 좋은 열 순서(선택)
    preferred = [c for c in ["title", "doi", "published_year", "host_venue_issn_l",
                             "institution_name", "ror_id"] if c in merged.columns]
    merged = merged[[*preferred, *[c for c in merged.columns if c not in preferred]]]

    merged_name = f"{'-'.join(keys)}_{year_start}_{year_end}_ror_extract_name.csv"
    out_path = LOCAL_WORKDIR / merged_name
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


# ======================================================================
# [엔드포인트] run_pipeline_cached — app.py가 호출 (Drive 영구 저장)
# ======================================================================
def run_pipeline_cached(issns: List[str], year_start: int, year_end: int,
                        email: str = 's0124kw@gmail.com',
                        include_only_with_abstract: bool = False,
                        make_html: bool = False,
                        base_dir: Path = Path("storage")):
    """
    1) 각 저널×연도 조각이 있으면 재사용, 없으면 생성 후 Drive 업로드
    2) 조각들을 모아 로컬에서 최종 CSV 병합
    3) (옵션) HTML 생성
    주의: base_dir 인자는 호환성만 유지(Drive 사용으로 무시)
    """
    issns = _normalize_issn_list(issns)

    for issn in issns:
        for y in range(int(year_start), int(year_end) + 1):
            _run_one_piece(issn, y, email, include_only_with_abstract)

    final_csv_path = _collect_merge(issns, int(year_start), int(year_end))

    html_path = None
    if make_html:
        html_path = make_html_from_csv(str(final_csv_path))

    return str(final_csv_path), html_path


# ======================================================================
# (옵션) 로컬 단독 테스트
# ======================================================================
if __name__ == "__main__":
    example_issns = ['0043-1354','0011-9164','0733-9429']
    out_csv, _ = run_pipeline_cached(
        issns=example_issns,
        year_start=2017, year_end=2019,
        email='s0124kw@gmail.com',
        include_only_with_abstract=False,
        make_html=False
    )
    print("FINAL:", out_csv)