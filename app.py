import streamlit as st
import pandas as pd
from curl_cffi import requests
from bs4 import BeautifulSoup, SoupStrainer
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import os

# 💡 구글 드라이브 인증 함수
def authenticate_gdrive_with_secrets():
    gauth = GoogleAuth()

    # Streamlit Secrets에서 데이터를 직접 가져와 설정
    # 박사님의 서비스 계정 정보가 메모리상에서 바로 전달됩니다.
    credentials = dict(st.secrets["gdrive"])

    settings = {
        "client_config_backend": "service",
        "service_config": {
            "client_json_dict": credentials, # 파일 경로 대신 딕셔너리(dict) 전달
        }
    }
    gauth.LoadCredentialsFromSettings(settings)
    return GoogleDrive(gauth)

# 💡 파일 업로드/업데이트 함수
def upload_to_gdrive(drive, local_path, folder_id):
    # 폴더 내에 동일한 이름의 파일이 있는지 확인 (있으면 덮어쓰기)
    file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
    
    target_file = None
    for file in file_list:
        if file['title'] == os.path.basename(local_path):
            target_file = file
            break
            
    if target_file:
        target_file.SetContentFile(local_path)
    else:
        target_file = drive.CreateFile({'title': os.path.basename(local_path), 'parents': [{'id': folder_id}]})
        target_file.SetContentFile(local_path)
    
    target_file.Upload()

# ---------------------------------------------------------
# 기본 웹페이지 설정 및 최적화 세션
# ---------------------------------------------------------
st.set_page_config(page_title="지방재정365 하이브리드 수집기", page_icon="🏛️", layout="wide")
st.title("🏛️ 지방재정365 세부사업 데이터 수집기 (초고속/매핑 통합본)")
st.markdown("""
이 웹 프로그램은 지방재정365 내부 API와 로컬 예산 데이터를 결합하여, 핵심 사업의 텍스트만 초고속으로 수집합니다.
""")

session_step1 = requests.Session(impersonate="chrome")
session_step2 = requests.Session(impersonate="chrome")

# ---------------------------------------------------------
# 💡 [새로 추가된 만능 헬퍼 함수] 어떤 CSV/엑셀이든 완벽하게 읽어냅니다.
# ---------------------------------------------------------
def load_safe_df(file_obj):
    file_obj.seek(0) # 포인터(책갈피)를 파일 맨 처음으로 초기화
    ext = file_obj.name.lower() # 대문자 .CSV를 소문자로 통일
    
    if ext.endswith('.csv'):
        # 공공데이터에서 주로 쓰이는 3가지 인코딩과 헤더 위치(0, 1)를 모두 시도
        for enc in ['utf-8', 'cp949', 'euc-kr']:
            for h in [0, 1]:
                try:
                    file_obj.seek(0)
                    df = pd.read_csv(file_obj, header=h, encoding=enc)
                    df.columns = df.columns.str.strip() # 컬럼명 공백 싹 제거
                    if any(col in df.columns for col in ['자치단체코드', '지역', '세부사업명']):
                        return df
                except Exception:
                    continue
        file_obj.seek(0)
        return pd.read_csv(file_obj, encoding='cp949', on_bad_lines='skip')
    else:
        for h in [0, 1]:
            try:
                file_obj.seek(0)
                df = pd.read_excel(file_obj, header=h)
                df.columns = df.columns.str.strip()
                if any(col in df.columns for col in ['자치단체코드', '지역', '세부사업명']):
                    return df
            except Exception:
                continue
        file_obj.seek(0)
        return pd.read_excel(file_obj)

# ---------------------------------------------------------
# [1단계 헬퍼 함수] API 데이터 수집
# ---------------------------------------------------------
def fetch_region_data(region, year, api_key):
    laf_cd = str(region['자치단체코드'])
    laf_nm = str(region['자치단체명'])
    api_url = "https://www.lofin365.go.kr/lf/hub/QWGJK"
    
    region_data = []
    pIndex = 1
    
    while True:
        payload = {
            'Key': api_key, 'Type': 'json', 'pIndex': pIndex, 'pSize': 1000,
            'fyr': year, 'laf_cd': laf_cd, 'exe_ymd': f"{year}1231"
        }
        try:
            response = session_step1.get(api_url, params=payload, timeout=15)
            data = response.json()
            
            try:
                items = data['QWGJK'][1]['row']
            except (KeyError, IndexError):
                items = []
                
            if not items: break
                
            for item in items:
                region_data.append({
                    '회계연도': item.get('fyr'),
                    '지자체코드': item.get('laf_cd'),
                    '지자체명': item.get('laf_hg_nm'),
                    '세부사업코드': item.get('dbiz_cd'),
                    '세부사업명': item.get('dbiz_nm'),
                    '예산현액': item.get('bdg_cash_amt', 0),
                    '지출액': item.get('ep_amt', 0)
                })
            
            if len(items) < 1000: break
            pIndex += 1
            time.sleep(0.01) 
            
        except Exception:
            break
            
    return region_data, laf_nm

# ---------------------------------------------------------
# UI 탭 설정
# ---------------------------------------------------------
tab1, tab2 = st.tabs(["[1단계] 사업목록 추출 및 매핑", "[2단계] 사업개요 추출"])

with tab1:
    st.header("1. API 사업목록 추출 및 로컬 데이터 병합")
    
    col1, col2 = st.columns(2)
    with col1:
        api_key = st.text_input("API 인증키 (Decoding Key)", type="password")
    with col2:
        years_list = [str(y) for y in range(2016, 2026)]
        target_year = st.selectbox("조회할 회계연도", years_list, index=len(years_list)-3)
        
    region_file = st.file_uploader("🗺️ [필수] 지역코드 파일 업로드 (CSV/Excel)", type=['csv', 'xlsx'])
    local_budget_file = st.file_uploader("📊 [필수] 로컬 예산현황 파일 업로드 (CSV/Excel)", type=['csv', 'xlsx'])
    
    selected_sido = []
    df_region = pd.DataFrame()
    
    if region_file is not None:
        try:
            # 💡 만능 함수로 에러 없이 파일 로드!
            df_region = load_safe_df(region_file)
            
            if '지역' in df_region.columns:
                unique_sido = df_region['지역'].dropna().unique().tolist()
                selected_sido = st.multiselect("📍 수집할 광역 단위 선택 (여러 개 선택 가능)", unique_sido, default=unique_sido)
            else:
                st.warning("업로드된 파일에 '지역' 컬럼이 없어 전체 지자체를 대상으로 합니다.")
        except Exception as e:
            st.error(f"파일을 읽는 중 오류가 발생했습니다: {e}")

    if st.button("🚀 1단계 추출 및 매핑 시작", key="btn_step1"):
        if not api_key or region_file is None or local_budget_file is None:
            st.error("API 인증키, 지역코드 파일, 로컬 예산현황 파일을 모두 확인해주세요!")
        else:
            with st.spinner("API 추출을 시작합니다..."):
                df_region_filtered = df_region[df_region['지역'].isin(selected_sido)] if selected_sido else df_region
                unique_regions = df_region_filtered[['자치단체코드', '자치단체명']].drop_duplicates().to_dict('records')
                
                target_list = []
                prog_bar_1 = st.progress(0)
                status_1 = st.empty()
                completed_count = 0
                
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [executor.submit(fetch_region_data, region, target_year, api_key) for region in unique_regions]
                    for future in as_completed(futures):
                        result_data, region_name = future.result()
                        if result_data:
                            target_list.extend(result_data)
                        completed_count += 1
                        prog_bar_1.progress(int((completed_count / len(unique_regions)) * 100))
                        status_1.text(f"🚀 API 수집 중... [{completed_count}/{len(unique_regions)}]")
                        time.sleep(0.01)
                
                if target_list:
                    # API 데이터 정리
                    df_api = pd.DataFrame(target_list).drop_duplicates(subset=['회계연도', '지자체코드', '세부사업코드'])
                    df_api['지자체명'] = df_api['지자체명'].astype(str).str.strip()
                    df_api['세부사업명'] = df_api['세부사업명'].astype(str).str.strip()
                    
                    status_1.text("💡 로컬 파일 로드 및 전처리 중... (고속 연산)")
                    
                    # 💡 만능 함수로 로컬 예산 파일 로드
                    df_local = load_safe_df(local_budget_file)
                    
                    df_local = df_local.dropna(subset=['지역', '자치단체', '세부사업명'])
                    df_local['지자체명'] = (df_local['지역'].astype(str).str.strip() + df_local['자치단체'].astype(str).str.strip())
                    df_local['세부사업명'] = df_local['세부사업명'].astype(str).str.strip()
                    
                    # 일반공공행정, 기타, 예비비 완벽 필터링(일상경비 노이즈 제거)
                    df_local = df_local[~df_local['분야'].astype(str).str.contains('일반공공행정|일반행정|기타|예비비', na=False)]
                    
                    status_1.text("💡 중복 사업 압축 중... (병목 최적화)")
                    
                    # 🚀 고속 연산 (drop_duplicates 후 unique 사용)
                    df_local_sub = df_local[['지자체명', '세부사업명', '회계', '분야', '부문']].drop_duplicates()
                    df_local_sub[['회계', '분야', '부문']] = df_local_sub[['회계', '분야', '부문']].fillna('').astype(str)
                    
                    df_local_agg = df_local_sub.groupby(['지자체명', '세부사업명'], as_index=False).agg({
                        '회계': lambda x: ', '.join([i for i in x.unique() if i]),
                        '분야': lambda x: ', '.join([i for i in x.unique() if i]),
                        '부문': lambda x: ', '.join([i for i in x.unique() if i])
                    })
                    
                    status_1.text("💡 최종 1:1 결합 중...")
                    
                    df_step1 = pd.merge(df_api, df_local_agg, on=['지자체명', '세부사업명'], how='inner')
                    df_step1.to_csv(f"[자동저장]_1단계_목록_{target_year}.csv", index=False, encoding='utf-8-sig')
                    
                    status_1.text("✅ 수집 및 매핑 완료!")
                    st.success(f"🎉 '일반공공행정' 제외 완벽 매핑! 총 {len(df_step1)}건 추출 완료.")
                    st.dataframe(df_step1[['지자체명', '세부사업명', '회계', '분야', '부문']].head(10)) 
                    
                    csv_step1 = df_step1.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    region_tag = "전체" if len(selected_sido) > 3 else "_".join(selected_sido)
                    st.download_button(
                        label="📥 1단계 최종 결과 다운로드 (CSV)",
                        data=csv_step1,
                        file_name=f"target_list_mapped_{region_tag}_{target_year}.csv",
                        mime="text/csv"
                    )

# ---------------------------------------------------------
# [2단계] 사업개요 텍스트 추출 로직 (최적화 lxml 버전)
# ---------------------------------------------------------
def extract_clean_text(html_text, target_keyword):
    only_body = SoupStrainer('body')
    soup = BeautifulSoup(html_text, 'lxml', parse_only=only_body)
    raw_text = soup.get_text(separator='|', strip=True)
    parts = raw_text.split('|')
    
    stop_words = ['사업목적', '사업기간', '총사업비', '사업규모', '사업내용', '지원형태', '지원조건', '사업위치', '시행주체', '추진근거', '추진경위', '추진계획', '소요재원', '월별배정액', '과거집행현황', '지출현황']
    extracted, found = [], False
    
    for piece in parts:
        piece = piece.strip()
        if not piece: continue
        is_stop_word = any(piece.replace(" ", "").startswith(sw) for sw in stop_words)
        if not found:
            if target_keyword.replace(" ", "") in piece.replace(" ", ""):
                found = True
                content = piece.split(target_keyword)[-1].replace(":", "").replace("：", "").strip()
                if content: extracted.append(content)
        else:
            if is_stop_word: break
            if piece not in [':', '：', '-', '+']: extracted.append(piece)
    return " ".join(extracted).strip()

def fetch_text_data(row):
    year = str(row.get('회계연도', row.get('fyr')))
    laf_cd = str(row.get('지자체코드', row.get('lafCd', row.get('laf_cd'))))
    dbiz_cd = str(row.get('세부사업코드', row.get('dbizCd', row.get('dbiz_cd'))))
    
    url = "https://www.lofin365.go.kr/lf/lnncGramStst/laf/exeSvi/retvDtlsBybsnAneSituDts.do"
    payload = {
        'menuUrl': '/lf/lnncGramStst/laf/exeSvi/retvDtlsBybsnAneSituDts.do',
        'menuNm': '세부사업별 세출현황 상세', 'menuParaCn': 'STST', 'menuId': 'LF3120204',
        'uprMenuId': 'LF3120202', 'sysDvCd': '', 'logReg': 'true',
        'dbizCd': dbiz_cd, 'lafCd': laf_cd, 'fyr': year, 'inqYmd': f"{year}1231"
    }
    
    try:
        response = session_step2.post(url, data=payload, timeout=20) 
        return {
            '회계연도': year, '지자체코드': laf_cd, '세부사업코드': dbiz_cd,
            '사업목적': extract_clean_text(response.text, '사업목적'),
            '사업기간': extract_clean_text(response.text, '사업기간'),
            '사업내용': extract_clean_text(response.text, '사업내용'),
            '추진계획': extract_clean_text(response.text, '추진계획')
        }
    except Exception:
        return None

with tab2:
    st.header("2. 사업개요 텍스트 추출 (스마트 이어하기 지원)")
    st.info("💡 멈췄을 경우, 폴더에 있는 `[자동저장]_2단계...csv` 파일을 여기에 올리면 남은 것만 이어서 수집합니다.")
    
    uploaded_file = st.file_uploader("📂 1단계 결과 파일 또는 [자동저장] 백업 파일 업로드", type=['csv'])
    
    if uploaded_file is not None:
        df_uploaded = load_safe_df(uploaded_file)
        st.write(f"총 **{len(df_uploaded)}**건의 데이터를 확인했습니다.")
        
        # 💡 [스마트 이어하기 감지 로직]
        if '사업내용' not in df_uploaded.columns:
            # 처음 시작하는 경우 빈 컬럼 생성
            df_uploaded['사업목적'] = None
            df_uploaded['사업기간'] = None
            df_uploaded['사업내용'] = None
            df_uploaded['추진계획'] = None
            
        done_count = df_uploaded['사업내용'].notna().sum()
        todo_count = len(df_uploaded) - done_count
        
        if done_count > 0:
            st.success(f"💾 이어하기 모드 감지됨! (이미 완료: {done_count}건 / 남은 작업: {todo_count}건)")
        
        if st.button("🚀 2단계 텍스트 병렬 추출 시작"):
            target_records = df_uploaded.to_dict('records')
            
            # 💡 수집해야 할 행(비어있는 행)만 필터링하고 인덱스 추적
            records_to_fetch = [
                (idx, row) for idx, row in enumerate(target_records) 
                if pd.isna(row.get('사업내용')) or str(row.get('사업내용')).strip() in ["", "None"]
            ]
            
            if not records_to_fetch:
                st.success("🎉 남은 작업이 없습니다! 모두 수집 완료되었습니다.")
                st.stop()
                
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # 앞서 말씀드린 대로 속도를 위해 20~30으로 조정하여 사용하셔도 됩니다.
            with ThreadPoolExecutor(max_workers=20) as executor:
                # 미래 객체에 인덱스(idx)를 매핑하여 원본 데이터의 제자리에 꽂아 넣기
                future_to_idx = {executor.submit(fetch_text_data, row): idx for idx, row in records_to_fetch}
                
                completed_in_this_run = 0
                total_to_fetch = len(records_to_fetch)
                
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    result = future.result()
                    
                    if result:
                        # 원본 레코드의 빈칸에 수집된 텍스트 즉시 덮어쓰기 (Merge 중복 오류 원천 차단)
                        target_records[idx]['사업목적'] = result['사업목적']
                        target_records[idx]['사업기간'] = result['사업기간']
                        target_records[idx]['사업내용'] = result['사업내용']
                        target_records[idx]['추진계획'] = result['추진계획']
                        
                    completed_in_this_run += 1
                                        
                    # 💡 1. [화면 업데이트]: 1건 완료될 때마다 실시간으로 진행률 갱신 (속도 저하 없음)
                    progress = int((completed_in_this_run / total_to_fetch) * 100)
                    progress_bar.progress(progress)
                    status_text.text(f"🚀 데이터 추출 중... ({completed_in_this_run} / {total_to_fetch} 건 완료)")
                    
                    # 💡 2. [물리적 백업 저장]: 디스크 I/O 병목(속도 저하)을 방지하기 위해 1000건마다 1번씩만 몰아서 파일 저장
                    if completed_in_this_run % 1000 == 0 or completed_in_this_run == total_to_fetch:
                        local_path = "[자동저장]_2단계_텍스트추출_백업.csv"
                        
                        # 1. 로컬 하드디스크에 먼저 저장 (안전 장치)
                        pd.DataFrame(target_records).to_csv(local_path, index=False, encoding='utf-8-sig')
                        status_text.text(f"🚀 {completed_in_this_run}/{total_to_fetch} 완료 - 💾 로컬 백업 완료!")

                        # 2. Google Drive 실시간 업로드 (Secrets 인증 방식)
                        try:
                            # ⚠️ 상단에 정의한 'authenticate_gdrive_with_secrets' 함수를 사용합니다.
                            drive = authenticate_gdrive_with_secrets() 
                            
                            # ⚠️ '박사님의_폴더_ID' 부분을 실제 구글 드라이브 폴더 ID로 꼭 수정하세요!
                            upload_to_gdrive(drive, local_path, "3NekjB0SM39VhTsw74lcTMGyPREOEDEU")
                            
                            status_text.text(f"🚀 {completed_in_this_run}/{total_to_fetch} 완료 - ☁️ Google Drive 업로드 성공!")
                        except Exception as e:
                            # 업로드 실패 시에도 로컬 저장은 완료된 상태이므로 경고만 표시
                            st.warning(f"⚠️ 구글 드라이브 업로드 중 오류 발생 (로컬 저장은 안전함): {e}")                        

                    time.sleep(0.01)
            
            status_text.text("✅ 추출 완료!")
            st.success("🎉 모든 텍스트 추출이 완벽하게 끝났습니다!")
            
            df_final = pd.DataFrame(target_records)
            st.dataframe(df_final[['지자체명', '세부사업명', '사업내용']].head(5))
            
            csv_final = df_final.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                label="📥 2단계 최종 통합 데이터 다운로드 (CSV)",
                data=csv_final,
                file_name="budget_text_final_result.csv",
                mime="text/csv"
            )
