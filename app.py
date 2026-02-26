import streamlit as st
import pandas as pd
from curl_cffi import requests
from bs4 import BeautifulSoup
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------
# 기본 웹페이지 설정
# ---------------------------------------------------------
st.set_page_config(page_title="지방재정365 크롤러", page_icon="🏛️", layout="wide")
st.title("🏛️ 지방재정365 세부사업 데이터 수집기")
st.markdown("""
이 웹 프로그램은 지방재정365 내부 API를 활용하여 원하는 지역의 세부사업 목록과 사업개요 텍스트를 **초고속 병렬 처리**로 수집합니다.
""")

tab1, tab2 = st.tabs(["[1단계] 사업목록 및 결산액 추출", "[2단계] 사업개요 텍스트 추출"])

# ---------------------------------------------------------
# [1단계 헬퍼 함수] 특정 지자체 데이터를 끝까지 수집하는 함수
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
            response = requests.get(api_url, params=payload, impersonate="chrome", timeout=15)
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
            time.sleep(0.1) 
            
        except Exception as e:
            break
            
    return region_data, laf_nm

# ---------------------------------------------------------
# [1단계] 사업목록 추출 UI
# ---------------------------------------------------------
with tab1:
    st.header("1. 타겟 사업목록 지역별 추출 (병렬)")
    st.info("지역코드 파일을 업로드하고 원하는 '광역 단위(시/도)'를 선택하여 초고속으로 수집합니다.")
    
    col1, col2 = st.columns(2)
    with col1:
        api_key = st.text_input("API 인증키 (Decoding Key)", type="password")
    with col2:
        years_list = [str(y) for y in range(2016, 2026)]
        target_year = st.selectbox("조회할 회계연도", years_list, index=len(years_list)-3)
        
    region_file = st.file_uploader("🗺️ 지역코드 파일 업로드 (예: code_2024.csv)", type=['csv', 'xlsx'])
    
    # 🌟 지역 선택 변수 초기화
    selected_sido = []
    df_region = pd.DataFrame()
    
    # 파일이 업로드되면 즉시 읽어서 광역단위 목록을 표시합니다!
    if region_file is not None:
        try:
            if region_file.name.endswith('.csv'):
                df_region = pd.read_csv(region_file, header=1)
            else:
                df_region = pd.read_excel(region_file, header=1)
                
            if '지역' in df_region.columns:
                unique_sido = df_region['지역'].dropna().unique().tolist()
                # 사용자가 원하는 지역만 다중 선택할 수 있도록 UI 제공 (기본값: 전체 선택)
                selected_sido = st.multiselect("📍 수집할 광역 단위 선택 (여러 개 선택 가능)", unique_sido, default=unique_sido)
            else:
                st.warning("업로드된 파일에 '지역' 컬럼이 없어 전체 지자체를 대상으로 합니다.")
        except Exception as e:
            st.error(f"파일을 읽는 중 오류가 발생했습니다: {e}")

    if st.button("🚀 1단계 선택 지역 추출 시작", key="btn_step1"):
        if not api_key:
            st.error("API 인증키를 입력해주세요!")
        elif region_file is None:
            st.error("지역코드 파일을 업로드해주세요!")
        elif '자치단체코드' not in df_region.columns:
            st.error("업로드한 파일에 '자치단체코드' 컬럼이 없습니다.")
        elif '지역' in df_region.columns and not selected_sido:
            st.error("수집할 광역 단위를 최소 1개 이상 선택해주세요!")
        else:
            with st.spinner("선택하신 지역의 병렬 추출을 준비합니다..."):
                # 선택한 광역 단위만 필터링!
                if selected_sido:
                    df_region_filtered = df_region[df_region['지역'].isin(selected_sido)]
                else:
                    df_region_filtered = df_region
                    
                unique_regions = df_region_filtered[['자치단체코드', '자치단체명']].drop_duplicates().to_dict('records')
                st.success(f"총 {len(unique_regions)}개의 지자체 추출을 시작합니다! (병렬 엔진 가동)")
                
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
                        status_1.text(f"병렬 수집 중... [{completed_count}/{len(unique_regions)}] '{region_name}' 수집 완료")
                        time.sleep(0.05)
                
                if target_list:
                    df_step1 = pd.DataFrame(target_list).drop_duplicates(subset=['회계연도', '지자체코드', '세부사업코드'])
                    status_1.text("✅ 선택 지역 병렬 데이터 수집 완료!")
                    st.success(f"🎉 총 {len(df_step1)}건의 사업 목록을 초고속으로 추출했습니다!")
                    st.dataframe(df_step1.head(10)) 
                    
                    csv_step1 = df_step1.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                    # 저장할 파일 이름에 선택한 지역을 표시해 줍니다.
                    region_tag = "전체" if len(selected_sido) > 3 else "_".join(selected_sido)
                    st.download_button(
                        label="📥 1단계 결과 다운로드 (CSV)",
                        data=csv_step1,
                        file_name=f"target_list_{region_tag}_{target_year}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("조건에 맞는 데이터가 없습니다.")

# ---------------------------------------------------------
# [2단계 헬퍼 함수] 사업개요 텍스트 추출
# ---------------------------------------------------------
def extract_clean_text(html_text, target_keyword):
    soup = BeautifulSoup(html_text, 'html.parser')
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
        response = requests.post(url, data=payload, impersonate="chrome", timeout=15)
        return {
            '회계연도': year, '지자체코드': laf_cd, '세부사업코드': dbiz_cd,
            '사업목적': extract_clean_text(response.text, '사업목적'),
            '사업기간': extract_clean_text(response.text, '사업기간'),
            '사업내용': extract_clean_text(response.text, '사업내용'),
            '추진계획': extract_clean_text(response.text, '추진계획')
        }
    except Exception:
        return None

# ---------------------------------------------------------
# [2단계] 사업개요 텍스트 추출 UI
# ---------------------------------------------------------
with tab2:
    st.header("2. 사업개요 텍스트 병렬 추출")
    st.info("1단계에서 뽑은 '사업목록(CSV)'을 업로드하면 텍스트 데이터를 초고속으로 긁어옵니다.")
    
    uploaded_file = st.file_uploader("📂 1단계 결과 파일(CSV) 업로드", type=['csv'])
    
    if uploaded_file is not None:
        df_uploaded = pd.read_csv(uploaded_file)
        st.write(f"총 **{len(df_uploaded)}**건의 데이터를 확인했습니다.")
        
        if st.button("🚀 2단계 텍스트 병렬 추출 시작"):
            target_records = df_uploaded.to_dict('records')
            extracted_texts = []
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(fetch_text_data, row) for row in target_records]
                
                for i, future in enumerate(as_completed(futures), 1):
                    result = future.result()
                    if result:
                        extracted_texts.append(result)
                    
                    progress = int((i / len(target_records)) * 100)
                    progress_bar.progress(progress)
                    status_text.text(f"추출 진행 중... ({i} / {len(target_records)} 건 완료)")
                    time.sleep(0.02)
            
            if extracted_texts:
                df_result = pd.DataFrame(extracted_texts)
                for col in ['회계연도', '지자체코드', '세부사업코드']:
                    if col in df_uploaded.columns and col in df_result.columns:
                        df_uploaded[col] = df_uploaded[col].astype(str)
                        df_result[col] = df_result[col].astype(str)
                        
                df_final = pd.merge(df_uploaded, df_result, on=['회계연도', '지자체코드', '세부사업코드'], how='left')
                
                status_text.text("✅ 추출 완료!")
                st.success("🎉 모든 텍스트 추출 및 병합이 완료되었습니다!")
                st.dataframe(df_final.head(5))
                
                csv_final = df_final.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(
                    label="📥 최종 통합 데이터 다운로드 (CSV)",
                    data=csv_final,
                    file_name="budget_text_final_custom_parallel.csv",
                    mime="text/csv"
                )
