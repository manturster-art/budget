[readme.md](https://github.com/user-attachments/files/25578295/readme.md)
# 🏛️ 지방재정365 세부사업 데이터 수집기 (초고속/매핑 통합본)

> **지방재정365(lofin365.go.kr)**의 내부 API와 **로컬 예산현황 데이터**를 1:1로 결합(Mapping)하여, 정책 분석에 필요한 핵심 세부사업의 개요 텍스트만 초고속으로 수집하는 파이썬 기반 하이브리드 데이터 추출 도구입니다.

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat-square&logo=Python&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=Streamlit&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat-square&logo=pandas&logoColor=white)

## 📖 프로젝트 개요
본 프로젝트는 수십만 건에 달하는 지자체 예산 데이터 중 **분석 가치가 높은 정책 사업만을 선별하여 대용량 텍스트를 안정적으로 수집**하기 위해 기획되었습니다. 수집된 데이터는 지자체 조례안 심사, 예산안 분석, 그리고 **기후예산 자동 분류(태깅) AI 모델 학습용 고품질 데이터셋** 구축 등에 활용할 수 있습니다.

---

## ✨ 핵심 최적화 및 주요 기능

1. **로컬 데이터 1:1 완벽 매핑 및 노이즈 필터링**
   * API 단독 수집 시 누락되는 '회계/분야/부문' 정보를 사용자의 로컬 엑셀/CSV 파일과 결합하여 복원합니다.
   * 연산 과정에서 단순 운영 경비인 **'일반공공행정', '일반행정', '기타', '예비비'를 완벽하게 자동 삭제**하여 수집 대상(모수)을 획기적으로 압축합니다.

2. **초고속 파싱 및 네트워크 병목 해소**
   * **`requests.Session` 기반 연결 재사용**: TCP/TLS Handshake 지연 시간을 제거하여 통신 속도를 극대화했습니다.
   * **`lxml` 파서 및 부분 파싱(`SoupStrainer`)**: C언어 기반 엔진을 도입하여 HTML 텍스트 추출 속도를 10배 이상 향상시키고 메모리 누수를 방지합니다.

3. **스마트 이어하기 (Resume) 및 실시간 백업**
   * 대용량 수집 중 네트워크 오류나 IP 차단으로 프로그램이 종료되어도, 생성된 `[자동저장]` 파일을 업로드하면 **이미 수집된 데이터는 건너뛰고 남은 데이터만 정확히 이어서 수집**합니다.

4. **강력한 봇(Bot) 차단 우회 및 만능 파일 리더**
   * `curl_cffi`의 `impersonate="chrome"` 옵션을 사용하여 공공기관 웹 방화벽(WAF)의 차단을 방지합니다.
   * CSV의 다양한 인코딩(`utf-8`, `cp949`, `euc-kr`)과 헤더 위치를 자동 감지하여 에러 없이 파일을 로드합니다.

---

## ⚙️ 시작하기 (Installation)

파이썬 3.8 이상이 필요합니다. 원활한 고속 파싱을 위해 `lxml` 설치가 필수적입니다.

```bash
# 필수 라이브러리 설치
pip install streamlit pandas beautifulsoup4 curl_cffi openpyxl lxml
🚀 사용 방법
본 프로그램은 터미널 환경에서 Streamlit 웹 서버를 구동하여 사용합니다.

Bash
# 프로그램 실행
streamlit run app.py
(주의: app.py 부분은 실제 저장하신 파이썬 파일명으로 변경하여 실행하세요.)

[1단계] 사업목록 추출 및 로컬 데이터 병합
웹 브라우저가 열리면 API 인증키를 입력합니다.

분석 대상 **지역코드 파일(CSV/Excel)**과 **로컬 예산현황 파일(CSV/Excel)**을 업로드합니다.

대상 연도와 광역 단위(시/도)를 선택 후 실행합니다.

'일반행정' 등이 제거된 알짜배기 매핑 결과물(target_list_mapped_...csv)을 다운로드합니다.

[2단계] 사업개요 텍스트 추출 (스마트 이어하기)
1단계에서 다운로드한 결과물(또는 이전에 중단된 [자동저장] 백업 파일)을 2단계 탭에 업로드합니다.

추출을 시작하면 병렬 스레드(기본 10~20개)가 작동하여 텍스트를 수집합니다.

완료 후 최종 통합된 budget_text_final_result.csv 파일을 다운로드합니다.

📂 프로젝트 구조
Plaintext
.
├── app.py                         # 하이브리드 크롤러 메인 소스코드
├── [자동저장]_1단계_목록_2024.csv    # 1단계 실시간 백업 파일
├── [자동저장]_2단계_텍스트추출.csv   # 2단계 실시간 백업 (이어하기용)
└── README.md                      # 프로젝트 문서
📝 라이선스 및 참고자료
데이터 출처: 지방재정365 (공공데이터 활용 가이드라인 준수 필수)

주요 기술 참조:

curl_cffi (TLS 우회 및 Session 관리)

Beautiful Soup (lxml 최적화)

이 프로젝트는 공익적 목적의 지자체 정책 분석 및 연구 고도화를 위해 제작되었습니다.
