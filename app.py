import streamlit as st
import os
from audio_recorder import record_audio
from audio_transcriber import transcribe_audio, save_text
import re

# 영어 용어 보정 함수 (audio_transcriber.py에 없으므로 추가)
def postprocess_text(text):
    corrections = {
        "셀렉트": "SELECT",
        "프롬": "FROM",
        "조인": "JOIN",
        "웨얼": "WHERE",
        "데이타베이스": "database",
        "쿼리": "query",
        "유엑스": "UX",
        "와이어프레임": "wireframe",
        "인서트": "INSERT",
        "업데이트": "UPDATE"
    }
    for wrong, correct in corrections.items():
        text = text.replace(wrong, correct)
    
    sql_keywords = ["SELECT", "FROM", "JOIN", "WHERE", "INSERT", "UPDATE"]
    for keyword in sql_keywords:
        text = re.sub(rf'\b{keyword.lower()}\b', keyword, text, flags=re.IGNORECASE)
    
    return text

# Streamlit 페이지 설정
st.set_page_config(page_title="Lecture Transcription App", page_icon="🎙️")
st.title("강의 음성-텍스트 변환기")
st.write("한국어와 영어 IT 용어(SQL, UX 등)를 포함한 강의 음성을 텍스트로 변환합니다.")

# 데이터 디렉토리 확인
os.makedirs("data", exist_ok=True)
audio_file = "data/test_10s.wav"
output_file = "data/transcription.txt"

# 세션 상태 초기화
if "text" not in st.session_state:
    st.session_state.text = ""
if "corrected_text" not in st.session_state:
    st.session_state.corrected_text = ""
if "audio_recorded" not in st.session_state:
    st.session_state.audio_recorded = False

# 녹음 섹션
st.subheader("1. 음성 녹음")
duration = st.slider("녹음 시간(초)", min_value=5, max_value=30, value=10)
if st.button("음성 녹음 시작"):
    with st.spinner(f"{duration}초 녹음 중..."):
        record_audio(audio_file, duration=duration)
        st.session_state.audio_recorded = True
        st.success(f"녹음 완료! 파일: {audio_file}")

# 변환 섹션
if st.session_state.audio_recorded:
    st.subheader("2. 텍스트 변환")
    model_name = st.selectbox("Whisper 모델 선택", ["small", "base", "tiny"], index=0)
    if st.button("음성을 텍스트로 변환"):
        with st.spinner("텍스트 변환 중..."):
            # Whisper로 변환
            text = transcribe_audio(audio_file, model_name=model_name)
            st.session_state.text = text
            # 후처리
            corrected_text = postprocess_text(text)
            st.session_state.corrected_text = corrected_text
            # 텍스트 저장
            save_text(corrected_text, output_file)
            st.success("변환 완료!")

# 결과 표시
if st.session_state.text:
    st.subheader("3. 변환 결과")
    col1, col2 = st.columns(2)
    with col1:
        st.write("**원본 텍스트**:")
        st.write(st.session_state.text)
    with col2:
        st.write("**보정된 텍스트**:")
        st.write(st.session_state.corrected_text)

    # 다운로드 버튼
    with open(output_file, "rb") as f:
        st.download_button(
            label="텍스트 파일 다운로드",
            data=f,
            file_name="transcription.txt",
            mime="text/plain"
        )

# 금융/핀테크 적용 예시
st.subheader("4. 금융/핀테크 적용 가능성")
st.markdown("""
- **SQL 강의**: "SELECT * FROM transactions WHERE amount > 1000"를 텍스트화해 거래 데이터 분석.
- **UX 강의**: "Wireframe으로 UI 최적화"를 기록해 금융 앱 인터페이스 개선.
- **실시간 잠재력**: 고객 상담 음성을 텍스트화해 CRM 시스템에 통합.
- **포트폴리오**: 금융권 면접에서 음성 처리 기술을 Streamlit으로 시연.
""")

# 사이드바: 시스템 정보 및 설정
with st.sidebar:
    st.header("설정 및 정보")
    st.write(f"**Whisper 모델**: {model_name}")
    st.write(f"**녹음 시간**: {duration}초")
    st.write("**언어**: 한국어+영어 자동 감지")
    st.write("**SQL/IT 용어 최적화**: SELECT, JOIN, database, query 등")
    st.write("**프로젝트 디렉토리**: C:\\Users\\JS\\lecture_summary")
    st.info("RAM 부족 시 Chrome, VS Code 등 백그라운드 앱을 종료하세요 (small 모델은 ~4GB 필요).")
    st.warning("마이크가 연결되어 있는지 확인하세요.")