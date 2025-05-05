import streamlit as st
import os
from audio_recorder import record_audio
from audio_transcriber import transcribe_audio, postprocess_text, save_text
from realtime_audio import stream_audio_once

# 데이터 디렉토리 확인
os.makedirs("data", exist_ok=True)
audio_file = "data/test_10s.wav"
realtime_audio_file = "data/realtime_5s.wav"
output_file = "data/transcription.txt"

# 세션 상태 초기화
if "text" not in st.session_state:
    st.session_state.text = ""
if "corrected_text" not in st.session_state:
    st.session_state.corrected_text = ""
if "realtime_text" not in st.session_state:
    st.session_state.realtime_text = []
if "audio_recorded" not in st.session_state:
    st.session_state.audio_recorded = False
if "realtime_recording" not in st.session_state:
    st.session_state.realtime_recording = False
if "realtime_error" not in st.session_state:
    st.session_state.realtime_error = ""

# Streamlit 페이지 설정
st.set_page_config(page_title="Lecture Transcription App", page_icon="🎙️")
st.title("강의 음성-텍스트 변환기")
st.write("한국어와 영어 IT 용어(SQL, UX 등)를 포함한 강의 음성을 텍스트로 변환합니다.")

# 탭 구성
tab1, tab2 = st.tabs(["오프라인 변환", "실시간 변환"])

# 탭 1: 오프라인 변환
with tab1:
    st.subheader("1. 음성 녹음")
    duration = st.slider("녹음 시간(초)", min_value=5, max_value=30, value=10, key="offline_duration")
    if st.button("음성 녹음 시작", key="offline_record"):
        with st.spinner(f"{duration}초 녹음 중..."):
            record_audio(audio_file, duration=duration)
            st.session_state.audio_recorded = True
            st.success(f"녹음 완료! 파일: {audio_file}")

    if st.session_state.audio_recorded:
        st.subheader("2. 텍스트 변환")
        model_name = st.selectbox("Whisper 모델 선택", ["tiny", "base", "small"], index=0, key="offline_model")
        if st.button("음성을 텍스트로 변환", key="offline_transcribe"):
            with st.spinner("텍스트 변환 중..."):
                text = transcribe_audio(audio_file, model_name=model_name)
                st.session_state.text = text
                corrected_text = postprocess_text(text)
                st.session_state.corrected_text = corrected_text
                save_text(corrected_text, output_file)
                st.success("변환 완료!")

    if st.session_state.text:
        st.subheader("3. 변환 결과")
        col1, col2 = st.columns(2)
        with col1:
            st.write("**원본 텍스트**:")
            st.write(st.session_state.text)
        with col2:
            st.write("**보정된 텍스트**:")
            st.write(st.session_state.corrected_text)

        with open(output_file, "rb") as f:
            st.download_button(
                label="텍스트 파일 다운로드",
                data=f,
                file_name="transcription.txt",
                mime="text/plain",
                key="offline_download"
            )

# 탭 2: 실시간 변환
with tab2:
    st.subheader("실시간 음성-텍스트 변환")
    st.write("마이크로 말하면 5초마다 텍스트로 변환됩니다. '녹음 시작/중지' 버튼으로 제어하세요.")
    model_name = st.selectbox("Whisper 모델 선택", ["tiny", "base"], index=0, key="realtime_model")
    start_stop_button = st.button("실시간 녹음 시작/중지")

    def update_realtime_text(text):
        if text and "오디오 파일을 찾을 수 없습니다" not in text and "에러" not in text:
            st.session_state.realtime_text.append(text)
        else:
            st.session_state.realtime_error = text if "에러" in text else "음성 인식 실패. 다시 시도하세요."

    if start_stop_button:
        st.session_state.realtime_recording = not st.session_state.realtime_recording
        if st.session_state.realtime_recording:
            st.session_state.realtime_text = []
            st.session_state.realtime_error = ""
            st.success("실시간 녹음 시작! 5초마다 텍스트가 업데이트됩니다.")

    if st.session_state.realtime_recording:
        stream_audio_once(
            realtime_audio_file,
            transcribe_audio,
            postprocess_text,
            update_realtime_text,
            buffer_duration=5,
            model_name=model_name
        )
        if st.session_state.realtime_error:
            st.error(st.session_state.realtime_error)
        st.rerun()

    if st.session_state.realtime_text:
        st.subheader("실시간 변환 결과")
        for i, text in enumerate(st.session_state.realtime_text):
            st.write(f"**세그먼트 {i+1}**: {text}")
        
        cumulative_text = "\n".join(st.session_state.realtime_text)
        save_text(cumulative_text, output_file)
        with open(output_file, "rb") as f:
            st.download_button(
                label="누적 텍스트 다운로드",
                data=f,
                file_name="realtime_transcription.txt",
                mime="text/plain",
                key="realtime_download"
            )

# 사이드바 설정
with st.sidebar:
    st.header("설정 및 정보")
    st.write(f"**오프라인 모델**: {model_name if 'model_name' in locals() else '미선택'}")
    st.write(f"**녹음 시간 (오프라인)**: {duration}초")
    st.write("**실시간 버퍼**: 5초")
    st.write("**언어**: 한국어+영어 자동 감지")
    st.write("**SQL/IT 용어 최적화**: SELECT, JOIN, database, query 등")
    st.write("**프로젝트 디렉토리**: C:\\Users\\JS\\DEV\\TEST\\practice_whisper")
    st.info("RAM 부족 시 Chrome, VS Code 종료 (tiny: ~1GB, small: ~4GB).")
    st.warning("마이크 연결 확인하세요.")