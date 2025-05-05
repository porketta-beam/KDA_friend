import os
from audio_recorder import record_audio
from audio_transcriber import transcribe_audio, save_text

사용_모델 = "small"

def main():
    # 설정
    audio_file = "data/test_10s.wav"
    output_file = "data/transcription.txt"
    duration = 10  # 10초

    # 디렉토리 확인
    os.makedirs("data", exist_ok=True)

    # 10초 음성 녹음
    # record_audio(audio_file, duration)

    # 음성 → 텍스트 변환
    text = transcribe_audio(audio_file, model_name=사용_모델)
    print("변환된 텍스트:", text)

    # 텍스트 저장
    save_text(text, output_file)
    print(f"텍스트가 {output_file}에 저장되었습니다.")

if __name__ == "__main__":
    main() 