import pyaudio
import wave
import whisper
import os

def record_audio(output_file, duration=10):
    """
    지정된 시간(기본 10초) 동안 음성을 녹음해 .wav 파일로 저장.
    Args:
        output_file (str): 저장할 .wav 파일 경로
        duration (int): 녹음 시간(초)
    """
    CHUNK = 1024
    FORMAT = pyaudio.paInt16
    CHANNELS = 1
    RATE = 16000

    p = pyaudio.PyAudio()
    stream = p.open(format=FORMAT, channels=CHANNELS, rate=RATE, input=True, frames_per_buffer=CHUNK)
    frames = []

    print(f"Recording for {duration} seconds...")
    for _ in range(0, int(RATE / CHUNK * duration)):
        data = stream.read(CHUNK, exception_on_overflow=False)
        frames.append(data)
    print("Recording done.")

    stream.stop_stream()
    stream.close()
    p.terminate()

    wf = wave.open(output_file, 'wb')
    wf.setnchannels(CHANNELS)
    wf.setsampwidth(p.get_sample_size(FORMAT))
    wf.setframerate(RATE)
    wf.writeframes(b''.join(frames))
    wf.close()
    print(f"Audio saved to {output_file}")

def transcribe_audio(audio_file, model_name="tiny"):
    """
    Whisper로 음성 파일을 텍스트로 변환.
    Args:
        audio_file (str): .wav 파일 경로
        model_name (str): Whisper 모델 (tiny/base)
    Returns:
        str: 변환된 텍스트
    """
    try:
        # 상대 경로를 절대 경로로 변환
        audio_file = os.path.abspath(audio_file)
        print(f"변환할 오디오 파일 경로: {audio_file}")
        
        # 파일 존재 확인
        if not os.path.exists(audio_file):
            return f"오디오 파일을 찾을 수 없습니다. (경로: {audio_file})"
            
        # 파일 크기 확인
        file_size = os.path.getsize(audio_file)
        print(f"오디오 파일 크기: {file_size} bytes")
        
        # Whisper 모델 로드
        model = whisper.load_model(model_name)
        
        # 파일 경로를 문자열로 변환
        audio_file_str = str(audio_file)
        print(f"Whisper에 전달할 파일 경로: {audio_file_str}")
        
        result = model.transcribe(audio_file_str, language=None)
        return result["text"]
    except FileNotFoundError:
        return f"오디오 파일을 찾을 수 없습니다. (경로: {audio_file})"
    except Exception as e:
        return f"에러 발생: {str(e)}"

def save_text(text, output_file="transcription.txt"):
    """
    변환된 텍스트를 .txt 파일로 저장.
    Args:
        text (str): 저장할 텍스트
        output_file (str): 출력 파일 경로
    """
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(text)

if __name__ == "__main__":
    # 설정
    audio_file = "data/test_10s.wav"
    output_file = "data/transcription.txt"
    duration = 10  # 10초

    # 디렉토리 확인
    os.makedirs("data", exist_ok=True)

    # 10초 음성 녹음
    # record_audio(audio_file, duration)

    # 음성 → 텍스트 변환
    text = transcribe_audio(audio_file, model_name="base")
    print("변환된 텍스트:", text)

    # 텍스트 저장
    save_text(text, output_file)
    print(f"텍스트가 {output_file}에 저장되었습니다.")