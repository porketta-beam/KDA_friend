import whisper
import os

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
        
        # 한국어와 영어를 모두 인식하도록 설정
        result = model.transcribe(
            audio_file_str,
            language=None,  # 자동 언어 감지
            task="transcribe",
            initial_prompt="이 오디오는 한국어로 말하고 있지만 영어 단어가 섞여있을 수 있습니다."
        )
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