import whisper
import os
import re

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
            initial_prompt="SQL, SELECT, FROM, JOIN, WHERE, INSERT, UPDATE, database, query, UX, wireframe"
        )
        return result["text"]
    except FileNotFoundError:
        return f"오디오 파일을 찾을 수 없습니다. (경로: {audio_file})"
    except Exception as e:
        return f"에러 발생: {str(e)}"

def postprocess_text(text):
    """
    텍스트에서 영어 IT 용어 오타 보정 및 대문자화.
    Args:
        text (str): Whisper 출력 텍스트
    Returns:
        str: 보정된 텍스트
    """
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

def save_text(text, output_file="transcription.txt"):
    """
    변환된 텍스트를 .txt 파일로 저장.
    Args:
        text (str): 저장할 텍스트
        output_file (str): 출력 파일 경로
    """
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(text)