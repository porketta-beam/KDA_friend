# 음성 인식(STT) 프로젝트 실행 가이드

## 사전 준비사항

### 1. FFmpeg 설치
* FFmpeg는 음성 파일 처리를 위해 필수적으로 설치해야 합니다.
* 설치 방법: https://kminito.tistory.com/108
* 설치 후 시스템 환경 변수 PATH에 FFmpeg bin 폴더 경로를 추가해야 합니다.
  (예: C:\ffmpeg\bin)

### 2. Python 패키지 설치
* 프로젝트에 필요한 모든 Python 패키지를 설치합니다:
```bash
pip install -r requirements.txt
```

### 3. 오디오 입력 장치 확인
* 마이크가 정상적으로 연결되어 있는지 확인합니다.
* Windows 설정에서 마이크 권한이 허용되어 있는지 확인합니다.

## 실행 방법

### 1. 웹 앱 실행 (Streamlit)
* Streamlit 기반 웹 앱을 실행하려면 아래 명령어를 사용하세요:
```bash
streamlit run app.py
```
* 웹 브라우저가 자동으로 열리며, 음성 녹음 및 변환 기능을 사용할 수 있습니다.

### 2. 기존 방식(STT.py 또는 main.py 실행)
1. `data` 폴더가 자동으로 생성됩니다.
2. `STT.py` 또는 `main.py`를 실행하면 10초 동안 음성을 녹음합니다.
3. 녹음된 음성은 자동으로 텍스트로 변환되어 `data/transcription.txt`에 저장됩니다.

## 주의사항
* FFmpeg가 설치되어 있지 않으면 음성 변환이 실패할 수 있습니다.
* 마이크가 연결되어 있지 않으면 녹음이 실패할 수 있습니다.