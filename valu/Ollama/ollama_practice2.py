import cv2
import pytesseract
from PIL import Image
import re
import json

# 1) pytesseract가 설치된 Tesseract 경로 설정 (윈도우 예시)
#    예: pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
#    macOS/Linux는 보통 PATH에 이미 걸려 있으면 주석 처리해도 무관합니다.
# pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# 2) 이미지 불러와서 전처리
img_path = "/mnt/data/fbd7b1b5-5153-4c4d-9a83-1e38e79ea25d.png"
img = cv2.imread(img_path)

# 그레이스케일 변환
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

# 이진화 (필요에 따라 임계값 조정)
_, thresh = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)

# 3) OCR로 텍스트 추출
#    config: 숫자와 영문자, 한글까지 인식하려면 whitelist를 따로 지정하거나 옵션 조정 가능
custom_config = r'--oem 3 --psm 6'
ocr_result = pytesseract.image_to_string(thresh, config=custom_config)

# 4) OCR 결과 확인 (디버깅용으로 출력해 보면 실제 인식된 문자열이 어떻게 나오는지 알 수 있습니다)
print("===== OCR RAW TEXT =====")
print(ocr_result)
print("========================")

# 5) 정규표현식으로 “분기 레이블(예: 1Q24) + 숫자” 페어 매칭
#    예시 패턴: (1Q24|2Q24|3Q24|4Q24|1Q25) → 분기
#               ([0-9]{2,3})           → 2~3자리 숫자 (예: 122, 116 등)
pattern = re.compile(r'(1Q24|2Q24|3Q24|4Q24|1Q25)\D*?([0-9]{2,3})')

matches = pattern.findall(ocr_result)

# 6) 결과를 딕셔너리로 정리
data = {}
for quarter, value_str in matches:
    try:
        data[quarter] = int(value_str)
    except ValueError:
        continue

# 7) 만약 OCR이 “1Q24 122”처럼 제대로 잡아내지 못했다면 (스크린샷에 노이즈가 많을 때),
#    pytesseract.image_to_data(...)를 써서 각 단어별(혹은 블록별) 좌표를 얻고, 
#    (x, y축 좌표 비교 → “막대 위 레이블 판별”) 같은 효율을 더할 수도 있습니다.

# 8) JSON 형태로 출력
json_output = json.dumps(data, ensure_ascii=False, indent=2)
print("===== Parsed JSON =====")
print(json_output)
