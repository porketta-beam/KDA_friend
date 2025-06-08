from PIL import Image
import camelot
import tabula
import json
import os
import pandas as pd

base_dir = os.path.dirname(os.path.abspath(__file__))
png_path = os.path.join(base_dir, 'image.png')
pdf_path = os.path.join(base_dir, 'kakao.pdf')

# # (1-1) 이미지 로드 및 RGB 변환
# img = Image.open(png_path).convert("RGB")
# # (1-2) PDF로 저장
# img.save(pdf_path, "PDF", resolution=100.0)





# 2) Camelot으로 테이블 읽기 (이미 1개가 감지된 상태)
tables = camelot.read_pdf(pdf_path, pages="4", flavor="stream")
print(f"Camelot detected {len(tables)} tables.")

# 3) 추출된 테이블을 JSON으로 직렬화
all_tables = []
for idx, table in enumerate(tables):
    df = table.df  # pandas DataFrame 형태
    # orient='records' → 한 행을 하나의 JSON 객체로 만듦
    json_str = df.to_json(orient="records", force_ascii=False)
    # 필요하다면 파이썬 객체로 다시 변환
    table_json = json.loads(json_str)
    all_tables.append({
        "table_index": idx,
        "rows": table_json
    })

# 4) 최종 결과 묶기
result = {
    "source_pdf": pdf_path,
    "num_tables": len(tables),
    "tables": all_tables
}

# 5) JSON 출력
data = json.dumps(result, ensure_ascii=False, indent=2)

# print(data)

# 2) JSON 파싱
data = json.loads(data)

# 3) 첫 번째 테이블의 "rows" 리스트 추출
rows = data["tables"][0]["rows"]

# 4) pandas DataFrame으로 변환
df = pd.DataFrame(rows)

# 5) DataFrame 출력
print(df)

# # ===============================================
# # 2-A) Camelot으로 PDF 내부 표 처리
# try:
#     tables_camelot = camelot.read_pdf(pdf_path, pages="1", flavor="stream")
#     camelot_json = []
#     for tbl in tables_camelot:
#         df = tbl.df
#         camelot_json.append(json.loads(df.to_json(orient="records", force_ascii=False)))
#     print(f"Camelot detected {len(tables_camelot)} tables.")
# except Exception as e:
#     print("Camelot 오류:", e)
#     tables_camelot = []
#     camelot_json = []

# # ===============================================
# # 2-B) Tabula-py로 PDF 내부 표 처리
# try:
#     dfs_tabula = tabula.read_pdf(pdf_path, pages="1", multiple_tables=True)
#     tabula_json = []
#     for df in dfs_tabula:
#         df.columns = df.columns.map(str)
#         tabula_json.append(json.loads(df.to_json(orient="records", force_ascii=False)))
#     print(f"Tabula-py detected {len(dfs_tabula)} tables.")
# except Exception as e:
#     print("Tabula-py 오류:", e)
#     dfs_tabula = []
#     tabula_json = []

# # ===============================================
# # 3) 최종 합쳐진 JSON 결과
# final_result = {
#     "source_image": os.path.basename(png_path),
#     "intermediate_pdf": os.path.basename(pdf_path),
#     "camelot": {
#         "num_tables": len(tables_camelot),
#         "tables": camelot_json
#     },
#     "tabula": {
#         "num_tables": len(dfs_tabula),
#         "tables": tabula_json
#     }
# }

# print(json.dumps(final_result, indent=2, ensure_ascii=False))
