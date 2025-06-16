import FinanceDataReader as fdr
import pandas as pd

# 상장폐지 종목 목록 가져오기
delisted = fdr.StockListing('KRX-DELISTING')

# 데이터프레임 열 이름 확인
print("상장폐지 데이터 열 이름:")
print(delisted.columns)

# 종목코드 열 이름 설정
code_column = 'Code' if 'Code' in delisted.columns else 'Symbol'

# 종목코드 필터링: 6자리 숫자이고 마지막 자리가 0
delisted = delisted[
    # 6자리 숫자 확인
    delisted[code_column].str.match(r'^\d{6}$') &
    # 마지막 자리가 0
    delisted[code_column].str.endswith('0') &
    # 2005년 이후 상장폐지
    (delisted['DelistingDate'] >= '2005-01-01')
]

# 기업 자체 상장폐지 사유 필터링
# 제외할 사유: 합병, 스팩 합병, 자진상장폐지
# 제외할 종목: 우선주, 스팩
# include_reasons = ['경영악화', '재무구조악화', '감사의견', '사업보고서']
exclude_reasons = ['피흡수합병', '완전자회사화', '완전자회사로 편입', '존속기간만료', '존속기간 만료', '해산 사유 발생']
exclude_preferred = ['우선주', '스팩']

# 필터링 조건
filtered_delisted = delisted[
    # 합병, 자회사화, 스팩 제외
    ~delisted['Reason'].str.contains('|'.join(exclude_reasons), case=False, na=False) &
    # 우선주, 스팩 제외
    ~delisted['Name'].str.contains('|'.join(exclude_preferred), case=False, na=False)
]

# 상장폐지 날짜로 정렬 (최신순)
filtered_delisted['DelistingDate'] = pd.to_datetime(filtered_delisted['DelistingDate'], errors='coerce')
sorted_delisted = filtered_delisted.sort_values(by='DelistingDate', ascending=False)

# 필요한 열만 선택
columns_to_show = [code_column, 'Name', 'Market', 'DelistingDate', 'Reason']
if all(col in filtered_delisted.columns for col in columns_to_show):
    sorted_delisted = sorted_delisted[columns_to_show]
else:
    print("일부 열이 누락되었습니다. 사용 가능한 열:", filtered_delisted.columns)

# 상위 10개 출력
print("\n최신 상장폐지 종목 목록 (상위 10개):")
print(sorted_delisted.head(10))

# CSV 파일로 저장
sorted_delisted.to_csv('src_data/delisted_stocks.csv', index=False, encoding='utf-8-sig')
print("\n최신순으로 정렬된 상장폐지 종목 목록이 'delisted_stocks.csv' 파일로 저장되었습니다.")
print(len(filtered_delisted))