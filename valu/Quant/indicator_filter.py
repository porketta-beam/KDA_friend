import pandas as pd
import os
from pathlib import Path

def filter_giants_pick(ticker):
    # 1. 데이터 파일 경로 설정
    base_dir = Path(__file__).parent / "src_data"
    input_path = base_dir / "indicators" / f"{ticker}_indicator.csv"
    output_path = base_dir / "giants_pick" / f"{ticker}_giants_pick.csv"
    
    # 2. CSV 파일 읽기
    try:
        df = pd.read_csv(input_path)
    except FileNotFoundError:
        print(f"Error: {input_path} 파일을 찾을 수 없습니다.")
        return
    
    # 3. 데이터가 비어 있는지 확인
    if df.empty:
        print(f"Error: {ticker} 데이터가 비어 있습니다.")
        return
    
    # 4. 모든 행에 대해 조건 판단
    # 평균 일일 거래량 >= 200,000주 및 시가총액 >= 1,000억 원
    df['Is_Giant'] = (df['Avg_Daily_Volume_252'] >= 200000) & (df['Market_Cap'] >= 100000000000)
    
    # 5. 결과 DataFrame 구성
    result_df = pd.DataFrame({
        'Ticker': [ticker] * len(df),
        'Date': df.get('Date', pd.Timestamp.now().strftime('%Y-%m-%d')),
        'Avg_Daily_Volume_252': df['Avg_Daily_Volume_252'],
        'Market_Cap': df['Market_Cap'],
        'Is_Giant': df['Is_Giant']
    })
    
    # 6. 출력 디렉토리 확인 및 생성
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # 7. 결과 저장
    try:
        result_df.to_csv(output_path, index=False)
        print(f"결과가 {output_path}에 저장되었습니다.")
    except Exception as e:
        print(f"Error: {output_path} 저장 중 오류 발생 - {e}")

# 사용 예시
if __name__ == "__main__":
    ticker = "035420.KS"  # 예시 ticker, 필요 시 다른 ticker로 변경
    filter_giants_pick(ticker)