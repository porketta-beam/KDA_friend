import pandas as pd
from sqlalchemy import create_engine

def fetch_financial_ratios(ticker):
    # PostgreSQL 연결 정보
    user = "postgres"
    password = "0307"
    host = "localhost"
    port = "5432"
    database = "value"

    try:
        # SQLAlchemy 엔진 생성
        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

        # ticker에서 '.KS' 제거 (예: '005930.KS' -> '005930')
        ticker = ticker.replace('.KS', '')

        # SQL 쿼리 작성
        query = f"""
        SELECT * FROM "2024_ratio"
        WHERE ticker = '{ticker}';
        """

        # 쿼리 실행하여 데이터프레임으로 가져오기
        df = pd.read_sql(query, engine)

        if df.empty:
            return None, f"종목 '{ticker}'에 대한 재무비율 데이터가 없습니다."
        
        return df, None

    except Exception as error:
        return None, f"데이터 조회 중 오류 발생: {error}"