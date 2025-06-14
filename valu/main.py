from fastapi import FastAPI
app = FastAPI()

################################################################################################
##############################          /api/          #########################################
################################################################################################

# get 요청 예제
from fastapi.responses import JSONResponse
@app.get("/api/cp_list")
def get_cp_list():
    return JSONResponse(content={'cp_list': 'json'})

# post 요청 예제
from pydantic import BaseModel
class Giant_request(BaseModel):
    cp_list : list[str]
    start_date : str
    end_date : str
    start_price : float
    end_price : float
    start_volume : int
    end_volume : int
    start_market_cap : int
    # 유저한테 받아올 정보 마음대로 넣기
    
@app.post("/api/giant_request_recommend")
def post_giant_request_recommend(request: Giant_request):
    print(request.cp_list)
    return JSONResponse(content={'cp_list': 'json'})

# path parameter 요청 예제
@app.get("/api/path_example/{item_id}")
def get_path_example(item_id: int):
    return JSONResponse(content={'item_id': item_id})


# query parameter 요청 예제
@app.get("/api/query_example")
def get_query_example(keyword: str = None, page: int = 1, limit: int = 10):
    # query parameter는 URL에 ?keyword=검색어&page=1&limit=10 형식으로 전달됨
    response = {
        "keyword": keyword,
        "page": page,
        "limit": limit,
        "message": "쿼리 파라미터 처리 예제입니다"
    }
    return JSONResponse(content=response)


################################################################################################
##############################   거장 관련 요청 라우터   #########################################
################################################################################################

from fastapi import Query
import psycopg2
from psycopg2.extras import RealDictCursor

# from valu.routers import api_pick

# # 라우터 등록
# app.include_router(api_pick.router, prefix="/api")

# DB 연결 설정
DB_INFO = {
    "dbname": "valu_test",
    "user": "postgres",
    "password": "0000",
    "host": "localhost",
    "port": "5432"
}

def get_db_connection():
    return psycopg2.connect(**DB_INFO)

@app.get("/api/pick/{date}")
def get_giant_pick(date: str, giant: str = Query(..., description="예: Graham, Fisher, Lynch")):
    giant_name = f"{giant}_Pick"
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT apd.ticker
        FROM account_processed_data apd
        JOIN date d ON apd.date_id = d.date_id
        JOIN account_list al ON apd.acc_id = al.acc_id
        WHERE d.date = %s AND al.acc_name = %s AND apd.acc_processed_data = 1
        """
        cur.execute(query, (date, giant_name))
        results = cur.fetchall()
        tickers = [row['ticker'] for row in results]
        return JSONResponse(content={"date": date, "giant": giant_name, "tickers": tickers})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
    finally:
        if conn:
            cur.close()
            conn.close()


################################################################################################
##############################          pages          #########################################
################################################################################################

# from fastapi.staticfiles import StaticFiles
# app.mount("/", StaticFiles(directory="Front/React/dist", html=True), name="static")

################################################################################################
##############################          run          ############################################
################################################################################################

import uvicorn
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)