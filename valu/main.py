from fastapi import FastAPI
from 정우님 작업한 폴더 import cal_indicator

app = FastAPI()

################################################################################################
##############################          /api/          #########################################
################################################################################################

################################################################
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



################################################################
# get 요청 예제
from fastapi.responses import JSONResponse
@app.get("/api/cp_list")
def 마음대로_get():
    temp = cal_indicator(변수 어쩌고)
    return JSONResponse(content={'cp_list': temp})


    
################################################################
@app.post("/api/giant_request_recommend")
def post_giant_request_recommend(request: Giant_request):
    print(request.cp_list)
    return JSONResponse(content={'cp_list': 'json'})

################################################################
# path parameter 요청 예제
@app.get("/api/path_example/{item_id}")
def get_path_example(item_id: int):
    return JSONResponse(content={'item_id': item_id})

################################################################
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
##############################          pages          #########################################
################################################################################################

from fastapi.staticfiles import StaticFiles
app.mount("/", StaticFiles(directory="Front/React/dist", html=True), name="static")

################################################################################################
##############################          run          ############################################
################################################################################################

import uvicorn
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)