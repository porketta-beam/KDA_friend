# valu/routers/api_pick.py
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from valu.services.pick_service import get_giant_picks, get_ticker_data

router = APIRouter()

@router.get("/ticker/{ticker}")
def get_ticker(ticker: str):
    try:
        result = get_ticker_data(ticker)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.get("/pick/{date}")
def get_picks(date: str, giant: str = Query(...)):
    try:
        result = get_giant_picks(date, giant)
        return JSONResponse(content=result)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)