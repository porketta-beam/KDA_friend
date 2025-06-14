from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from valu.services.pick_service import get_giant_picks

router = APIRouter()

@router.get("/pick/{date}")
def get_picks(date: str, giant: str = Query(...)):
    try:
        tickers = get_giant_picks(date, giant)
        return JSONResponse(content={
            "date": date,
            "giant": f"{giant}_Pick",
            "tickers": tickers
        })
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
