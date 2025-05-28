from pathlib import Path

# 데이터 유형과 파일명 매핑
FINANCIAL_DATA_TYPES = {
    "financials": "financials",
    "quarterly_financials": "quarterly_financials",
    "balance_sheet": "balance_sheet",
    "quarterly_balance_sheet": "quarterly_balance_sheet",
    "cashflow": "cash_flow",
    "quarterly_cashflow": "quarterly_cash_flow"
}

# 기본 타임아웃 (초)
REQUEST_TIMEOUT = 0.5