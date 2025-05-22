import pandas as pd
import yfinance as yf
import io
import requests
import streamlit as st

@st.cache_data
def get_kospi_kosdaq_list():
    url = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download"
    response = requests.get(url)
    response.encoding = 'euc-kr'
    tables = pd.read_html(io.StringIO(response.text))
    df = tables[0]
    df = df[['회사명', '종목코드']]
    df['종목코드'] = df['종목코드'].apply(lambda x: f"{x:06d}.KS")
    df = df.drop_duplicates(subset=['회사명'], keep='first')
    return df

def get_stock_data(stock_code):
    return yf.download(stock_code, period="6mo")