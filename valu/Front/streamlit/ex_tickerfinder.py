import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import yfinance as yf
import io
import requests

@st.cache_data
def get_kospi_kosdaq_list():
    url = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download"
    response = requests.get(url)
    response.encoding = 'euc-kr'
    tables = pd.read_html(io.StringIO(response.text))
    df = tables[0]
    df = df[['회사명', '종목코드']]
    df['종목코드'] = df['종목코드'].apply(lambda x: f"{x:06d}.KS")
    # 회사명이 중복된 경우 첫 번째 데이터만 유지하고 나머지는 제거
    df = df.drop_duplicates(subset=['회사명'], keep='first')
    return df

def main():
    st.set_page_config(page_title="Stock Picker", layout="wide")
    st.title("🔍빨리 편리")

    stock_df = get_kospi_kosdaq_list()
    company_list = ["종목을 선택하세요"] + stock_df['회사명'].tolist()  # 빈 문자열을 첫 번째 옵션으로 추가

    selected_name = st.selectbox(
        "회사명을 검색하세요 (자동완성 가능)",
        options=company_list,
        index=0,  # 기본값으로 첫 번째 옵션("") 선택
        placeholder="종목을 선택하세요"  # Streamlit 1.27.0 이상에서 동작
    )

    if not selected_name:  # selected_name이 빈 문자열일 경우
        st.info("📢 종목을 입력하세요!")
        return

    selected_name = selected_name.strip()
    filtered = stock_df[stock_df['회사명'] == selected_name]['종목코드']
    if filtered.empty:
        st.error(f"종목 '{selected_name}'에 해당하는 코드가 없습니다.")
        return
    elif len(filtered) > 1:
        st.warning(f"종목 '{selected_name}'에 대해 여러 코드가 있습니다. 첫 번째 코드를 사용합니다.")
    selected_code = filtered.values[0]
    st.success(f"선택한 종목: {selected_name} / 코드: {selected_code}")

    df = yf.download(selected_code, period="6mo")
    if df.empty:
        st.error(f"종목 코드 '{selected_code}'에 대한 데이터를 가져올 수 없습니다.")
        return
    
    # 차트 표시
    # 한글 폰트 설정
    plt.rcParams['font.family'] = 'Malgun Gothic'
    # 마이너스 기호 깨짐 방지
    plt.rcParams['axes.unicode_minus'] = False
    st.subheader("📈 주가 차트")
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.plot(df.index, df['Close'])
    ax.set_title(f"{selected_name} 종가 추이")
    ax.set_xlabel("날짜")
    ax.set_ylabel("종가 (원)")
    ax.grid(True)
    st.pyplot(fig)
    
    # 데이터 테이블 표시
    st.subheader("📋 상세 데이터")
    st.dataframe(df.tail(), use_container_width=True)

    # Text inputs and buttons
    user_input = st.text_input("첫 번째 텍스트", key="text1")
    if st.button("첫 번째 사이드바", key="btn1"):
        st.session_state.active_sidebar = 1

    text2 = st.text_input("두 번째 텍스트", key="text2")
    if st.button("두 번째 사이드바", key="btn2"):
        st.session_state.active_sidebar = 2

    text3 = st.text_input("세 번째 텍스트", key="text3")
    if st.button("세 번째 사이드바", key="btn3"):
        st.session_state.active_sidebar = 3

    # Sidebar content
    with st.sidebar:
        st.title("📑 사이드바")

        if 'active_sidebar' not in st.session_state:
            st.session_state.active_sidebar = None

        if st.session_state.active_sidebar == 1:
            st.write("첫 번째 텍스트 박스의 사이드바")
            st.write(f"입력된 텍스트: {user_input}")

        elif st.session_state.active_sidebar == 2:
            st.write("두 번째 텍스트 박스의 사이드바")
            st.write(f"입력된 텍스트: {text2}")

        elif st.session_state.active_sidebar == 3:
            st.write("세 번째 텍스트 박스의 사이드바")
            st.write(f"입력된 텍스트: {text3}")

        else:
            st.write("사이드바를 열려면 버튼을 클릭하세요.")

if __name__ == "__main__":
    main()