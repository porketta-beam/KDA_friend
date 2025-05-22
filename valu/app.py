import streamlit as st
from src.data.stock_data import get_kospi_kosdaq_list, get_stock_data
from src.ui.charts import setup_chart_style, plot_stock_price
from src.ui.sidebar import render_sidebar_content
from src.utils.session import initialize_session_state, handle_chat_submit

def main():
    st.set_page_config(
        page_title="Stock Picker",
        page_icon=":chart_with_upwards_trend:",
        layout="wide"
    )
    st.title("🔍빨리 편리")

    # 세션 상태 초기화
    initialize_session_state()

    # 주식 데이터 가져오기
    stock_df = get_kospi_kosdaq_list()
    company_list = ["종목을 선택하세요"] + stock_df['회사명'].tolist()

    selected_name = st.selectbox(
        "회사명을 검색하세요 (자동완성 가능)",
        options=company_list,
        index=0,
        placeholder="종목을 선택하세요"
    )

    if selected_name == "종목을 선택하세요":
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

    # 주식 데이터 가져오기
    df = get_stock_data(selected_code)
    if df.empty:
        st.error(f"종목 코드 '{selected_code}'에 대한 데이터를 가져올 수 없습니다.")
        return
    
    st.session_state.active_sidebar = 1
    
    col1, col2 = st.columns(2)

    with col2:
        # 차트 표시
        setup_chart_style()
        st.subheader("📈 주가 차트")
        fig = plot_stock_price(df, selected_name)
        st.pyplot(fig)
        
        # 데이터 테이블 표시
        st.subheader("📋 상세 데이터")
        st.dataframe(df.tail(), use_container_width=True)

    with col1:
        # Text inputs and buttons
        text1 = st.text_area("지표 추천", key="text1", height=None, max_chars=None)
        if st.button("첫 번째 사이드바", key="btn1"):
            st.session_state.active_sidebar = 1

        text2 = st.text_area("전략추천", key="text2", height=150)
        if st.button("두 번째 사이드바", key="btn2"):
            st.session_state.active_sidebar = 2

        text3 = st.text_area("의사결정", key="text3", height=150)
        if st.button("세 번째 사이드바", key="btn3"):
            st.session_state.active_sidebar = 3

    # Sidebar content
    with st.sidebar:
        render_sidebar_content(st.session_state.active_sidebar, text1, text2, text3, df)

if __name__ == "__main__":
    main()