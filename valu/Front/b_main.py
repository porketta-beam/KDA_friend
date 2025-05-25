import streamlit as st

def render_main_content():
    st.title("타이틀 빠질 생각 중 안 이쁨")
    
    nav1, nav2, nav3, nav4 = st.columns([2,2,2,1])
    with nav1:
        focused_corp = st.button("SK하이닉스 \n 000660")
    with nav2:
        favor_user = st.button("당신의 투자성향은?")
    with nav4:
        back_botton = st.button("뒤로가기")
    st.write("혁주 하이")
    st.write("혁주 하이")
    st.write("혁주 하이")
    st.write("혁주 하이")
