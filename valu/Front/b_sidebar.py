import streamlit as st
import time

def render_sidebar_content():    
    st.sidebar.title("Mark Minervini")
    
    focused_giant = st.sidebar.container(border=True)
    with focused_giant:
        focused_giant.write("Mark Minervini")
        focused_giant.image("https://static.valley.town/image/gurus/mark-minervini/original.jpg")
        focused_giant.write("이 부분 CSS 적용 필요")
    
    chat_history = st.sidebar.container(height=400)
    with chat_history:
        st.write("Chat History")
        bot_message = st.chat_message("assistant")
        user_message = st.chat_message("user")
        bot_message.write("안녕하세요! 마크 미너비니입니다.")
        user_message.write("세션처리 필요.")
        bot_message.write("""
        아이고 형님 종목 추천 들어가겠습니다
        
        리가켐 바이오 들어가신 흑우 없으시죠?
        삼성전자 들어간 흑우 없으시죠?
        알잘딱깔센 종목 추천 들어갑니다잉
        입딱 벌리십쇼
        """)

    
    # 마지막 요소로 chat_input 배치
    st.sidebar.chat_input("마크 미너비니에게 물어보세요")

