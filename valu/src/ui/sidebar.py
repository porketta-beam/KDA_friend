import streamlit as st
import ollama
import pandas as pd

def render_chat_sidebar(active_sidebar, df=None):
    st.write("💬 채팅")
    
    ########## history 초기화 ##########
    if "history" not in st.session_state:
        st.session_state["history"] = []
        if df is not None:
            # DataFrame 정보를 시스템 메시지로 추가
            df_info = f"""
            다음은 주식 데이터프레임의 정보입니다:
            - 컬럼: {', '.join(df.columns)}
            - 행 수: {len(df)}
            - 데이터 타입: {df.dtypes.to_dict()}
            - 처음 5개 행:
            {df.head().to_string()}
            """
            st.session_state["history"].append({
                "role": "system",
                "content": df_info
            })

    ########## history 출력 ##########
    for message in st.session_state["history"]:
        if message["role"] != "system":  # 시스템 메시지는 출력하지 않음
            with st.chat_message(message["role"]):
                st.write(message["content"])

    ########## 사용자 입력 ##########
    prompt = st.chat_input("데이터프레임에 대해 질문하세요.")
    if prompt:
        with st.chat_message("user"):
            st.write(prompt)
        st.session_state["history"].append({"role": "user", "content": prompt})

        # 스트리밍 응답을 위한 메시지 컨테이너 생성
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""
            
            # 스트리밍 응답 처리
            for chunk in ollama.chat(
                model="gemma3:4b",
                messages=st.session_state["history"],
                stream=True
            ):
                if chunk.message.content:
                    full_response += chunk.message.content
                    message_placeholder.write(full_response + "▌")
            
            # 최종 응답 표시
            message_placeholder.write(full_response)
        
        # 히스토리에 응답 추가
        st.session_state["history"].append({"role": "assistant", "content": full_response})

def render_sidebar_content(active_sidebar, text1=None, text2=None, text3=None, df=None):
    st.title("📑 사이드바")
    
    if active_sidebar == 1:
        render_chat_sidebar(active_sidebar, df)
    elif active_sidebar == 2:
        render_chat_sidebar(active_sidebar, df)
    elif active_sidebar == 3:
        render_chat_sidebar(active_sidebar, df)
    else:
        st.write("사이드바를 열려면 버튼을 클릭하세요.") 