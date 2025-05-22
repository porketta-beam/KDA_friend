# -*- coding: utf-8 -*-
import streamlit as st
from openai import OpenAI

def main():
    st.set_page_config(page_title="챗봇", page_icon="🤖")
    
    # 사이드바 설정
    st.sidebar.header('설정')
    
    # OpenAI API 키 입력
    api_key = st.sidebar.text_input('OpenAI API 키를 입력하세요', type='password')
    
    # API 키 유효성 검사
    if api_key and not api_key.startswith('sk-'):
        st.sidebar.error('올바른 OpenAI API 키를 입력해주세요 (sk-로 시작해야 합니다)')
        api_key = None
    
    # 메인 화면
    st.title('AI 챗봇')
    st.write('궁금한 것을 물어보세요!')

    # 세션 상태 초기화
    if 'messages' not in st.session_state:
        st.session_state.messages = []

    # 이전 대화 내용 표시
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # 사용자 입력
    if prompt := st.chat_input("메시지를 입력하세요"):
        if not api_key:
            st.error('OpenAI API 키를 입력해주세요')
            return
        elif not api_key.startswith('sk-'):
            st.error('올바른 OpenAI API 키를 입력해주세요 (sk-로 시작해야 합니다)')
            return

        # OpenAI 클라이언트 초기화
        client = OpenAI(api_key=api_key)
        
        # 사용자 메시지 추가
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        try:
            # API 호출
            with st.chat_message("assistant"):
                message_placeholder = st.empty()
                response = client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": m["role"], "content": m["content"]}
                        for m in st.session_state.messages
                    ],
                    stream=True
                )
                
                full_response = ""
                for chunk in response:
                    if chunk.choices[0].delta.content:
                        full_response += chunk.choices[0].delta.content
                        message_placeholder.markdown(full_response + "▌")
                message_placeholder.markdown(full_response)
                
                # 어시스턴트 응답 저장
                st.session_state.messages.append({"role": "assistant", "content": full_response})

        except Exception as e:
            try:
                st.error(f'오류가 발생했습니다: {str(e)}')
            except UnicodeEncodeError:
                st.error('오류가 발생했습니다(인코딩 문제): ' + str(e).encode('utf-8', 'replace').decode('utf-8'))

if __name__ == '__main__':
    main()
