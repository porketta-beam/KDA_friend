import streamlit as st
from datetime import datetime

def handle_chat_response(message):
    """
    사용자의 메시지에 대한 응답을 생성하는 함수
    
    Args:
        message (str): 사용자가 입력한 메시지
        
    Returns:
        dict: 응답 메시지와 시간 정보를 포함한 딕셔너리
    """
    current_time = datetime.now().strftime("%H:%M")
    
    # 응답 메시지 생성
    response = {
        "message": "네 듣고 있습니다",
        "time": current_time,
        "is_user": False
    }
    
    return response
