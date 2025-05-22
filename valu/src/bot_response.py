from datetime import datetime
from .bot.indicator_bot import handle_indicator_response
from .bot.strategy_bot import handle_strategy_response
from .bot.decision_bot import handle_decision_response

def handle_chat_response(message, stage=None):
    """
    사용자의 메시지에 대한 응답을 생성하는 함수
    
    Args:
        message (str): 사용자가 입력한 메시지
        stage (int): 현재 단계 (1: 지표 추천, 2: 전략 추천, 3: 의사결정)
        
    Returns:
        dict: 응답 메시지와 시간 정보를 포함한 딕셔너리
    """
    current_time = datetime.now().strftime("%H:%M")
    
    # 단계별 응답 처리
    if stage == 1:
        response_message = handle_indicator_response(message)
    elif stage == 2:
        response_message = handle_strategy_response(message)
    elif stage == 3:
        response_message = handle_decision_response(message)
    else:
        response_message = "어떤 단계에서 도움이 필요하신가요?"
    
    response = {
        "message": response_message,
        "time": current_time,
        "is_user": False
    }
    
    return response
