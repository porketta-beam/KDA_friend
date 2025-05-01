from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
import sqlite3
import uuid
import random
import uvicorn

app = FastAPI()

class Message(BaseModel):
    content: str
    sender: str
    receiver: str
    timestamp: datetime = datetime.now()

@app.post("/messages")
async def receive_message(message: Message):
    try:
        conn = sqlite3.connect('messages.db')
        cursor = conn.cursor()
        
        # 메시지 테이블이 없는 경우 생성
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages
            (id TEXT PRIMARY KEY,
             content TEXT,
             sender TEXT, 
             receiver TEXT,
             timestamp TIMESTAMP)
        ''')
        
        # 새 메시지 저장
        message_id = str(uuid.uuid4())
        cursor.execute('''
            INSERT INTO messages (id, content, sender, receiver, timestamp)
            VALUES (?, ?, ?, ?, ?)
        ''', (message_id, message.content, message.sender, message.receiver, message.timestamp))
        
        conn.commit()
        conn.close()
        
        return {"message_id": message_id, "status": "메시지가 성공적으로 저장되었습니다."}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"메시지 저장 중 오류 발생: {str(e)}")

@app.get("/messages/{receiver}")
async def get_messages(receiver: str):
    try:
        conn = sqlite3.connect('messages.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM messages 
            WHERE receiver = ?
            ORDER BY timestamp DESC
        ''', (receiver,))
        
        messages = cursor.fetchall()
        conn.close()
        
        return [{"id": msg[0], 
                "content": msg[1],
                "sender": msg[2],
                "receiver": msg[3],
                "timestamp": msg[4]} for msg in messages]
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"메시지 조회 중 오류 발생: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)