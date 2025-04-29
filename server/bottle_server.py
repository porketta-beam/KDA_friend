from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime, timedelta
import sqlite3
import uuid
import random
import uvicorn

app = FastAPI()

# SQLite 데이터베이스 설정
conn = sqlite3.connect("bottle_messages.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY,
        content TEXT,
        created_at TIMESTAMP,
        sender_id TEXT
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS replies (
        reply_id TEXT PRIMARY KEY,
        message_id TEXT,
        content TEXT,
        created_at TIMESTAMP,
        FOREIGN KEY (message_id) REFERENCES messages (message_id)
    )
""")
conn.commit()

# 요청 모델
class MessageCreate(BaseModel):
    content: str

class ReplyCreate(BaseModel):
    message_id: str
    content: str

# 메시지 저장
@app.post("/send_message")
async def send_message(message: MessageCreate):
    message_id = str(uuid.uuid4())
    sender_id = str(uuid.uuid4())  # 익명 ID
    created_at = datetime.now()
    
    cursor.execute(
        "INSERT INTO messages (message_id, content, created_at, sender_id) VALUES (?, ?, ?, ?)",
        (message_id, message.content, created_at, sender_id)
    )
    conn.commit()
    return {"message_id": message_id, "status": "sent"}

# 메시지 가져오기 (무작위)
@app.get("/get_message")
async def get_message():
    # 1시간 지난 메시지 삭제
    cursor.execute("DELETE FROM messages WHERE created_at < ?", 
                  (datetime.now() - timedelta(hours=1),))
    conn.commit()
    
    # 무작위 메시지 선택
    cursor.execute("SELECT message_id, content FROM messages")
    messages = cursor.fetchall()
    if not messages:
        raise HTTPException(status_code=404, detail="No messages available")
    
    selected = random.choice(messages)
    return {"message_id": selected[0], "content": selected[1]}

# 답장 보내기
@app.post("/send_reply")
async def send_reply(reply: ReplyCreate):
    reply_id = str(uuid.uuid4())
    created_at = datetime.now()
    
    cursor.execute(
        "INSERT INTO replies (reply_id, message_id, content, created_at) VALUES (?, ?, ?, ?)",
        (reply_id, reply.message_id, reply.content, created_at)
    )
    conn.commit()
    
    # 원본 메시지 삭제 (1회성 대화)
    cursor.execute("DELETE FROM messages WHERE message_id = ?", (reply.message_id,))
    conn.commit()
    
    return {"reply_id": reply_id, "status": "replied"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

