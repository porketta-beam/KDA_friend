import os
import json
from src.document_loader import DocumentLoader
from src.embedder import Embedder
from src.query_engine import QueryEngine

def load_config(config_path="config/config.json"):
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    os.environ["OLLAMA_HOST"] = config["ollama_base_url"]
    return config

def embed_documents(config):
    loader = DocumentLoader(config)
    docs = loader.load_documents()
    chunked_docs = loader.chunk_documents(docs)
    embedder = Embedder(config)
    vectorstore = embedder.embed_and_index(chunked_docs)
    embedder.save_index(vectorstore)

def query_loop(config):
    query_engine = QueryEngine(config)
    print("=== RAG 모드로 실행: 로컬 문서 기반 답변 ===")
    print("'exit' 또는 'quit'를 입력하여 종료\n")
    cnt = 1
    while True:
        question = input(f"[질문 {cnt}] ")
        if question.strip().lower() in ("exit", "quit"):
            print("종료합니다.")
            break
        query_engine.process_query(question, cnt)
        cnt += 1

def main():
    config = load_config()
    mode = input("모드 선택 (embed/query): ").strip().lower()
    if mode == "embed":
        embed_documents(config)
    elif mode == "query":
        query_loop(config)
    else:
        print("잘못된 모드입니다. 'embed' 또는 'query'를 선택하세요.")

if __name__ == "__main__":
    main()