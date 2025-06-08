from langchain_community.llms import Ollama
from langchain_community.embeddings import OllamaEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.chains import ConversationalRetrievalChain
from langchain.memory import ConversationBufferMemory
from .utils import unwrap, save_answer
import time

class QueryEngine:
    def __init__(self, config):
        self.config = config
        self.embeddings = OllamaEmbeddings(model=config["embedding_model"], base_url=config["ollama_base_url"])
        self.vectorstore = FAISS.load_local(
            config["faiss_index_path"],
            self.embeddings,
            allow_dangerous_deserialization=True
        )
        self.retriever = self.vectorstore.as_retriever(search_kwargs={"k": config["retriever_k"]})
        self.memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
        self.conv_chain = ConversationalRetrievalChain.from_llm(
            llm=Ollama(
                model=config["llm_model"],
                base_url=config["ollama_base_url"],
                temperature=config["temperature"],
                max_tokens=config["max_tokens"]
            ),
            retriever=self.retriever,
            memory=self.memory,
            return_source_documents=False
        )
        self.output_formats = config.get("output_formats", ["txt"])
        self.query_mode = "rag"  # 로컬에서 웹 검색 제외
        self.prompts = config.get("prompts", {})

    def process_rag(self, question):
        """RAG 답변 생성"""
        rag_prompt = self.prompts.get("rag", {}).get("template", "질문: {question}").format(question=question)
        rag_result = self.conv_chain({"question": rag_prompt})
        return unwrap(rag_result["answer"])

    def process_query(self, question, query_number):
        start_time = time.time()
        try:
            answer = self.process_rag(question)
            print(f"\n[RAG 답변 {query_number}]\n{answer}\n")
            save_answer(answer, f"{query_number}_rag", self.output_formats, prefix="rag_")
        except Exception as e:
            print(f"[에러] RAG 모드 처리 중 문제 발생: {e}")
            answer = f"에러: {e}"
        print(f"[처리 시간] {time.time() - start_time:.2f}초")