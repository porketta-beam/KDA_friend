from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import OllamaEmbeddings

class Embedder:
    def __init__(self, config):
        self.config = config
        self.embedding_model = config["embedding_model"]
        self.batch_size = config["batch_size"]
        self.faiss_index_path = config["faiss_index_path"]
        self.ollama_base_url = config["ollama_base_url"]

    def embed_and_index(self, documents):
        embeddings = OllamaEmbeddings(model=self.embedding_model, base_url=self.ollama_base_url)
        vectorstore = FAISS.from_documents(documents, embeddings)
        return vectorstore

    def save_index(self, vectorstore):
        vectorstore.save_local(self.faiss_index_path)
        print(f"[저장] FAISS 인덱스: {self.faiss_index_path}")