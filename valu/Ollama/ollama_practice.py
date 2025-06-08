import os
from langchain_community.document_loaders import UnstructuredPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import OllamaEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_community.llms import Ollama
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from pdf2image import convert_from_path
from langchain.docstore.document import Document
import pytesseract

# Tesseract 경로 설정 (Windows)
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# 1. PDF에서 텍스트와 표 추출
def extract_pdf_content(pdf_path):
    try:
        loader = UnstructuredPDFLoader(pdf_path, mode="elements")
        documents = loader.load()
        text_docs = [doc for doc in documents if doc.metadata.get("category") != "Table"]
        table_docs = [doc for doc in documents if doc.metadata.get("category") == "Table"]
        return text_docs, table_docs
    except Exception as e:
        print(f"PDF 추출 오류: {e}")
        return [], []

# 2. PDF를 이미지로 변환하고 OCR로 텍스트 추출
def extract_image_text(pdf_path, output_dir="temp_images"):
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        images = convert_from_path(pdf_path, dpi=150)  # CPU 최적화
        image_texts = []
        for i, image in enumerate(images):
            image_path = f"{output_dir}/page_{i+1}.png"
            image.save(image_path, "PNG")
            text = pytesseract.image_to_string(image, lang="eng+kor")
            image_texts.append(text)
        return image_texts
    except Exception as e:
        print(f"이미지 추출 오류: {e}")
        return []

# 3. 텍스트와 표 데이터를 청크로 분리
def chunk_documents(documents):
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,  # CPU 환경 최적화
        chunk_overlap=100,
        length_function=len
    )
    return text_splitter.split_documents(documents)

# 4. 임베딩 및 벡터 저장소 생성
def create_vector_store(text_docs, table_docs, image_texts):
    try:
        embeddings = OllamaEmbeddings(model="mxbai-embed-large", base_url="http://127.0.0.1:11435")
        all_docs = text_docs + table_docs
        image_docs = [Document(page_content=text, metadata={"source": "image"}) for text in image_texts]
        all_docs.extend(image_docs)
        chunks = chunk_documents(all_docs)
        vector_store = FAISS.from_documents(chunks, embeddings)
        return vector_store
    except Exception as e:
        print(f"벡터 저장소 생성 오류: {e}")
        return None

# 5. 벡터 저장소 로드
def load_vector_store(index_path="faiss_index"):
    try:
        embeddings = OllamaEmbeddings(model="mxbai-embed-large", base_url="http://127.0.0.1:11435")
        vector_store = FAISS.load_local(index_path, embeddings, allow_dangerous_deserialization=True)
        return vector_store
    except Exception as e:
        print(f"벡터 저장소 로드 오류: {e}")
        return None

# 6. RAG 체인 설정
def setup_rag_chain(vector_store):
    try:
        llm = Ollama(model="llama3:8b", base_url="http://127.0.0.1:11435")
        prompt_template = """주어진 문맥을 바탕으로 질문에 정확하고 간결하게 답변하세요. 문맥에 없는 정보는 추가하지 마세요.
        문맥: {context}
        질문: {question}
        답변: """
        prompt = PromptTemplate(
            template=prompt_template,
            input_variables=["context", "question"]
        )
        qa_chain = RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=vector_store.as_retriever(search_kwargs={"k": 3}),
            chain_type_kwargs={"prompt": prompt}
        )
        return qa_chain
    except Exception as e:
        print(f"RAG 체인 설정 오류: {e}")
        return None

# 7. 질문에 답변 생성
def answer_question(qa_chain, question):
    try:
        result = qa_chain.invoke({"query": question})
        return result["result"]
    except Exception as e:
        print(f"답변 생성 오류: {e}")
        return "답변을 생성할 수 없습니다."

# 실행 예시
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    pdf_path = os.path.join(base_dir, 'sample.pdf')
    text_docs, table_docs = extract_pdf_content(pdf_path)
    image_texts = extract_image_text(pdf_path)
    vector_store = create_vector_store(text_docs, table_docs, image_texts)
    if vector_store:
        vector_store.save_local("faiss_index")
        
        vector_store = load_vector_store()
        if vector_store:
            qa_chain = setup_rag_chain(vector_store)
            if qa_chain:
                question = "PDF에 포함된 표에서 주요 데이터를 요약해줘."
                answer = answer_question(qa_chain, question)
                print(f"질문: {question}\n답변: {answer}")