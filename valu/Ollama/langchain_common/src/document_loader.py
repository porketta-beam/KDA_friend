import os
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import TextLoader, CSVLoader, UnstructuredPowerPointLoader, UnstructuredPDFLoader, UnstructuredHTMLLoader
from pdf2image import convert_from_path
from langchain.docstore.document import Document
import pytesseract

class DocumentLoader:
    def __init__(self, config):
        self.config = config
        self.base_path = config["base_path"]
        self.document_types = config["document_types"]
        self.chunk_size = config["chunk_size"]
        self.chunk_overlap = config["chunk_overlap"]
        self.poppler_path = config.get("poppler_path", r"C:\Program Files\poppler-24.08.0\Library\bin")
        pytesseract.pytesseract.tesseract_cmd = config.get("tesseract_path", r"C:\Program Files\Tesseract-OCR\tesseract.exe")

    def load_documents(self):
        documents = []
        for doc_type, settings in self.document_types.items():
            extensions = settings["extensions"]
            loader_class = settings["loader"]
            loader_kwargs = settings.get("kwargs", {})
            for ext in extensions:
                for root, _, files in os.walk(self.base_path):
                    for file in files:
                        if file.endswith(f".{ext}"):
                            file_path = os.path.join(root, file)
                            try:
                                if loader_class == "TextLoader":
                                    loader = TextLoader(file_path, **loader_kwargs)
                                elif loader_class == "CSVLoader":
                                    loader = CSVLoader(file_path, **loader_kwargs)
                                elif loader_class == "UnstructuredPowerPointLoader":
                                    loader = UnstructuredPowerPointLoader(file_path, **loader_kwargs)
                                elif loader_class == "UnstructuredPDFLoader":
                                    loader = UnstructuredPDFLoader(file_path, mode="elements", **loader_kwargs)
                                    docs = loader.load()
                                    # PDF 이미지에서 텍스트 추출 (OCR)
                                    image_texts = self.extract_image_text(file_path)
                                    image_docs = [Document(page_content=text, metadata={"source": file_path, "type": "image"}) for text in image_texts]
                                    docs.extend(image_docs)
                                    documents.extend(docs)
                                    print(f"[로드] {file_path} (텍스트, 표, 이미지)")
                                    continue
                                elif loader_class == "UnstructuredHTMLLoader":
                                    loader = UnstructuredHTMLLoader(file_path, **loader_kwargs)
                                else:
                                    print(f"⚠️ 지원하지 않는 로더: {loader_class}")
                                    continue
                                docs = loader.load()
                                documents.extend(docs)
                                print(f"[로드] {file_path}")
                            except Exception as e:
                                print(f"[에러] {file_path} 로드 실패: {e}")
        return documents

    def extract_image_text(self, pdf_path, output_dir="temp_images"):
        try:
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            images = convert_from_path(pdf_path, dpi=150, poppler_path=self.poppler_path)
            image_texts = []
            for i, image in enumerate(images):
                image_path = f"{output_dir}/page_{i+1}.png"
                image.save(image_path, "PNG")
                text = pytesseract.image_to_string(image, lang="eng+kor")
                image_texts.append(text)
            return image_texts
        except Exception as e:
            print(f"[에러] PDF 이미지 텍스트 추출 실패: {pdf_path}, {e}")
            return []

    def chunk_documents(self, documents):
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap
        )
        return text_splitter.split_documents(documents)