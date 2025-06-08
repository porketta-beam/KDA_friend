```markdown
# LangChain Document RAG

이 프로젝트는 LangChain을 사용하여 다양한 문서(.txt, .ipynb, .sql, .csv, .ppt, .pptx, .pdf)를 처리하고 FAISS 벡터 스토어에 임베딩한 후, RAG, 웹 검색, 또는 둘의 통합을 통해 질문에 답변합니다. 병렬 답변 생성과 2차 질문 기능을 지원하며, 프롬프트는 `config.yaml`에서 커스터마이징 가능합니다.

## 설정

1. **의존성 설치**:
   ```bash
   pip install -r requirements.txt
   ```

2. **설정 파일 수정**:
   - `config/config.yaml` 파일에서 OpenAI API 키, 문서 경로, 출력 형식, 질의 모드, 프롬프트 템플릿, 병렬 모드, 2차 질문 설정 등을 수정하세요.

3. **디렉토리 구조**:
   ```
   project_root/
   ├── config/
   │   └── config.yaml
   ├── src/
   │   ├── document_loader.py
   │   ├── embedder.py
   │   ├── query_engine.py
   │   ├── utils.py
   ├── main.py
   ├── requirements.txt
   └── README.md
   ```

## 사용 방법

1. **문서 임베딩**:
   ```bash
   python main.py
   ```
   - `embed` 모드를 선택하여 문서를 로드, 청킹, 임베딩하고 FAISS 인덱스를 생성합니다.

2. **질의 처리**:
   ```bash
   python main.py
   ```
   - `query` 모드를 선택하여 대화형 질의 루프를 시작합니다.
   - `config.yaml`의 `query_mode`로 기본 답변 모드 지정(`rag`, `web`, `combined`).
   - `parallel_modes`로 병렬 실행할 모드 지정(예: `["rag", "web"]`).
   - `secondary_query.enabled`가 `true`일 경우, 기본 모드 답변에 대해 2차 질문 수행.
   - 답변은 `config.yaml`의 `output_formats`로 저장(파일명에 모드 접두사 포함).
   - `exit` 또는 `quit`를 입력하여 종료합니다.

## 기능

- **문서 로딩**: `.txt`, `.ipynb`, `.sql`, `.csv`, `.ppt`, `.pptx`, `.pdf` 지원. `config.yaml`의 `base_path`와 `document_types`로 다양한 문서 처리.
- **청킹**: 토큰 초과 방지를 위해 문서 분할.
- **임베딩**: OpenAI 임베딩과 FAISS 사용.
- **질의 처리**: `query_mode`로 RAG, 웹 검색, 또는 통합 모드 선택. `parallel_modes`로 병렬 답변 생성.
- **2차 질문**: `secondary_query` 설정으로 초기 답변 기반 추가 질문 수행.
- **프롬프트 엔지니어링**: `config.yaml`의 `prompts`로 RAG, 웹 검색, 통합, 키워드 생성 프롬프트 커스터마이징.
- **출력 형식**: `config.yaml`에서 `txt`, `html`, 또는 둘 다로 저장.

## 프롬프트 커스터마이징

`config.yaml`의 `prompts` 섹션에서 프롬프트 템플릿 수정:
- `rag.template`: 플레이스홀더 `{question}`
- `web.template`: 플레이스홀더 `{question}`, `{web_results}`
- `keyword.template`: 플레이스홀더 `{question}`
- `combined.template`: 플레이스홀더 `{question}`, `{answer}`, `{web_query}`, `{web_results}`
- `secondary_query.template`: 플레이스홀더 `{question}`, `{initial_answer}`

예시:
```yaml
prompts:
  rag:
    template: |
      질문: {question}
      로컬 문서를 참고하여 간결하고 정확한 답변을 제공하며, 초보자를 위한 예시 코드를 포함하세요.
  secondary_query:
    template: |
      사용자 질문: {question}
      초기 답변: {initial_answer}
      답변을 검토하고, 누락된 세부 사항이나 추가 탐구가 필요한 부분을 찾아 2차 질문을 생성한 후 답변하세요.
```

## 설정 수정

`config/config.yaml`에서 커스터마이징:
- API 키
- 문서 경로 및 유형
- 청킹 파라미터
- 임베딩 및 FAISS 설정
- LLM 파라미터 (모델, 온도, 최대 토큰)
- 질의 모드 (`query_mode`)
- 병렬 모드 (`parallel_modes`)
- 2차 질문 설정 (`secondary_query.enabled`, `secondary_query.template`)
- 출력 형식 (`output_formats`)
- 프롬프트 템플릿 (`prompts`)
```