import html

def unwrap(x):
    """문자열로 변환."""
    return str(x)

def save_answer(content: str, file_suffix: str, formats: list, prefix: str = ""):
    """내용을 지정된 형식으로 저장."""
    for fmt in formats:
        if fmt == "txt":
            path = f"{prefix}final_answer_{file_suffix}.txt"
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"[저장] {path}")
        elif fmt == "html":
            path = f"{prefix}final_answer_{file_suffix}.html"
            escaped = html.escape(content)
            html_body = escaped.replace("\n", "<br>\n")
            html_content = f"""
            <html>
            <body>
            {html_body}
            </body>
            </html>
            """
            with open(path, "w", encoding="utf-8") as f:
                f.write(html_content)
            print(f"[저장] {path}")
        else:
            print(f"⚠️ 지원하지 않는 형식: {fmt}")