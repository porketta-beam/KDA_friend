import os
import io
import zipfile
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from tqdm import tqdm
from pykrx import stock

def download_corp_code_zip(api_key: str) -> bytes:
    """OpenDART에서 기업 리스트 ZIP(XML) 다운로드"""
    print("기업 리스트(zip) 다운로드 중...")
    url = f'https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={api_key}'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        raise RuntimeError(f"corpCode.xml 다운로드 실패: {e}")

def parse_zip_to_dataframe(xml_zip_content: bytes) -> pd.DataFrame:
    """ZIP 파일에서 XML 추출 및 기업 리스트 DataFrame 생성"""
    try:
        with zipfile.ZipFile(io.BytesIO(xml_zip_content)) as z:
            xml_data = z.read(z.namelist()[0]).decode("utf-8")
    except zipfile.BadZipFile as e:
        raise RuntimeError(f"ZIP 파일 처리 실패: {e}")

    try:
        root = ET.fromstring(xml_data)
        data = [
            {
                "ticker": corp.findtext("stock_code", default=""),
                "corp_code": corp.findtext("corp_code", default=""),
                "corp_name": corp.findtext("corp_name", default=""),
                "modify_date": corp.findtext("modify_date", default="")
            }
            for corp in tqdm(root.findall("list"), desc="XML 파싱 중")
        ]
        return pd.DataFrame(data)
    except ET.ParseError as e:
        raise RuntimeError(f"XML 파싱 실패: {e}")

def filter_listed_companies(df: pd.DataFrame) -> pd.DataFrame:
    """pykrx 데이터와 교집합"""

    print("pykrx에서 상장 종목코드 가져오는 중...")
    pykrx_tickers = stock.get_market_ticker_list(market="ALL")
    pykrx_df = pd.DataFrame({'ticker': pykrx_tickers})

    listed = df.merge(pykrx_df, on="ticker", how="inner")
    cols = ['ticker', 'corp_code', 'corp_name', 'modify_date']
    return listed[cols].sort_values(by='corp_name')

def save_dataframe(df: pd.DataFrame, filepath: str):
    """DataFrame을 CSV로 저장"""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    try:
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"저장 완료: {filepath}")
    except (PermissionError, OSError) as e:
        raise RuntimeError(f"CSV 저장 실패: {e}")

def main(api_key: str, output_dir: str) -> pd.DataFrame:
    """상장사 리스트를 처리하고 ticker_info.csv에 저장 (항상 덮어쓰기)"""
    save_path = os.path.join(output_dir, "ticker_info.csv")
    
    # 새 데이터 생성
    xml_zip = download_corp_code_zip(api_key)
    corp_df = parse_zip_to_dataframe(xml_zip)
    listed_df = filter_listed_companies(corp_df)
    
    # 항상 새 데이터로 저장
    save_dataframe(listed_df, save_path)
    print(f"\n상장사 수: {len(listed_df)}")
    return listed_df

if __name__ == "__main__":
    # ⬇️ 여기에 본인의 DART 인증키 입력
    api_key = ''
    output_dir = r"C:\Users\gkstnrud\Dev\KDA_friend\valu\Value\src_data"
    main(api_key, output_dir)