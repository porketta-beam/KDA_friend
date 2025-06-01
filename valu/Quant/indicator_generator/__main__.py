import sys
from pathlib import Path
import logging
from .gen_indicator import main as run_gen_indicator
from .calculate_rs import calculate_rs
from .indicator_filter import filter_giants_pick

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

def run_pipeline():
    """
    전체 지표 생성 및 필터링 파이프라인 실행.
    단계: gen_indicator -> calculate_rs -> indicator_filter
    """
    base_dir = Path(__file__).parent.parent / "src_data"
    
    try:
        # 1. 지표 생성
        logger.info("Starting gen_indicator...")
        run_gen_indicator()
        logger.info("Completed gen_indicator.")
        
        # 2. 상대강도(RS) 계산
        logger.info("Starting calculate_rs...")
        calculate_rs()
        logger.info("Completed calculate_rs.")
        
        # 3. 거장별 필터링
        logger.info("Starting indicator_filter...")
        filter_giants_pick()
        logger.info("Completed indicator_filter.")
        
        logger.info("Pipeline completed successfully.")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()