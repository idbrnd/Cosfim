from typing import Union, List, Dict, Any
import uvicorn
from fastapi import FastAPI, HTTPException, Form, File, UploadFile, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import logging
import asyncio
import threading
import queue
import uuid
import time
import json
from datetime import datetime
from pathlib import Path
from contextlib import asynccontextmanager
from multi import MultiCosfimManager, Forwarder, CosfimHandler

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s')
logger = logging.getLogger("FastAPI-COSFIM")

# 전역 설정
#API_END_POINT = "http://121.65.104.214:8081/api/v1/widget/upload/cosfim"
API_END_POINT = "http://223.130.139.28/api/v1/widget/upload/cosfim"

USER_ID = "20052970"
USER_PW = "20052970"

# 전역 관리자 인스턴스
manager = None

class CosfimInputDto(BaseModel):
    waterSystemName: str
    damName: str
    damCode: str
    templateId: str
    widgetName : str
    sessionId : str
    optData: str

class TaskStatusResponse(BaseModel):
    task_id: str
    status: str  # "queued", "processing", "completed", "failed"
    created_at: str
    completed_at: str = None
    error_message: str = None
    result: Dict[str, Any] = None

class QueueStatusResponse(BaseModel):
    queue_size: int
    is_processing: bool
    total_completed: int
    current_task: str = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작/종료 시 실행되는 컨텍스트 매니저"""
    global manager
    
    # 시작 시
    logger.info("COSFIM Queue Manager 초기화 중...")
    manager = MultiCosfimManager()
    manager.start_processing()
    logger.info("COSFIM Queue Manager 시작 완료")
    
    yield
    
    # 종료 시
    logger.info("COSFIM Queue Manager 종료 중...")
    if manager:
        manager.stop_processing()
    logger.info("COSFIM Queue Manager 종료 완료")

app = FastAPI(
    title="COSFIM Queue API",
    description="큐 기반 COSFIM 댐 데이터 처리 시스템",
    version="1.0.0",
    lifespan=lifespan
)

# 작업 상태 저장소 (실제로는 DB를 사용하는 것이 좋음)
task_storage: Dict[str, Dict[str, Any]] = {}

class TaskTracker:
    """작업 추적 클래스"""
    
    @staticmethod
    def create_task(task_id: str, water_system: str, dam_name: str):
        """새 작업 생성"""
        task_storage[task_id] = {
            "task_id": task_id,
            "status": "queued",
            "created_at": datetime.now().isoformat(),
            "water_system": water_system,
            "dam_name": dam_name,
            "completed_at": None,
            "error_message": None,
            "result": None
        }
    
    @staticmethod
    def update_task_status(task_id: str, status: str, error_message: str = None, result: Dict = None):
        """작업 상태 업데이트"""
        if task_id in task_storage:
            task_storage[task_id]["status"] = status
            if status in ["completed", "failed"]:
                task_storage[task_id]["completed_at"] = datetime.now().isoformat()
            if error_message:
                task_storage[task_id]["error_message"] = error_message
            if result:
                task_storage[task_id]["result"] = result
    
    @staticmethod
    def get_task(task_id: str) -> Dict[str, Any]:
        """작업 정보 조회"""
        return task_storage.get(task_id)
    
    @staticmethod
    def get_all_tasks() -> List[Dict[str, Any]]:
        """모든 작업 조회"""
        return list(task_storage.values())

def process_file_content(file_content: bytes) -> str:
    """업로드된 txt 파일 내용 처리"""
    try:
        # UTF-8 디코딩 시도
        opt_data_str = file_content.decode('utf-8')
    except UnicodeDecodeError:
        try:
            # CP949 (한국어 Windows) 디코딩 시도
            opt_data_str = file_content.decode('cp949')
        except UnicodeDecodeError:
            try:
                # EUC-KR 시도
                opt_data_str = file_content.decode('euc-kr')
            except UnicodeDecodeError:
                # 마지막 시도 - Latin-1
                opt_data_str = file_content.decode('latin-1')
    
    # 개행 문자 정규화 (Windows, Unix, Mac 호환)
    opt_data_processed = opt_data_str.replace("\r\n", "\n").replace("\r", "\n")
    
    # 첫 줄 끝 공백 맞추기 (원본 코드와 호환성 위해)
    lines = opt_data_processed.split("\n")
    if len(lines) > 0 and not lines[0].endswith("  "):
        lines[0] = lines[0].rstrip() + "  "
    
    # 빈 줄 끝의 공백 제거하지 않고 유지 (OPT 포맷의 경우 중요할 수 있음)
    return "\n".join(lines)

async def background_result_updater():
    """백그라운드에서 주기적으로 결과 업데이트"""
    while True:
        try:
            if manager:
                # 완료된 결과들 가져오기
                recent_results = manager.task_queue.get_results()
                
                for result in recent_results:
                    task_id = result.get('task_id')
                    if task_id and task_id in task_storage:
                        if result.get('success'):
                            TaskTracker.update_task_status(
                                task_id, 
                                "completed", 
                                result=result
                            )
                            logger.info(f"Task {task_id} completed successfully")
                        else:
                            TaskTracker.update_task_status(
                                task_id, 
                                "failed", 
                                error_message=result.get('error', 'Unknown error')
                            )
                            logger.error(f"Task {task_id} failed: {result.get('error')}")
            
            await asyncio.sleep(5)  # 5초마다 확인
            
        except Exception as e:
            logger.error(f"Background updater error: {e}")
            await asyncio.sleep(10)

# 백그라운드 작업 시작
@app.on_event("startup")
async def startup_event():
    """앱 시작 시 백그라운드 작업 시작"""
    asyncio.create_task(background_result_updater())

@app.get("/")
def read_root():
    """API 루트"""
    return {
        "message": "COSFIM Queue API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
def health_check():
    """헬스 체크"""
    global manager
    return {
        "status": "healthy",
        "queue_manager": "running" if manager and manager.task_queue.is_running else "stopped",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/v1/cosfim/submit", response_model=Dict[str, str])
async def submit_cosfim_task(
    waterSystemName: str = Form(..., description="수계명 (예: 낙동강)"),
    damName: str = Form(..., description="댐명 (예: 합천댐)"),
    damCode: str = Form(..., description="댐 코드"),
    templateId: str = Form(..., description="템플릿 ID"),
    widgetName : str = Form(..., description="LLM에서 생성한 위젯명"),
    optData: UploadFile = File(..., description="OPT 데이터 텍스트 파일 (.txt)"),
    sessionId : str = Form(..., description="sessionId")
):
    """COSFIM 작업을 큐에 제출"""
    
    print(sessionId, widgetName)

    try:
        global manager
        
        if not manager or not manager.task_queue.is_running:
            raise HTTPException(status_code=503, detail="Queue manager is not running")
        
        # 파일 확장자 검증
        if not optData.filename.lower().endswith('.txt'):
            raise HTTPException(status_code=400, detail="optData must be a .txt file")
        
        # 파일 내용 읽기 및 처리
        file_content = await optData.read()
        opt_data_processed = process_file_content(file_content)
        
        # 작업을 큐에 추가
        task_id = manager.add_dam_task(
            water_system_name=waterSystemName,
            dam_name=damName,
            dam_code=damCode,
            template_id=templateId,
            user_id=USER_ID,
            user_pw=USER_PW,
            opt_data=opt_data_processed,
            api_end_point=API_END_POINT,
            session_id = sessionId,
            widget_name = widgetName
        )
        
        # 작업 추적 정보 생성
        TaskTracker.create_task(task_id, waterSystemName, damName)
        
        logger.info(f"Task submitted: {task_id} for {damName} with txt file: {optData.filename}")
        
        return {
            "task_id": task_id,
            "status": "queued",
            "message": f"Task for {damName} has been queued successfully with OPT file: {optData.filename}"
        }
        
    except Exception as e:
        logger.error(f"Error submitting task: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to submit task: {str(e)}")



if __name__ == "__main__":
    uvicorn.run(
        "app:app", 
        host='0.0.0.0', 
        port=8000, 
        reload=True,
        reload_excludes=["work_*", "*.csv", "*.OPT", "*.zip"]  # 작업 디렉토리와 결과 파일 제외
    )
    # uvicorn.run("app:app", host='0.0.0.0', port=8000, reload=True)
