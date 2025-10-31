
import os
import sys
import time
import logging
import pywinauto
from pywinauto.application import Application
import pywinauto.keyboard as keyboard
import pyperclip
import pandas as pd
from io import StringIO
import requests
from pywinauto.timings import TimeoutError
from pywinauto.findwindows import ElementNotFoundError
from contextlib import suppress
import threading
import queue
from datetime import datetime
import uuid
import json
from pathlib import Path
import subprocess

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s')


class TaskQueue:
    """작업 큐 관리 클래스"""
    def __init__(self):
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.is_running = False
        self.worker_thread = None
        
    def add_task(self, task_data):
        """작업을 큐에 추가"""
        task_id = str(uuid.uuid4())
        task = {
            'id': task_id,
            'timestamp': datetime.now(),
            'data': task_data
        }
        self.task_queue.put(task)
        logging.info(f"Task added to queue: {task_id}")
        return task_id
    
    def start_worker(self):
        """워커 스레드 시작"""
        if not self.is_running:
            self.is_running = True
            self.worker_thread = threading.Thread(target=self._worker_loop)
            self.worker_thread.daemon = True
            self.worker_thread.start()
            logging.info("Worker thread started")
    
    def stop_worker(self):
        """워커 스레드 중지"""
        self.is_running = False
        self.task_queue.put(None)  # 종료 신호
        if self.worker_thread:
            self.worker_thread.join()
        logging.info("Worker thread stopped")
    
    def _worker_loop(self):
        """워커 루프 - 순차적으로 작업 처리"""
        while self.is_running:
            try:
                task = self.task_queue.get(timeout=1)
                if task is None:  # 종료 신호
                    break
                
                logging.info(f"Processing task: {task['id']}")
                result = self._process_task(task)
                self.result_queue.put(result)
                self.task_queue.task_done()
                
                # 작업 간 간격 (프로세스 완전 종료 및 GUI 안정화 대기)
                logging.info("다음 작업 시작 전 5초 대기...")
                time.sleep(5)
                
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Error in worker loop: {e}")
                error_result = {
                    'task_id': task.get('id', 'unknown') if 'task' in locals() else 'unknown',
                    'success': False,
                    'error': str(e)
                }
                self.result_queue.put(error_result)
    
    def _process_task(self, task):
        """단일 작업 처리"""
        task_data = task['data']
        task_id = task['id']
        
        try:
            # 작업별 디렉토리 생성
            work_dir = Path(f"./work_{task_id[:8]}")
            work_dir.mkdir(exist_ok=True)
            
            forwarder = Forwarder(
                task_data['api_end_point'],
                task_data['water_system_name'],
                task_data['dam_name'],
                task_data['dam_code'],
                task_data['template_id'],
                task_data['session_id'],
                task_data['widget_name']
            )
            
            handler = CosfimHandler(
                forwarder=forwarder,
                water_system_name=task_data['water_system_name'],
                dam_name=task_data['dam_name'],
                user_id=task_data['user_id'],
                user_pw=task_data['user_pw'],
                opt_data=task_data['opt_data'],
                work_dir=work_dir,
                task_id=task_id[:8]
            )
            
            # 작업 실행
            handler.process()
            
            return {
                'task_id': task_id,
                'success': True,
                'message': f"Successfully processed {task_data['dam_name']}",
                'work_dir': str(work_dir)
            }
            
        except Exception as e:
            logging.error(f"Task {task_id} failed: {e}")
            return {
                'task_id': task_id,
                'success': False,
                'error': str(e)
            }
    
    def get_results(self):
        """결과 가져오기"""
        results = []
        while not self.result_queue.empty():
            try:
                result = self.result_queue.get_nowait()
                results.append(result)
            except queue.Empty:
                break
        return results


class Forwarder:
    def __init__(self, end_point, water_system_name, dam_name, dam_code, template_id, session_id, widget_name):
        self.end_point = end_point
        self.dam_name = dam_name
        self.water_system_name = water_system_name
        self.dam_code = dam_code
        self.template_id = template_id
        self.session_id = session_id
        self.widget_name = widget_name

    def forward(self, success=True, data_path="table_data.csv", err_msg=""):
        files = None
        try: 
            info_data = {
                "damName": self.dam_name,
                "waterSystemName": self.water_system_name, 
                "damCode": self.dam_code,
                "widgetName" : self.widget_name,
                "error": ""
            }
            query_params = {
                "templateId": self.template_id,
                "sessionId" : self.session_id
            }
            
            max_retries = 3
            last_exception = None
            
            for attempt in range(1, max_retries + 1):
                try:
                    if success:
                        message = "success"
                        filename = os.path.basename(data_path)
                        
                        with open(data_path, "rb") as csv_file:
                            files = {"file": (filename, csv_file, "text/csv")}
                            response = requests.post(
                                self.end_point, 
                                files=files, 
                                data=info_data,
                                params=query_params
                            )
                    else:
                        info_data["error"] = err_msg if err_msg else "unknown error"
                        filename = None
                        response = requests.post(
                                self.end_point, 
                                files=None, 
                                data=info_data,
                                params=query_params
                            )

                    # === 요청 정보 출력 ===
                    logging.info("=" * 80)
                    logging.info("[요청 정보]")
                    logging.info(f"URL: {response.request.url}")
                    logging.info(f"Method: {response.request.method}")
                    logging.info(f"Headers: {dict(response.request.headers)}")
                    if success:
                        logging.info(f"Files: {filename}")
                    logging.info(f"Data: {info_data}")
                    logging.info(f"Query Params: {query_params}")
                    
                    # === 응답 정보 출력 ===
                    logging.info("-" * 80)
                    logging.info("[응답 정보]")
                    logging.info(f"Status Code: {response.status_code}")
                    logging.info(f"Reason: {response.reason}")
                    logging.info(f"Headers: {dict(response.headers)}")
                    logging.info(f"Body: {response.text}")
                    
                    # JSON 응답인 경우 예쁘게 출력
                    try:
                        json_response = response.json()
                        logging.info(f"Body (JSON):\n{json.dumps(json_response, indent=2, ensure_ascii=False)}")
                    except:
                        pass
                    
                    logging.info("=" * 80)
                
                    if response.status_code == 200:
                        logging.info("✅ 데이터를 서버에서 성공적으로 수신함")
                        logging.info(f"포워딩 함수 실행 완료: 댐={self.dam_name}, 파일={filename if success else 'N/A'}")
                        return  # 성공 시 함수 종료
                    else:
                        error_msg = f"데이터를 보냈으나 서버에서 실패 응답을 보냄: {response.status_code}, {response.text}"
                        last_exception = Exception(error_msg)
                        
                        if attempt < max_retries:
                            logging.warning(f"⚠️ 포워딩 실패 ({attempt}/{max_retries}), {max_retries - attempt}번 더 재시도.")

                            time.sleep(1)  # 재시도 전 1초 대기
                        else:
                            logging.error(error_msg)
                            raise last_exception
                            
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logging.warning(f"⚠️ 포워딩 실패 ({attempt}/{max_retries}), {max_retries - attempt}번 더 재시도.")
                        time.sleep(1)  # 재시도 전 1초 대기
                    else:
                        logging.error(f"포워딩 함수 실행중에 오류 발생 (최대 재시도 횟수 초과): {e}")
                        raise
            
            # 모든 재시도 실패 시
            if last_exception:
                raise last_exception
                
        except Exception as e:
            logging.error(f"포워딩 함수 실행중에 오류 발생: {e}")
            raise


class CosfimHandler:
    APP_PATH = r"C:\Program Files (x86)\KWater\댐군 홍수조절 연계 운영 시스템\COSFIM_GUI"
    BASE_FILE_DIR = r"C:\COSFIM\WRKSPACE"
    WAIT_TIME = 0.1
    WAIT_TIME_LONG = 0.5
    WAIT_TIME_LONG_LONG = 1
    
    def __init__(self, forwarder, water_system_name, dam_name, user_id, user_pw, opt_data=None, work_dir=None, task_id=None):
        self.forwarder = forwarder
        self.logger = logging.getLogger(f"CosfimHandler-{task_id or 'main'}")
        
        # 작업별 격리
        self.task_id = task_id or str(uuid.uuid4())[:8]
        self.work_dir = work_dir or Path(f"./work_{self.task_id}")
        self.work_dir.mkdir(exist_ok=True)
        
        # 파일 경로 격리
        self.FILE_DIR = str(self.work_dir / "workspace")
        Path(self.FILE_DIR).mkdir(exist_ok=True)
        self.csv_filename = f"table_data_{self.task_id}.csv"
        
        # 인자 및 데이터
        self.water_system_name = water_system_name
        self.dam_name = dam_name
        self.user_id = user_id
        self.user_pw = user_pw
        self.opt_data = opt_data

        if self.opt_data is None:
            return
        self.opt_data = self.set_opt_data(self.opt_data)
        self.logger.info(f"Opt data processed for task {self.task_id}")
        self.start_time = self.get_start_time(self.opt_data)
        self.time_interval = self.get_time_interval(self.opt_data, self.start_time_idx)
        self.time_interval_list = self.get_time_interval_list(self.time_interval)
        self.is_new_instance = None
        
        self.opt_name_map = {
            "낙동강": {
                "하류":"NAMF", "안동댐":"ADMF", "임하댐":"IHMF", "합천댐":"HCMF", "남강댐":"NKMF",  "밀양댐":"MYMF", 
                "운문댐":"UMMF", "영천댐":"YCMF", "영주댐":"YJMF", "성덕댐":"SDMF", "군위댐":"GWMF",  "부항댐":"BHMF",
                "보현댐":"BOMF",  "안계댐":"AKMF", "감포댐":"GPMF", "창녕함안보":"HAMF", "회천":"HOMF"
                },
            "태화강": {
                "하류":"THMF", "대곡댐":"DKMF", "사연댐":"SAMF", "대암댐":"DAMF", "선암댐":"SNMF", "형산강":"HRMF"
                },
            "서낙동": {
                "하류":"WNMF", "창녕함안보":"HAMF"
                },
            "거제권": {
                "하류":"GJMF",  "연초댐":"YNMF", "구천댐":"KCMF"
                },
        }

        # UI 요소 초기화
        self.app = None
        self.main_win = None
        self.tool_bar = None
        self.save_btn = None
        self.load_btn = None
        self.water_system_box = None
        self.dam_box = None

    def set_opt_data(self, opt_data):
        if self.opt_data is not None and os.path.exists(self.opt_data):
            with open(opt_data, "r") as f:
                opt_data = f.read()
            return opt_data
        else:
            return self.opt_data

    def get_start_time(self, opt_data):
        cnt = 0
        opt_data = opt_data.split("\n")
        for idx, line in enumerate(opt_data):
            if line[:2] in("19","20"):
                cnt += 1
                self.logger.info(f"{cnt=}, {line=}")
                if cnt == 2:
                    ele = line.split(" ")
                    year, month, day, hr, min = ele[-6:]
                    self.logger.info(f"{year=} {month=} {day=} {hr=} {min=}")
                    self.start_time_idx = idx
                    break
        return year, month, day, hr, min

    def get_time_interval(self, opt_data, start_time_idx):
        opt_data = opt_data.split("\n")
        ele = opt_data[start_time_idx+2].split(" ")
        time_interval_int = ele[-1]
        time_interval_map = {"10": "10분", "30": "30분", "60": "60분",  "1440": "24시간"}
        time_interval = time_interval_map[time_interval_int]
        self.logger.info(f"{time_interval=}")
        return time_interval

    def get_time_interval_list(self, time_interval):
        try: 
            time_interval_map = {
            "10분": ["00","10","20","30","40","50"],
            "30분": ["00","30"],
            "60분": ["00"]}
            time_interval_list = time_interval_map[time_interval]
        except KeyError:
            time_interval_list = ["00"]
        return time_interval_list

#    def safe_close_existing_instances(self):
#        """기존 COSFIM 인스턴스를 안전하게 종료"""
#        try:
#            existing_app = Application(backend='uia')
#            existing_app.connect(title_re="COSFIM.*Web Service")
#            
#            # 기존 창들 정리
#            for window in existing_app.windows():
#                try:
#                    window.close()
#                    time.sleep(0.5)
#                except:
#                    pass
#            
#            self.logger.info("기존 COSFIM 인스턴스 정리 완료")
#            time.sleep(2)  # 충분한 대기 시간
#        except:
#            self.logger.info("기존 COSFIM 인스턴스 없음")



    def safe_close_existing_instances(self):
        """기존 COSFIM 인스턴스를 안전하게 종료"""
        self.logger.info("=== 기존 COSFIM 프로세스 정리 시작 ===")
        
        # 1단계: UI를 통한 정리
        try:
            existing_app = Application(backend='uia')
            existing_app.connect(title_re="COSFIM.*Web Service")
            
            # 프로세스 ID 수집
            pids = []
            for window in existing_app.windows():
                try:
                    pid = window.process_id()
                    pids.append(pid)
                    window.close()
                    self.logger.info(f"기존 창 닫기: PID {pid}")
                    time.sleep(0.5)
                except Exception as e:
                    self.logger.warning(f"기존 창 닫기 실패: {e}")
            
            # 프로세스 강제 종료
            for pid in set(pids):  # 중복 제거
                try:
                    subprocess.run(['taskkill', '/F', '/PID', str(pid)], 
                                 capture_output=True, timeout=5)
                    self.logger.info(f"기존 프로세스 강제 종료: PID {pid}")
                except Exception as e:
                    self.logger.warning(f"프로세스 종료 실패 (PID {pid}): {e}")
            
            time.sleep(2)
        except Exception as e:
            self.logger.info(f"UI 기반 정리 스킵 (기존 인스턴스 없음): {e}")
            
        # 2단계: 프로세스 이름으로 강제 정리
        try:
            result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq COSFIM_GUI.exe'], 
                                  capture_output=True, text=True, timeout=5)
            if 'COSFIM_GUI.exe' in result.stdout:
                self.logger.warning("⚠️ 남아있는 COSFIM_GUI.exe 발견! 강제 종료...")
                subprocess.run(['taskkill', '/F', '/IM', 'COSFIM_GUI.exe'], 
                             capture_output=True, timeout=5)
                self.logger.info("COSFIM_GUI.exe 강제 종료 완료")
                time.sleep(3)
            else:
                self.logger.info("남아있는 COSFIM_GUI.exe 프로세스 없음")
        except Exception as e:
            self.logger.warning(f"프로세스 이름 기반 정리 오류: {e}")
        
        # 3단계: 최종 확인
        for attempt in range(5):  # 최대 5초 대기
            try:
                result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq COSFIM_GUI.exe'], 
                                      capture_output=True, text=True, timeout=5)
                if 'COSFIM_GUI.exe' not in result.stdout:
                    self.logger.info(f"✅ 기존 프로세스 정리 완료 확인 ({attempt+1}초)")
                    time.sleep(1)  # 추가 안전 마진
                    return
            except:
                pass
            time.sleep(1)
        
        self.logger.warning("❌ 프로세스 정리 확인 시간 초과 - 계속 진행")



    def launch_app(self):
        """앱 실행 - 기존 인스턴스 정리 후 새로 시작"""
        try:
            # 기존 인스턴스 정리
            self.safe_close_existing_instances()
            
            # 새 인스턴스 시작
            self.app = Application(backend="uia").start(self.APP_PATH)          
            self.login_win = self.app.window(title_re="로그인")
            self.login_win.wait("visible", timeout=30)
            self.logger.info("새 COSFIM 인스턴스 실행 성공")
            self.is_new_instance = True
            
            self._login()
            self._update_check()
            
        except Exception as e:
            self.logger.error(f"앱 실행 실패: {e}")
            raise

    def _login(self):
        self.login_win.set_focus()
        time.sleep(self.WAIT_TIME_LONG_LONG)

        self.login_win.child_window(auto_id="textBox_ID", control_type="Edit").type_keys(self.user_id)
        self.login_win.child_window(auto_id="textBox_PWD", control_type="Edit").type_keys(self.user_pw)
        #login_box = self.login_win.child_window(auto_id="textBox_ID", control_type="Edit")
        #login_box.set_edit_text(self.user_id)

        #pwd_box = self.login_win.child_window(auto_id="textBox_PWD", control_type="Edit")
        #pwd_box.set_edit_text(self.user_pw)

        self.login_win.child_window(auto_id="button_Accept", control_type="Button").click_input()
        self.logger.info("로그인 성공")
        time.sleep(self.WAIT_TIME_LONG_LONG)
    
    def _update_check(self):
        try: 
            update_win = self.app.window(title_re="선택")
            update_win.wait("visible", timeout=5)
            update_win.child_window(auto_id="7", control_type="Button").click_input()
            self.logger.info("업데이트 요청 무시")
        except:
            self.logger.info("업데이트 요청 없음")
            return

    def get_elements(self):
        self.main_win = self._main_win()
        self._close_residue_windows()
        self.tool_bar, self.save_btn, self.load_btn = self._tool_bar()   
        self.water_system_box, self.dam_box, self.time_interval_box, self.time_picker_start = self._select_box()

    def _main_win(self):
        main_win = self.app.window(title_re="COSFIM.*Web Service", control_type="Window")
        main_win.wait("visible", timeout=10)
        self.logger.info("메인 창 로딩 완료")
        main_win.set_focus()
        time.sleep(self.WAIT_TIME)
        return main_win

    def _close_windows(self, window):
        """에러 무시하고 윈도우 제거"""
        with suppress(Exception):
            if window.exists():
                self.logger.info(f"윈도우 제거: {window.window_text()}")
                window.close()
            time.sleep(self.WAIT_TIME)

    def _close_residue_windows(self):
        """이전 실행에서 남은 윈도우가 있다면 제거"""
        self.logger.info("불필요한 윈도우 정리 시작...")
        data_output_wins = [
            self.app.window(auto_id="GraphForm", control_type="Window"),            
            self.app.window(auto_id="AnalysisForm", control_type="Window"),
            self.app.window(auto_id="DiagramSlideForm", control_type="Window")
        ]
        for window in data_output_wins:
            self._close_windows(window)

        self.main_win.set_focus()
        time.sleep(self.WAIT_TIME)
        error_wins = [
            self.main_win.child_window(title="선택", control_type="Window"),
            self.main_win.child_window(title="알림", control_type="Window"),
        ]
        for window in error_wins:            
            with suppress(Exception):
                if window.exists():
                    self.logger.info(f"윈도우 제거: {window.window_text()}")
                    window.child_window(title="아니요(N)", auto_id="7", control_type="Button").click_input()
                time.sleep(self.WAIT_TIME)

        self.logger.info("불필요한 윈도우 정리 완료")

    def _focus_main_win(self):
        self.main_win.set_focus()
        time.sleep(self.WAIT_TIME)

    def _tool_bar(self):
        tool_bar = self.main_win.child_window(auto_id="toolBar", control_type="ToolBar")
        save_btn = tool_bar.child_window(title="현재 모의를 저장 합니다.", control_type="SplitButton")
        load_btn = tool_bar.child_window(title="기존 모의를 읽어 옵니다.", control_type="SplitButton")
        return tool_bar, save_btn, load_btn

    def _select_box(self):
        water_system_box = self.main_win.child_window(auto_id="comboBox_waterSystem", control_type="ComboBox")
        dam_box = self.main_win.child_window(auto_id="comboBox_DamName", control_type="ComboBox")
        time_interval_box = self.main_win.child_window(auto_id="comboBox_TimeInterval", control_type="ComboBox")
        time_picker_start = self.main_win.child_window(auto_id="timePicker_Current", control_type="Pane")
        return water_system_box, dam_box, time_interval_box, time_picker_start

    def _select_water_system(self):
        self._focus_main_win()
        self.water_system_box.click_input()
        time.sleep(self.WAIT_TIME)
        self.water_system_box.child_window(title=self.water_system_name, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        self.logger.info(f"수계 선택 {self.water_system_name=}")
    
    def _select_dam(self):
        self._focus_main_win()
        self.dam_box.click_input()
        time.sleep(self.WAIT_TIME)
        self.dam_box.child_window(title=self.dam_name, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        self.logger.info(f"댐 선택 {self.dam_name=}")
    
    def _check_error_window(self):
        """에러 창을 확인하는 함수"""
        try:
            error_win = self.main_win.child_window(title="선택", control_type="Window")
            error_win.wait("visible", timeout=2)
            error_win.set_focus()
            time.sleep(self.WAIT_TIME)

            self.logger.info("OPT 파일 불러오기 중 에러 발생")
            
            error_title = "OPT 파일 불러오기 중 에러 발생"
            try:
                error_text_win = error_win.child_window(control_type="Text")
                error_title = error_text_win.window_text()
            except:
                pass 
            error_win.child_window(title="아니요(N)", control_type="Button").click_input()
            time.sleep(self.WAIT_TIME)
            raise ValueError(f"에러 창 발생: {error_title}")
        except ValueError as e:
            raise 
        except (ElementNotFoundError, TimeoutError):
            self.logger.info("에러 창 없음 - 정상 진행")
        except Exception as e:
            self.logger.error(f"에러 창 확인 중 예상치 못한 에러: {e}")

    def select_options(self):
        try: 
            self._focus_main_win()
            self._select_water_system()
            self._select_dam()
            self.load_btn.click_input()
            time.sleep(self.WAIT_TIME_LONG_LONG)
            self._check_error_window()
            self.logger.info("OPT 파일 불러오기 완료")
        except Exception as e:
            self.logger.error(f"옵션 선택 중 에러 발생: {e}")
            raise

    def _check_opt_file(self): 
        opt_name = self.opt_name_map[self.water_system_name][self.dam_name]
        opt_file_path = os.path.join(self.FILE_DIR, f"{opt_name}.OPT")
        self.logger.info(f"옵션 파일 경로 {opt_file_path=}")
        
        if not os.path.exists(opt_file_path):
            with open(opt_file_path, "w") as f:
                pass
            self.logger.info("OPT 파일 생성 완료")
        else:
            self.logger.info("OPT 파일 존재")
        return opt_file_path
        
    def _save_opt_file(self, opt_file_path, opt_data):
        if opt_data is None:
            self.logger.info("OPT 데이터가 제공되지 않음")
            return
        
        with open(opt_file_path, "w") as f:
            f.write(opt_data)
        self.logger.info("OPT 파일 적용 완료")

    def handle_opt_file(self):
        opt_file_path = self._check_opt_file()
        self._save_opt_file(opt_file_path, self.opt_data)

    def _get_data(self):
        self.main_win.set_focus()
        time.sleep(self.WAIT_TIME)
        keyboard.send_keys("{F5}")

        graph_win = self.app.window(auto_id="GraphForm", control_type="Window")
        graph_win.wait("visible", timeout=300)
        graph_win.set_focus()
        time.sleep(self.WAIT_TIME)
        table_tap = graph_win.child_window(auto_id="tabControl", control_type="Tab").child_window(title="테이블", control_type="TabItem")
        table_tap.click_input()
        time.sleep(self.WAIT_TIME)

        table_area = graph_win.child_window(title="테이블", auto_id="tabPage_Table", control_type="Pane")
        table_sheet = table_area.child_window(auto_id="sheet_DetailView", control_type="Pane")

        pywinauto.mouse.click(coords=(table_sheet.rectangle().left+2, table_sheet.rectangle().top+2))
        time.sleep(self.WAIT_TIME)

        for _ in range(8):
            pywinauto.keyboard.send_keys("+{RIGHT}")
            time.sleep(self.WAIT_TIME)
        
        pywinauto.keyboard.send_keys("^c")
        time.sleep(self.WAIT_TIME_LONG)
        table_data = pyperclip.paste()

        analysis_win = self.app.window(auto_id="AnalysisForm", control_type="Window")
        diagram_win = self.app.window(auto_id="DiagramSlideForm", control_type="Window")

        graph_win.close()
        analysis_win.close()
        diagram_win.close()
        time.sleep(self.WAIT_TIME_LONG)

        return table_data
    
    def _save_data(self, clipboard_data):
        try:
            data_io = StringIO(clipboard_data)
            df = pd.read_csv(data_io, sep='\t', encoding='utf-8')
            # .copy()를 사용하여 독립적인 DataFrame 생성
            filtered_df = df[["월일시분", "관측우량(mm)", "유효우량(mm)", "관측유입(㎥/s)", "계산유입(㎥/s)", "댐수위(El. m)", "총방류(㎥/s)"]].copy()
            filtered_df.rename(columns={
                "월일시분": "obsrdt", 
                "관측우량(mm)": "obsrf", 
                "유효우량(mm)": "effrf", 
                "관측유입(㎥/s)": "obsinflow", 
                "계산유입(㎥/s)": "calcinflow", 
                "댐수위(El. m)": "lowlevel", 
                "총방류(㎥/s)": "totdcwtrqy"
            }, inplace=True)

            filtered_df["obsrdt"] = filtered_df["obsrdt"].apply(lambda x: str(x).replace(" ", "").replace(":", "").replace("-", ""))
            
            # 작업별 CSV 파일 저장
            csv_path = self.work_dir / self.csv_filename
            filtered_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"데이터를 {csv_path} 파일로 저장했습니다.")
            return str(csv_path)

        except Exception as e:
            self.logger.error(f"데이터프레임 변환 중 오류 발생: {e}")
            raise

    def handle_data(self):
        try:
            clipboard_data = self._get_data()
            csv_path = self._save_data(clipboard_data)
            self.forwarder.forward(success=True, data_path=csv_path)
            return csv_path
        except TimeoutError as e:
            raise
        except Exception as e:
            self.logger.error(f"데이터 처리 중 에러 발생: {e}")
            raise

#    def cleanup(self):
#        """리소스 정리"""
#        try:
#            if self.app:
#                for window in self.app.windows():
#                    try:
#                        window.close()
#                    except:
#                        pass
#            self.logger.info("앱 정리 완료")
#        except Exception as e:
#            self.logger.error(f"정리 중 에러: {e}")

    def cleanup(self):
        """리소스 정리 - 강제 종료 포함"""
        pids = []
        
        try:
            # 1단계: 앱 객체를 통한 정리
            if self.app:
                self.logger.info("앱 객체를 통한 프로세스 정리 시작...")
                for window in self.app.windows():
                    try:
                        pid = window.process_id()
                        pids.append(pid)
                        window.close()
                        self.logger.info(f"창 닫기 완료: PID {pid}")
                    except Exception as e:
                        self.logger.warning(f"창 닫기 실패: {e}")
                
                # 수집된 PID 강제 종료
                for pid in set(pids):
                    try:
                        subprocess.run(['taskkill', '/F', '/PID', str(pid)], 
                                     capture_output=True, timeout=5)
                        self.logger.info(f"프로세스 강제 종료 완료: PID {pid}")
                    except Exception as e:
                        self.logger.warning(f"프로세스 강제 종료 실패 (PID {pid}): {e}")
                
                time.sleep(1)
                
        except Exception as e:
            self.logger.error(f"앱 객체 정리 중 에러: {e}")
        
        # 2단계: 프로세스 이름으로 강제 정리 (안전장치)
        try:
            self.logger.info("프로세스 이름 기반 정리 시작...")
            result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq COSFIM_GUI.exe'], 
                                  capture_output=True, text=True, timeout=5)
            
            if 'COSFIM_GUI.exe' in result.stdout:
                self.logger.warning("⚠️ 남아있는 COSFIM_GUI.exe 프로세스 발견! 강제 종료 시도...")
                subprocess.run(['taskkill', '/F', '/IM', 'COSFIM_GUI.exe'], 
                             capture_output=True, timeout=5)
                self.logger.info("COSFIM_GUI.exe 프로세스 강제 종료 완료")
                time.sleep(2)
            else:
                self.logger.info("남아있는 COSFIM_GUI.exe 프로세스 없음")
                
        except Exception as e:
            self.logger.error(f"프로세스 이름 기반 정리 중 에러: {e}")
        
        # 3단계: 최종 확인 및 대기
        try:
            result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq COSFIM_GUI.exe'], 
                                  capture_output=True, text=True, timeout=5)
            if 'COSFIM_GUI.exe' not in result.stdout:
                self.logger.info("✅ 모든 COSFIM 프로세스 정리 완료")
            else:
                self.logger.error("❌ 일부 COSFIM 프로세스가 여전히 남아있을 수 있습니다")
        except:
            pass
        
        time.sleep(2)  # 프로세스 완전 종료 대기

    def process(self):
        """전체 처리 프로세스"""
        try:
            if self.opt_data is None or self.opt_data == "":
                raise ValueError("옵션 데이터가 제공되지 않았습니다")

            self.launch_app()
            self.logger.info("===런치 완료===")

            self.get_elements()
            self.logger.info("===요소 처리 완료===")

            self.handle_opt_file()
            self.logger.info("===옵션 처리 완료===")

            self.select_options()
            self.logger.info("===항목 선택 완료===")

            csv_path = self.handle_data()
            self.logger.info("===데이터 처리 완료===")
            
            return csv_path

        except Exception as e:
            self.logger.error(f"처리 중 에러 발생: {e}", exc_info=True)
            # 에러 포워딩 시도 (실패해도 cleanup은 실행되도록)
            try:
                self.forwarder.forward(success=False, err_msg=str(e))
            except Exception as forward_err:
                self.logger.error(f"에러 보고 포워딩 실패: {forward_err}")
            raise
        finally:
            self.cleanup()


class MultiCosfimManager:
    """다중 COSFIM 작업 관리자"""
    def __init__(self):
        self.task_queue = TaskQueue()
        self.results = []
        
    def add_dam_task(self, water_system_name, dam_name, dam_code, template_id, 
                     user_id, user_pw, opt_data, api_end_point, session_id, widget_name):
        """댐 작업 추가"""
        task_data = {
            'water_system_name': water_system_name,
            'dam_name': dam_name,
            'dam_code': dam_code,
            'template_id': template_id,
            'user_id': user_id,
            'user_pw': user_pw,
            'opt_data': opt_data,
            'api_end_point': api_end_point,
            'session_id' : session_id,
            'widget_name' : widget_name
        }
        
        task_id = self.task_queue.add_task(task_data)
        logging.info(f"Dam task added: {dam_name} (Task ID: {task_id})")
        return task_id
    
    def start_processing(self):
        """처리 시작"""
        self.task_queue.start_worker()
        logging.info("Multi-COSFIM processing started")
    
    def stop_processing(self):
        """처리 중지"""
        self.task_queue.stop_worker()
        logging.info("Multi-COSFIM processing stopped")
    
    def wait_for_completion(self, timeout=None):
        """모든 작업 완료 대기"""
        start_time = time.time()
        while not self.task_queue.task_queue.empty():
            if timeout and (time.time() - start_time) > timeout:
                logging.warning("작업 완료 대기 시간 초과")
                break
            time.sleep(1)
        
        # 최종 결과 수집
        final_results = self.task_queue.get_results()
        self.results.extend(final_results)
        return self.results
    
    def get_status(self):
        """현재 상태 조회"""
        return {
            'queue_size': self.task_queue.task_queue.qsize(),
            'is_running': self.task_queue.is_running,
            'completed_tasks': len(self.results),
            'recent_results': self.task_queue.get_results()
        }
    
    def save_results_to_file(self, filename="multi_cosfim_results.json"):
        """결과를 파일로 저장"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False, default=str)
            logging.info(f"결과가 {filename}에 저장되었습니다.")
        except Exception as e:
            logging.error(f"결과 저장 중 에러: {e}")


def create_sample_tasks():
    """샘플 작업들을 생성하는 함수"""
    
    # 기본 설정
    api_end_point = "http://223.130.139.28/api/v1/widget/upload/cosfim"
    user_id = "20052970"
    user_pw = "20052970"
    
    # 샘플 OPT 데이터
    sample_opt_data = """Y 2025 080513 081313 N  
N
1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00
1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00
1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00
1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00
1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00
2025 08 05 13 00
2025 08 06 13 00
1
24 30
0.000 0.000 0.000 0.000 0.000 1.000 1.000
1 0 0 3 2025 8 6 13 0
1
1
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.000 
0.0 
"""
    
    # 여러 댐 작업 정의
    dam_tasks = [
        {
            'water_system_name': "낙동강",
            'dam_name': "합천댐",
            'dam_code': "2015110",
            'template_id': "cbc038d8-87f6-4e81-a794-4b9945bc6a1a",
            'opt_data': sample_opt_data
        },
        {
            'water_system_name': "낙동강", 
            'dam_name': "안동댐",
            'dam_code': "2015111",
            'template_id': "cbc038d8-87f6-4e81-a794-4b9945bc6a1b",
            'opt_data': sample_opt_data.replace("2025 08 05", "2025 08 06")  # 약간 다른 날짜
        },
        {
            'water_system_name': "태화강",
            'dam_name': "대곡댐", 
            'dam_code': "2015112",
            'template_id': "cbc038d8-87f6-4e81-a794-4b9945bc6a1c",
            'opt_data': sample_opt_data.replace("합천댐", "대곡댐")
        }
    ]
    
    return dam_tasks, api_end_point, user_id, user_pw


def main():
    """메인 실행 함수"""
    try:
        # 관리자 생성
        manager = MultiCosfimManager()
        
        # 샘플 작업들 생성
        dam_tasks, api_end_point, user_id, user_pw = create_sample_tasks()
        
        # 작업들을 큐에 추가
        task_ids = []
        for task in dam_tasks:
            task_id = manager.add_dam_task(
                water_system_name=task['water_system_name'],
                dam_name=task['dam_name'],
                dam_code=task['dam_code'],
                template_id=task['template_id'],
                user_id=user_id,
                user_pw=user_pw,
                opt_data=task['opt_data'],
                api_end_point=api_end_point,
                session_id = task['session_id'],
                widget_name = task['widget_name']
            )
            task_ids.append(task_id)
        
        logging.info(f"총 {len(dam_tasks)}개 작업이 큐에 추가되었습니다.")
        
        # 처리 시작
        manager.start_processing()
        
        # 진행상황 모니터링
        try:
            while manager.task_queue.is_running and not manager.task_queue.task_queue.empty():
                status = manager.get_status()
                logging.info(f"진행 상황 - 대기 중: {status['queue_size']}, 완료: {status['completed_tasks']}")
                
                # 최근 완료된 결과 확인
                recent_results = status['recent_results']
                for result in recent_results:
                    if result['success']:
                        logging.info(f"✅ 작업 완료: {result.get('message', 'Unknown task')}")
                    else:
                        logging.error(f"❌ 작업 실패: {result.get('error', 'Unknown error')}")
                
                time.sleep(10)  # 10초마다 상태 확인
                
        except KeyboardInterrupt:
            logging.info("사용자에 의한 중단 요청")
            manager.stop_processing()
        
        # 최종 결과 수집
        final_results = manager.wait_for_completion(timeout=300)  # 5분 대기
        
        # 결과 요약
        successful_tasks = [r for r in final_results if r.get('success', False)]
        failed_tasks = [r for r in final_results if not r.get('success', True)]
        
        logging.info("=" * 50)
        logging.info("최종 결과 요약")
        logging.info(f"성공한 작업: {len(successful_tasks)}")
        logging.info(f"실패한 작업: {len(failed_tasks)}")
        
        if successful_tasks:
            logging.info("성공한 작업들:")
            for task in successful_tasks:
                logging.info(f"  - {task.get('message', 'Unknown')}")
        
        if failed_tasks:
            logging.info("실패한 작업들:")
            for task in failed_tasks:
                logging.info(f"  - {task.get('error', 'Unknown error')}")
        
        # 결과를 파일로 저장
        manager.save_results_to_file()
        
        logging.info("=" * 50)
        
    except Exception as e:
        logging.error(f"메인 실행 중 에러 발생: {e}")
        return 1
    
    finally:
        try:
            manager.stop_processing()
        except:
            pass
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logging.info("프로그램이 중단되었습니다.")
        sys.exit(130)
    except Exception as e:
        logging.error(f"예상치 못한 에러: {e}")
        sys.exit(1)
