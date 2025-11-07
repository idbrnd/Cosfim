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
import win32gui
import win32con
import win32api
import pywinauto.keyboard as keyboard

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
        logging.info("워커 루프 시작 - 작업 대기 중...")
        while self.is_running:
            try:
                task = self.task_queue.get(timeout=1)
                if task is None:  # 종료 신호
                    logging.info("종료 신호 수신, 워커 루프 종료")
                    break
                
                logging.info(f"Processing task: {task['id']}")
                result = self._process_task(task)
                self.result_queue.put(result)
                self.task_queue.task_done()
                
                # 작업 간 간격 (GUI 안정화)
                time.sleep(2)
                logging.info("작업 완료, 다음 작업 대기 중...")
                
            except queue.Empty:
                # 대기 중임을 알리는 로그 (30초마다 한 번씩만)
                if not hasattr(self, '_last_waiting_log'):
                    self._last_waiting_log = time.time()
                if time.time() - self._last_waiting_log > 30:
                    logging.info("워커 대기 중... (큐가 비어있음)")
                    self._last_waiting_log = time.time()
                continue
            except Exception as e:
                logging.error(f"Error in worker loop: {e}")
                error_result = {
                    'task_id': task.get('id', 'unknown') if 'task' in locals() else 'unknown',
                    'success': False,
                    'error': str(e)
                }
                self.result_queue.put(error_result)
        
        logging.info("워커 루프 종료됨")
    
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
                task_data['session_id']
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
    def __init__(self, end_point, water_system_name, dam_name, dam_code, template_id, session_id):
        self.end_point = end_point
        self.dam_name = dam_name
        self.water_system_name = water_system_name
        self.dam_code = dam_code
        self.template_id = template_id
        self.session_id = session_id

    def forward(self, success=True, data_path="table_data.csv", err_msg=""):
        files = None
        try: 
            info_data = {
                "damName": self.dam_name,
                "waterSystemName": self.water_system_name, 
                "damCode": self.dam_code,
                "error": ""
            }
            query_params = {
                "templateId": self.template_id,
                "sessionId" : self.session_id
            }
            
            if success:
                message = "success"
                with open(data_path, "rb") as csv_file:
                    files = {"file": (data_path, csv_file, "text/csv")}
                    response = requests.post(
                        self.end_point, 
                        files=files, 
                        data=info_data,
                        params=query_params
                    )
            else:
                info_data["error"] = err_msg if err_msg else "unknown error"
                response = requests.post(
                        self.end_point, 
                        files=None, 
                        data=info_data,
                        params=query_params
                    )

            logging.info(f"Response status: {response.status_code}")

            if response.status_code == 200:
                logging.info("서버 응답: " + response.text)
            else:
                logging.info(f"서버 응답 실패: {response.status_code}, {response.text}")

            logging.info(f"포워딩 완료: 성공 여부: {success}")
        except Exception as e:
            logging.info(f"포워딩 실패:{e}")


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

    def safe_close_existing_instances(self):
        """기존 COSFIM 인스턴스를 안전하게 종료"""
        import subprocess
        
        try:
            existing_app = Application(backend='uia')
            existing_app.connect(title_re="COSFIM.*Web Service")
            
            # 프로세스 ID 수집
            pids = []
            for window in existing_app.windows():
                try:
                    pids.append(window.process_id())
                    window.close()
                    time.sleep(0.5)
                except:
                    pass
            
            # 프로세스 강제 종료
            for pid in set(pids):  # 중복 제거
                try:
                    subprocess.run(['taskkill', '/F', '/PID', str(pid)], 
                                 capture_output=True, timeout=5)
                    self.logger.info(f"프로세스 강제 종료: PID {pid}")
                except Exception as e:
                    self.logger.warning(f"프로세스 종료 실패 (PID {pid}): {e}")
            
            self.logger.info("기존 COSFIM 인스턴스 정리 완료")
            time.sleep(3)  # 프로세스 완전 종료 대기
        except Exception as e:
            self.logger.info(f"기존 COSFIM 인스턴스 없음 또는 정리 실패: {e}")
            
        # 프로세스 이름으로도 확인 및 종료
        try:
            result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq COSFIM_GUI.exe'], 
                                  capture_output=True, text=True, timeout=5)
            if 'COSFIM_GUI.exe' in result.stdout:
                subprocess.run(['taskkill', '/F', '/IM', 'COSFIM_GUI.exe'], 
                             capture_output=True, timeout=5)
                self.logger.info("COSFIM_GUI.exe 프로세스 강제 종료")
                time.sleep(2)
        except Exception as e:
            self.logger.info(f"프로세스 이름 기반 정리 스킵: {e}")

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
        # set_focus 제거 (세션 없는 환경에서 블로킹됨)
        time.sleep(self.WAIT_TIME_LONG_LONG)

        # Windows 서버에서 세션이 없을 때를 위해 set_edit_text 사용
        login_box = self.login_win.child_window(auto_id="textBox_ID", control_type="Edit")
        login_box.set_edit_text(self.user_id)
        
        pwd_box = self.login_win.child_window(auto_id="textBox_PWD", control_type="Edit")
        pwd_box.set_edit_text(self.user_pw)
        
        login_btn = self.login_win.child_window(auto_id="button_Accept", control_type="Button")
        self._click_button(login_btn, "로그인 버튼")
        self.logger.info("로그인 성공")
        time.sleep(self.WAIT_TIME_LONG_LONG)
    
    def _update_check(self):
        try: 
            update_win = self.app.window(title_re="선택")
            update_win.wait("visible", timeout=5)
            update_btn = update_win.child_window(auto_id="7", control_type="Button")
            self._click_button(update_btn, "업데이트 무시 버튼")
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
        # set_focus 제거 (세션 없는 환경에서 블로킹됨)
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

        # set_focus 제거 (세션 없는 환경에서 블로킹됨)
        time.sleep(self.WAIT_TIME)
        error_wins = [
            self.main_win.child_window(title="선택", control_type="Window"),
            self.main_win.child_window(title="알림", control_type="Window"),
        ]
        for window in error_wins:            
            with suppress(Exception):
                if window.exists():
                    self.logger.info(f"윈도우 제거: {window.window_text()}")
                    no_btn = window.child_window(title="아니요(N)", auto_id="7", control_type="Button")
                    self._click_button(no_btn, "아니요 버튼")
                time.sleep(self.WAIT_TIME)

        self.logger.info("불필요한 윈도우 정리 완료")

    def _focus_main_win(self):
        # set_focus 제거 (세션 없는 환경에서 블로킹됨)
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

    def _select_combobox_item(self, combobox, item_text):
        """ComboBox 항목 선택 (세션 불필요한 방법들 시도)"""
        try:
            # 방법 1: Windows 메시지로 직접 항목 선택
            hwnd = combobox.handle
            # CB_FINDSTRINGEXACT로 항목 인덱스 찾기
            index = win32gui.SendMessage(hwnd, win32con.CB_FINDSTRINGEXACT, -1, item_text)
            if index >= 0:
                # CB_SETCURSEL로 선택
                win32gui.SendMessage(hwnd, win32con.CB_SETCURSEL, index, 0)
                # CBN_SELCHANGE 알림 전송
                parent_hwnd = win32gui.GetParent(hwnd)
                win32gui.SendMessage(parent_hwnd, win32con.WM_COMMAND, 
                                   (win32con.CBN_SELCHANGE << 16) | win32gui.GetDlgCtrlID(hwnd), hwnd)
                self.logger.info(f"ComboBox 항목 선택 성공: {item_text}")
                return True
        except Exception as e:
            self.logger.warning(f"Windows 메시지 방법 실패: {e}")
        
        try:
            # 방법 2: set_text() 메서드 시도
            combobox.set_text(item_text)
            self.logger.info(f"set_text() 방법으로 선택 성공: {item_text}")
            return True
        except Exception as e:
            self.logger.warning(f"set_text() 방법 실패: {e}")
        
        raise RuntimeError(f"ComboBox 항목 선택 실패: {item_text}")
    
    def _send_key(self, window, vk_code, ctrl=False, shift=False, alt=False):
        """Windows 메시지로 키 전송 (세션 불필요)"""
       
        try:
            hwnd = window.handle
            
            # 스캔 코드 가져오기
            scan_code = win32api.MapVirtualKey(vk_code, 0)
            
            # lParam 생성: repeat_count(1) | scan_code | extended_flag | context_code | previous_state | transition_state
            # Bit 0-15: repeat count (1)
            # Bit 16-23: scan code
            # Bit 24: extended key flag (0 for most keys, 1 for extended keys like arrows, home, end)
            # Bit 29: context code (Alt key)
            # Bit 30: previous key state
            # Bit 31: transition state
            
            # Extended keys: arrows, home, end, page up/down, insert, delete
            extended_keys = [0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x2D, 0x2E]  # PgUp, PgDn, End, Home, Left, Up, Right, Down, Insert, Delete
            is_extended = vk_code in extended_keys
            
            # 수정자 키 상태 설정
            modifier_flags = 0
            if ctrl:
                modifier_flags |= 0x20000000  # Context code for Ctrl
            if alt:
                modifier_flags |= 0x20000000  # Context code for Alt
            
            # KEYDOWN lParam: repeat=1 | scan_code | extended_flag
            lparam_down = 1 | (scan_code << 16) | (1 << 24 if is_extended else 0) | modifier_flags
            # KEYUP lParam: repeat=1 | scan_code | extended_flag | previous_state=1 | transition_state=1
            lparam_up = 1 | (scan_code << 16) | (1 << 24 if is_extended else 0) | (1 << 30) | (1 << 31) | modifier_flags
            
            # 수정자 키 눌림
            if ctrl:
                ctrl_scan = win32api.MapVirtualKey(win32con.VK_CONTROL, 0)
                ctrl_lparam_down = 1 | (ctrl_scan << 16)
                win32gui.SendMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_CONTROL, ctrl_lparam_down)
            if shift:
                shift_scan = win32api.MapVirtualKey(win32con.VK_SHIFT, 0)
                shift_lparam_down = 1 | (shift_scan << 16)
                win32gui.SendMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_SHIFT, shift_lparam_down)
            if alt:
                alt_scan = win32api.MapVirtualKey(win32con.VK_MENU, 0)
                alt_lparam_down = 1 | (alt_scan << 16) | (1 << 29)
                win32gui.SendMessage(hwnd, win32con.WM_KEYDOWN, win32con.VK_MENU, alt_lparam_down)
            
            # 메인 키 눌림 및 떼기
            win32gui.SendMessage(hwnd, win32con.WM_KEYDOWN, vk_code, lparam_down)
            win32gui.SendMessage(hwnd, win32con.WM_KEYUP, vk_code, lparam_up)
            
            # 수정자 키 떼기
            if alt:
                alt_scan = win32api.MapVirtualKey(win32con.VK_MENU, 0)
                alt_lparam_up = 1 | (alt_scan << 16) | (1 << 29) | (1 << 30) | (1 << 31)
                win32gui.SendMessage(hwnd, win32con.WM_KEYUP, win32con.VK_MENU, alt_lparam_up)
            if shift:
                shift_scan = win32api.MapVirtualKey(win32con.VK_SHIFT, 0)
                shift_lparam_up = 1 | (shift_scan << 16) | (1 << 30) | (1 << 31)
                win32gui.SendMessage(hwnd, win32con.WM_KEYUP, win32con.VK_SHIFT, shift_lparam_up)
            if ctrl:
                ctrl_scan = win32api.MapVirtualKey(win32con.VK_CONTROL, 0)
                ctrl_lparam_up = 1 | (ctrl_scan << 16) | (1 << 30) | (1 << 31)
                win32gui.SendMessage(hwnd, win32con.WM_KEYUP, win32con.VK_CONTROL, ctrl_lparam_up)
            
            self.logger.info(f"키 전송 성공: VK_{vk_code} (scan={scan_code}, extended={is_extended})")
            return True
        except Exception as e:
            self.logger.error(f"키 전송 실패: {e}")
            return False
    
    def _select_water_system(self):
        self._focus_main_win()
        self._select_combobox_item(self.water_system_box, self.water_system_name)
        time.sleep(self.WAIT_TIME_LONG)
        self.logger.info(f"수계 선택 {self.water_system_name=}")
    
    def _select_dam(self):
        self._focus_main_win()
        self._select_combobox_item(self.dam_box, self.dam_name)
        time.sleep(self.WAIT_TIME_LONG)
        self.logger.info(f"댐 선택 {self.dam_name=}")
    
    def _check_error_window(self):
        """에러 창을 확인하는 함수"""
        try:
            error_win = self.main_win.child_window(title="선택", control_type="Window")
            error_win.wait("visible", timeout=2)
            # set_focus 제거 (세션 없는 환경에서 블로킹됨)
            time.sleep(self.WAIT_TIME)

            self.logger.info("OPT 파일 불러오기 중 에러 발생")
            
            error_title = "OPT 파일 불러오기 중 에러 발생"
            try:
                error_text_win = error_win.child_window(control_type="Text")
                error_title = error_text_win.window_text()
            except:
                pass 
            no_btn = error_win.child_window(title="아니요(N)", control_type="Button")
            self._click_button(no_btn, "에러 창 아니요 버튼")
            time.sleep(self.WAIT_TIME)
            raise ValueError(f"에러 창 발생: {error_title}")
        except ValueError as e:
            raise 
        except (ElementNotFoundError, TimeoutError):
            self.logger.info("에러 창 없음 - 정상 진행")
        except Exception as e:
            self.logger.error(f"에러 창 확인 중 예상치 못한 에러: {e}")

    def _click_button(self, button, button_name="버튼"):
        """버튼 클릭 (세션 불필요한 방법 시도)"""
        try:
            # 방법 1: invoke() 메서드 (SplitButton 등 지원)
            button.invoke()
            self.logger.info(f"{button_name} invoke() 성공")
            return True
        except Exception as e:
            self.logger.warning(f"{button_name} invoke() 실패: {e}")
        
        try:
            # 방법 2: Windows 메시지로 직접 클릭
            hwnd = button.handle
            win32gui.SendMessage(hwnd, win32con.BM_CLICK, 0, 0)
            self.logger.info(f"{button_name} BM_CLICK 성공")
            return True
        except Exception as e:
            self.logger.warning(f"{button_name} BM_CLICK 실패: {e}")
        
        try:
            # 방법 3: click() 메서드 (일반 버튼)
            button.click()
            self.logger.info(f"{button_name} click() 성공")
            return True
        except Exception as e:
            self.logger.warning(f"{button_name} click() 실패: {e}")
        
        raise RuntimeError(f"{button_name} 클릭 실패")
    
    def select_options(self):
        try: 
            self._focus_main_win()
            self._select_water_system()
            self._select_dam()
            self._click_button(self.load_btn, "불러오기 버튼")
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
        # set_focus 제거 (세션 없는 환경에서 블로킹됨)
        time.sleep(self.WAIT_TIME)
        
        # F5 키 전송 (VK_F5 = 0x74)
        self._send_key(self.main_win, 0x74)
        self.logger.info("F5 키 전송 완료")
        
        graph_win = self.app.window(auto_id="GraphForm", control_type="Window")
        graph_win.wait("visible", timeout=300)
        # set_focus 제거 (세션 없는 환경에서 블로킹됨)
        time.sleep(self.WAIT_TIME)
        
        # 테이블 탭 선택
        tab_control = graph_win.child_window(auto_id="tabControl", control_type="Tab")
        table_tap = tab_control.child_window(title="테이블", control_type="TabItem")
        
        # 테이블 탭 선택 (여러 방법 시도)
        self.logger.info("테이블 탭 선택 시도")
        tab_selected = False
        
        # 방법 1: UIA SelectionItemPattern 사용
        try:
            table_tap.select()
            time.sleep(self.WAIT_TIME_LONG)
            # 테이블 영역이 나타났는지 확인
            test_area = graph_win.child_window(auto_id="tabPage_Table", control_type="Pane")
            if test_area.exists(timeout=2):
                self.logger.info("테이블 탭 선택 성공 (select() 메서드)")
                tab_selected = True
        except Exception as e:
            self.logger.warning(f"select() 메서드 실패: {e}")
        
        # 방법 2: 탭 아이템 직접 클릭
        if not tab_selected:
            try:
                hwnd = table_tap.handle
                rect = table_tap.rectangle()
                
                # 탭 중앙 좌표 계산
                center_x = (rect.right - rect.left) // 2
                center_y = (rect.bottom - rect.top) // 2
                
                # WM_LBUTTONDOWN/UP으로 클릭
                lParam = (center_y << 16) | (center_x & 0xFFFF)
                win32gui.SendMessage(hwnd, win32con.WM_LBUTTONDOWN, win32con.MK_LBUTTON, lParam)
                time.sleep(0.1)
                win32gui.SendMessage(hwnd, win32con.WM_LBUTTONUP, 0, lParam)
                time.sleep(self.WAIT_TIME_LONG)
                
                # 테이블 영역이 나타났는지 확인
                test_area = graph_win.child_window(auto_id="tabPage_Table", control_type="Pane")
                if test_area.exists(timeout=2):
                    self.logger.info(f"테이블 탭 클릭 성공 (WM_LBUTTONDOWN/UP, 좌표: {center_x}, {center_y})")
                    tab_selected = True
            except Exception as e:
                self.logger.warning(f"직접 클릭 실패: {e}")
        
        # 방법 3: _click_button() 사용
        if not tab_selected:
            self.logger.info("테이블 탭 _click_button() 시도")
            self._click_button(table_tap, "테이블 탭")
            time.sleep(self.WAIT_TIME_LONG)
        
        time.sleep(self.WAIT_TIME)  # 추가 대기
        
        # 테이블 영역이 나타날 때까지 대기
        table_area = None
        for attempt in range(3):  # 최대 3번 시도
            try:
                table_area = graph_win.child_window(title="테이블", auto_id="tabPage_Table", control_type="Pane")
                table_area.wait("visible", timeout=5)
                self.logger.info("테이블 영역 찾기 성공 (title + auto_id)")
                break
            except:
                try:
                    # auto_id만으로 찾기 시도
                    table_area = graph_win.child_window(auto_id="tabPage_Table", control_type="Pane")
                    table_area.wait("visible", timeout=5)
                    self.logger.info("테이블 영역 찾기 성공 (auto_id)")
                    break
                except:
                    if attempt < 2:
                        self.logger.warning(f"테이블 영역 찾기 실패 (시도 {attempt + 1}/3), 탭 재클릭 시도")
                        # 탭 다시 클릭
                        try:
                            table_tap.select()
                        except:
                            self._click_button(table_tap, "테이블 탭 재시도")
                        time.sleep(self.WAIT_TIME_LONG)
                    else:
                        self.logger.error("테이블 영역을 찾을 수 없습니다")
                        raise ValueError("테이블 탭 선택 후 테이블 영역이 나타나지 않습니다. 수동으로 테이블 탭을 확인하세요.")
        
        if not table_area:
            raise ValueError("테이블 영역을 찾을 수 없습니다")
        
        # 테이블 시트 찾기
        try:
            table_sheet = table_area.child_window(auto_id="sheet_DetailView", control_type="Pane")
            table_sheet.wait("visible", timeout=10)
        except:
            # 다른 방법으로 찾기
            table_sheet = graph_win.child_window(auto_id="sheet_DetailView", control_type="Pane")
            table_sheet.wait("visible", timeout=10)
        
        self.logger.info("테이블 시트 찾기 성공")
        
        # 데이터 복사 시도 (1단계 → 2단계 → 3단계)
        table_data = None
        validated = False
        required_any = [
            "월일시분", "일시", "날짜시간", "DateTime",
            "관측우량(mm)", "관측우량", "Observed Rainfall",
            "유효우량(mm)", "유효우량", "Effective Rainfall",
            "관측유입(㎥/s)", "관측유입", "Observed Inflow",
            "계산유입(㎥/s)", "계산유입", "Calculated Inflow",
            "댐수위(El. m)", "댐수위", "Dam Level",
            "총방류(㎥/s)", "총방류", "Total Release"
        ]
        
        # 잘못된 데이터 필터링용 (코드 에디터 등에서 복사된 것 감지)
        invalid_keywords = ["import ", "def ", "class ", "from ", "return ", "if ", "for ", "while "]
        
        # ========== 1단계: Windows 메시지 방식 (Shift+End) ==========
        self.logger.info("=" * 60)
        self.logger.info("=== 1단계 시도: Windows 메시지 - Shift+End ===")
        
        # 클립보드 초기화
        try:
            pyperclip.copy("")
            self.logger.info("클립보드 초기화 완료")
        except Exception as e:
            self.logger.warning(f"클립보드 초기화 실패: {e}")
        
        time.sleep(0.2)
        
        # 윈도우 강제 활성화 (세션 있을 때만 작동, 없으면 스킵)
        try:
            graph_hwnd = graph_win.handle
            win32gui.SetForegroundWindow(graph_hwnd)
            self.logger.info("Graph 윈도우 강제 활성화 (세션 필요)")
            time.sleep(0.3)
        except Exception as e:
            self.logger.info(f"SetForegroundWindow 스킵 (세션 없음): {e}")
        
        # set_focus 제거 (세션 없는 환경에서 블로킹됨)
        time.sleep(0.5)
        
        # 테이블 클릭
        try:
            hwnd = table_sheet.handle
            x = 10
            y = 10
            lParam = (y << 16) | (x & 0xFFFF)
            win32gui.SendMessage(hwnd, win32con.WM_LBUTTONDOWN, win32con.MK_LBUTTON, lParam)
            time.sleep(0.1)
            win32gui.SendMessage(hwnd, win32con.WM_LBUTTONUP, 0, lParam)
            time.sleep(0.3)
            self.logger.info("테이블 클릭 완료")
        except Exception as e:
            self.logger.warning(f"테이블 클릭 실패: {e}")
        
        # Shift+End
        self._send_key(table_sheet, 0x23, shift=True)  # VK_END = 0x23
        time.sleep(0.5)
        self.logger.info("Shift+End 전송 완료")
        
        # 복사 (WM_COPY 또는 Ctrl+C)
        try:
            win32gui.SendMessage(hwnd, 0x301, 0, 0)  # WM_COPY
            time.sleep(0.3)
            self.logger.info("WM_COPY 메시지 전송")
        except:
            self._send_key(table_sheet, 0x43, ctrl=True)  # Ctrl+C
            time.sleep(0.3)
            self.logger.info("Ctrl+C 키 메시지 전송")
        
        time.sleep(self.WAIT_TIME_LONG)
        
        # 검증
        table_data = pyperclip.paste()
        size = len(table_data) if table_data else 0
        self.logger.info(f"1단계 클립보드 데이터 길이: {size} bytes")
        
        # 잘못된 데이터 필터링
        is_valid_data = True
        if table_data:
            first_line = table_data.split('\n')[0] if '\n' in table_data else table_data[:100]
            for keyword in invalid_keywords:
                if keyword in first_line:
                    self.logger.warning(f"❌ 잘못된 데이터 감지 (키워드: '{keyword.strip()}')")
                    is_valid_data = False
                    break
        
        if is_valid_data and size >= 10:
            try:
                data_io = StringIO(table_data)
                df_try = pd.read_csv(data_io, sep='\t', nrows=1, encoding='utf-8')
                cols = [str(c) for c in df_try.columns]
                self.logger.info(f"1단계 헤더 감지: {cols}")
                if any(col in cols for col in required_any):
                    self.logger.info("✅ 1단계 성공 (Shift+End)")
                    validated = True
                else:
                    self.logger.warning("❌ 1단계 실패: 예상 컬럼명 없음")
            except Exception as e:
                self.logger.warning(f"❌ 1단계 실패: {e}")
        else:
            self.logger.warning("❌ 1단계 실패: 데이터 크기 부족 또는 잘못된 데이터")
        
        self.logger.info("=" * 60)
        
        # ========== 2단계: Windows 메시지 방식 (Shift+End + Shift+Right) ==========
        if not validated:
            self.logger.info("=" * 60)
            self.logger.info("=== 2단계 시도: Windows 메시지 - Shift+Right 추가 ===")
            
            # 클립보드 초기화
            pyperclip.copy("")
            time.sleep(0.2)
            
            # set_focus 제거 (세션 없는 환경에서 블로킹됨)
            time.sleep(0.5)
            
            # 테이블 클릭
            try:
                lParam = (y << 16) | (x & 0xFFFF)
                win32gui.SendMessage(hwnd, win32con.WM_LBUTTONDOWN, win32con.MK_LBUTTON, lParam)
                time.sleep(0.1)
                win32gui.SendMessage(hwnd, win32con.WM_LBUTTONUP, 0, lParam)
                time.sleep(0.3)
            except Exception as e:
                self.logger.warning(f"테이블 클릭 실패: {e}")
            
            # Shift+End
            self._send_key(table_sheet, 0x23, shift=True)
            time.sleep(0.5)
            
            # Shift+Right 10번
            for i in range(10):
                self._send_key(table_sheet, 0x27, shift=True)  # VK_RIGHT = 0x27
                time.sleep(0.05)
            time.sleep(0.3)
            self.logger.info("Shift+End + Shift+Right 10번 전송 완료")
            
            # 복사
            try:
                win32gui.SendMessage(hwnd, 0x301, 0, 0)
                time.sleep(0.3)
            except:
                self._send_key(table_sheet, 0x43, ctrl=True)
                time.sleep(0.3)
            
            time.sleep(self.WAIT_TIME_LONG)
            
            # 검증
            table_data = pyperclip.paste()
            size = len(table_data) if table_data else 0
            self.logger.info(f"2단계 클립보드 데이터 길이: {size} bytes")
            
            is_valid_data = True
            if table_data:
                first_line = table_data.split('\n')[0] if '\n' in table_data else table_data[:100]
                for keyword in invalid_keywords:
                    if keyword in first_line:
                        self.logger.warning(f"❌ 잘못된 데이터 감지")
                        is_valid_data = False
                        break
            
            if is_valid_data and size >= 10:
                try:
                    data_io = StringIO(table_data)
                    df_try = pd.read_csv(data_io, sep='\t', nrows=1, encoding='utf-8')
                    cols = [str(c) for c in df_try.columns]
                    self.logger.info(f"2단계 헤더 감지: {cols}")
                    if any(col in cols for col in required_any):
                        self.logger.info("✅ 2단계 성공 (Shift+Right 추가)")
                        validated = True
                    else:
                        self.logger.warning("❌ 2단계 실패: 예상 컬럼명 없음")
                except Exception as e:
                    self.logger.warning(f"❌ 2단계 실패: {e}")
            else:
                self.logger.warning("❌ 2단계 실패: 데이터 크기 부족 또는 잘못된 데이터")
            
            self.logger.info("=" * 60)
        
        # ========== 3단계: GUI 방식 (세션 필요) ==========
        if not validated:
            self.logger.info("=" * 60)
            self.logger.info("=== 3단계 시도: GUI 방식 (세션 필요) ===")
            try:
                # set_focus 제거 (세션 없는 환경에서 블로킹됨)
                time.sleep(0.5)
                
                # pywinauto keyboard로 Ctrl+Home → Shift+End
                keyboard.send_keys("^{HOME}")  # Ctrl+Home
                time.sleep(0.3)
                keyboard.send_keys("+{END}")   # Shift+End
                time.sleep(0.5)
                
                # Shift+Right 10번
                for i in range(10):
                    keyboard.send_keys("+{RIGHT}")
                    time.sleep(0.05)
                time.sleep(0.3)
                
                self.logger.info("3단계 GUI 키 전송 완료")
                
                # 복사
                keyboard.send_keys("^c")  # Ctrl+C
                time.sleep(self.WAIT_TIME_LONG)
                
                # 검증
                table_data = pyperclip.paste()
                size = len(table_data) if table_data else 0
                self.logger.info(f"3단계 클립보드 데이터 길이: {size} bytes")
                
                if size >= 10:
                    data_io = StringIO(table_data)
                    df_try = pd.read_csv(data_io, sep='\t', nrows=1, encoding='utf-8')
                    cols = [str(c) for c in df_try.columns]
                    self.logger.info(f"3단계 헤더 감지: {cols}")
                    if any(col in cols for col in required_any):
                        self.logger.info("✅ 3단계 성공 (GUI 방식)")
                        validated = True
                    else:
                        self.logger.warning("❌ 3단계 실패: 예상 컬럼명 없음")
                else:
                    self.logger.warning("❌ 3단계 실패: 데이터 크기 부족")
                    
            except Exception as e:
                self.logger.error(f"❌ 3단계 실패: {e}")
            
            self.logger.info("=" * 60)
        
        if not validated:
            raise ValueError("1~3단계 모두 실패 (헤더 미검출)")

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
            
            # 실제 컬럼명 로그
            self.logger.info(f"클립보드에서 읽은 컬럼: {df.columns.tolist()}")
            
            # 컬럼명 매핑 (유연하게 처리)
            column_mapping = {
                "obsrdt": ["월일시분", "일시", "날짜시간", "DateTime"],
                "obsrf": ["관측우량(mm)", "관측우량", "Observed Rainfall"],
                "effrf": ["유효우량(mm)", "유효우량", "Effective Rainfall"],
                "obsinflow": ["관측유입(㎥/s)", "관측유입", "Observed Inflow"],
                "calcinflow": ["계산유입(㎥/s)", "계산유입", "Calculated Inflow"],
                "lowlevel": ["댐수위(El. m)", "댐수위", "Dam Level"],
                "totdcwtrqy": ["총방류(㎥/s)", "총방류", "Total Release"]
            }
            
            # 실제 존재하는 컬럼 찾기
            rename_dict = {}
            selected_columns = []
            
            for target_col, possible_names in column_mapping.items():
                for possible_name in possible_names:
                    if possible_name in df.columns:
                        rename_dict[possible_name] = target_col
                        selected_columns.append(possible_name)
                        break
            
            self.logger.info(f"매칭된 컬럼: {rename_dict}")
            
            if not selected_columns:
                raise ValueError(f"필요한 컬럼을 찾을 수 없습니다. 실제 컬럼: {df.columns.tolist()}")
            
            # 필요한 컬럼만 선택
            filtered_df = df[selected_columns].copy()
            
            # 컬럼명 변경
            filtered_df.rename(columns=rename_dict, inplace=True)

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

    def cleanup(self):
        """리소스 정리"""
        import subprocess
        
        pids = []
        try:
            if self.app:
                # PID 수집
                for window in self.app.windows():
                    try:
                        pids.append(window.process_id())
                        window.close()
                    except:
                        pass
                
                # 프로세스 강제 종료
                for pid in set(pids):
                    try:
                        subprocess.run(['taskkill', '/F', '/PID', str(pid)], 
                                     capture_output=True, timeout=5)
                        self.logger.info(f"정리: 프로세스 강제 종료 PID {pid}")
                    except:
                        pass
                
            self.logger.info("앱 정리 완료")
            time.sleep(2)  # 프로세스 완전 종료 대기
        except Exception as e:
            self.logger.error(f"정리 중 에러: {e}")

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
            self.forwarder.forward(success=False, err_msg=str(e))
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
