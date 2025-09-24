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
from dto import CosfimData


# water_system_name = "거제권"
# dam_name = "구천댐"
# dam_code = "2011602"
# template_id = "cbc038d8-87f6-4e81-a794-4b9945bc6a1a"
# opt_data = """O 2025 070624 071424 N  
# N
# 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00
# 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00
# 2025 07 06 00 00
# 2025 07 11 00 00
# 1
# 120 60
# 0.000 0.000 1.000 1.000
# 1 0 0 3 2025 7 11 24 0
# 1 1 0 0
# 1
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.000 
# 0.0 
# """
# api_end_point = "http://223.130.139.28/api/v1/widget/upload/cosfim"
# user_id = "20052970"
# user_pw = "20052970"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Forwarder:
    def __init__(self, end_point, water_system_name, dam_name, dam_code, template_id):
        self.end_point = end_point
        self.dam_name = dam_name
        self.water_system_name = water_system_name
        self.dam_code = dam_code
        self.template_id = template_id

    def forward(self, succcess=True, data_path="table_data.csv", err_msg=""):
        files = None
        try: 
            info_data = {
                "damName": self.dam_name,
                "waterSystemName": self.water_system_name, 
                "damCode": self.dam_code,
                "error": ""
            }
            query_params = {
                "templateId": self.template_id
            }
            
            if succcess:
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

            logging.info(f"{response.request.__dict__=}")

            if response.status_code == 200:
                logging.info("서버 응답: " + response.text)
            else:
                logging.info(f"서버 응답 실패: {response.status_code}, {response.text}")

            logging.info(f"포워딩 완료: 성공 여부: {succcess}, 메시지: {message}")
        except Exception as e:
            logging.info(f"포워딩 실패:{e}")


class CosfimHandler:
    APP_PATH = r"C:\Program Files (x86)\KWater\댐군 홍수조절 연계 운영 시스템\COSFIM_GUI"
    FILE_DIR = r"C:\COSFIM\WRKSPACE"
    WAIT_TIME = 0.1
    WAIT_TIME_LONG = 0.5
    WAIT_TIME_LONG_LONG = 1
    
    def __init__(self, forwarder, water_system_name, dam_name, user_id, user_pw, opt_data=None):
        self.forwarder = forwarder
        # 인자 및 데이터
        self.water_system_name = water_system_name
        self.dam_name = dam_name
        self.user_id = user_id
        self.user_pw = user_pw
        self.opt_data = opt_data

        if self.opt_data is None:
            return
        self.opt_data = self.set_opt_data(self.opt_data)
        logging.info(f"======{self.opt_data=}")
        self.start_time = self.get_start_time(self.opt_data)
        self.time_interval = self.get_time_interval(self.opt_data, self.start_time_idx)
        self.time_interval_list = self.get_time_interval_list(self.time_interval)
        self.is_new_instance = None
        self.file_data = None
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
        } # C:\COSFIM\GUI\DAM.spec 파일에 추가적인 댐(구분) 정보/C:\COSFIM\GUI\수리모형\FLDWAV.spec에 강(수계) 정보 있음

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
        # OPT data 파싱
        cnt = 0
        opt_data = opt_data.split("\n")
        for idx, line in enumerate(opt_data):
            if line[:2] in("19","20"):
                cnt += 1
                logging.info(f"{cnt=}, {line=}")
                if cnt == 2:
                    ele = line.split(" ")
                    year, month, day, hr, min = ele[-6:]
                    logging.info(f"{year=} {month=} {day=} {hr=} {min=}")
                    self.start_time_idx = idx
                    break
        return year, month, day, hr, min

    def get_time_interval(self, opt_data, start_time_idx):
        # OPT data 파싱
        opt_data = opt_data.split("\n")
        ele = opt_data[start_time_idx+2].split(" ")
        time_interval_int = ele[-1]
        time_interval_map = {"10": "10분", "30": "30분", "60": "60분",  "1440": "24시간"}
        time_interval = time_interval_map[time_interval_int]
        logging.info(f"{time_interval=}")
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

    # 엡 시작
    def _login(self):
            self.login_win.set_focus()
            time.sleep(self.WAIT_TIME_LONG_LONG)

            # ID/PW 입력
            self.login_win.child_window(auto_id="textBox_ID", control_type="Edit").type_keys(self.user_id)
            self.login_win.child_window(auto_id="textBox_PWD", control_type="Edit").type_keys(self.user_pw)
            self.login_win.child_window(auto_id="button_Accept", control_type="Button").click_input()
            logging.info("런치 - 로그인 -로그인 성공")
            time.sleep(self.WAIT_TIME_LONG_LONG)
    
    def _update_check(self):
        try: 
            update_win = self.app.window(title_re="선택")
            update_win.wait("visible", timeout=5)
            # update_win.child_window(auto_id="6", control_type="Button").click_input()
            # =============시연을 위한 업데이트 무시(버전 고정)===================
            update_win.child_window(auto_id="7", control_type="Button").click_input()
            logging.info("런치 - 업데이트 - 요청 발생, 업데이트 무시")
        except:
            logging.info("런치 - 업데이트 - 요청 없음, 업데이트 생략")
            return
    
    def launch_app(self):
        # Application 설정
        try:
            self.app = Application(backend='uia')
            self.app.connect(title_re="COSFIM.*Web Service")  # 필요시 코스핌 앱 종료 
            logging.info("런치 - 기존 창에 연결 성공")
            self.is_new_instance = False
        except Exception as e:
            self.app = Application(backend="uia").start(self.APP_PATH)          
            self.login_win = self.app.window(title_re="로그인")
            self.login_win.wait("visible", timeout=30)
            logging.info("런치 - 새로 실행 성공")
            self.is_new_instance = True

        if self.is_new_instance:
            
            self._login()
            self._update_check()
        else:
            logging.info("런치 - 기존 창 사용으로 로그인과 업데이트 요청 확인 생략")


    def get_elements(self):
        self.main_win = self._main_win()
        if not self.is_new_instance:
            self._close_residue_windows()
        self.tool_bar, self.save_btn, self.load_btn = self._tool_bar()   
        self.water_system_box, self.dam_box, self.time_interval_box, self.time_picker_start = self._select_box()

    # 메인 화면 처리
    def _main_win(self):
        # 메인 화면
        main_win = self.app.window(title_re="COSFIM.*Web Service", control_type="Window")
        
        # 새로 시작한 경우 메인 화면 뜨는거 기다림
        if self.is_new_instance:
            main_win.wait("visible", timeout=10)
            logging.info("메인 창 로딩 완료")
        else:
            logging.info("기존 메인 창 사용")
        main_win.set_focus()
        time.sleep(self.WAIT_TIME)
        return main_win


    def _close_windows(self, window):
        """에러 무시하고 윈도우 제거"""
        with suppress(Exception):
            if window.exists():
                logging.info(f"윈도우 제거: {window.window_text()}")
                window.close()
            time.sleep(self.WAIT_TIME)


    def _close_residue_windows(self):
        """이전 실행에서 남은 윈도우가 있다면 제거"""
        # 데이터 출력 창 
        logging.info("불필요한 윈도우 정리 시작...")
        data_output_wins = [
            self.app.window(auto_id="GraphForm", control_type="Window"),            
            self.app.window(auto_id="AnalysisForm", control_type="Window"),
            self.app.window(auto_id="DiagramSlideForm", control_type="Window")
        ]
        for window in data_output_wins:
            self._close_windows(window)

        # 에러, 알림창 
        self.main_win.set_focus()
        time.sleep(self.WAIT_TIME)
        error_wins = [
            self.main_win.child_window(title="선택", control_type="Window"),
            self.main_win.child_window(title="알림", control_type="Window"),
        ]
        for window in error_wins:            
            with suppress(Exception):
                if window.exists():
                    logging.info(f"윈도우 제거: {window.window_text()}")
                    window.child_window(title="아니요(N)", auto_id="7", control_type="Button").click_input()
                time.sleep(self.WAIT_TIME)

        logging.info("불필요한 윈도우 정리 완료")

    def _focus_main_win(self):
        self.main_win.set_focus()
        time.sleep(self.WAIT_TIME)


    def _tool_bar(self):
        # 툴바
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

    # 항목 선택 
    def _select_water_system(self):
        # 수계, 구분 선택
        self._focus_main_win()
        self.water_system_box.click_input()
        time.sleep(self.WAIT_TIME)
        self.water_system_box.child_window(title=self.water_system_name, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        logging.info(f"수계 선택 {self.water_system_name=}")
    
    def _select_dam(self):
        self._focus_main_win()
        self.dam_box.click_input()
        time.sleep(self.WAIT_TIME)
        self.dam_box.child_window(title=self.dam_name, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        logging.info(f"댐 선택 {self.dam_name=}")
    
    # 분석 단위 선택 
    def _select_time_interval(self):
        time_interval = self.time_interval
        self._focus_main_win()
        self.time_interval_box.click_input()
        time.sleep(self.WAIT_TIME)
        self.time_interval_box.child_window(title=time_interval, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        logging.info(f"시간 간격 선택 {time_interval=}")

    # 시작 시간 선택
    def _select_time_picker_start_date(self):
        year, month, day = self.start_time[:3]
        self._focus_main_win()
        time_picker_start_date= self.time_picker_start.child_window(auto_id="dateTimePicker", control_type="Pane")

        # 날짜-연
        pywinauto.mouse.click(coords=(time_picker_start_date.rectangle().left+2, time_picker_start_date.rectangle().top+2))
        time.sleep(self.WAIT_TIME_LONG)
        pywinauto.keyboard.send_keys(year)
        time.sleep(self.WAIT_TIME_LONG)
    
        # 날짜-월
        pywinauto.mouse.click(coords=(time_picker_start_date.rectangle().left+37, time_picker_start_date.rectangle().top+2))
        time.sleep(self.WAIT_TIME_LONG)
        pywinauto.keyboard.send_keys(month)
        time.sleep(self.WAIT_TIME_LONG)
    
        # 날짜-일
        pywinauto.mouse.click(coords=(time_picker_start_date.rectangle().left+55, time_picker_start_date.rectangle().top+2))
        time.sleep(self.WAIT_TIME_LONG)
        pywinauto.keyboard.send_keys(day)
        time.sleep(self.WAIT_TIME_LONG)
        logging.info(f"시작 날짜 선택:{year}-{month}-{day}")

        
    def _select_time_picker_start_hr_min(self):
        hr, min = self.start_time[-2:]
        self._focus_main_win()
        time_picker_start_hr= self.time_picker_start.child_window(auto_id="comboBox_Hour", control_type="ComboBox")
        time_picker_start_min= self.time_picker_start.child_window(auto_id="comboBox_Minute", control_type="ComboBox")

        # 시간
        time_picker_start_hr.click_input()
        time.sleep(self.WAIT_TIME_LONG_LONG)
        time_picker_start_hr.child_window(title=hr, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        # 분
        time_picker_start_min.click_input()
        time.sleep(self.WAIT_TIME_LONG_LONG)
        time_picker_start_min.child_window(title=min, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)

        logging.info(f"시작 시간 선택:{hr}:{min}")


    def _check_error_window(self):
        """에러 창을 확인하는 함수"""
        try:
            error_win = self.main_win.child_window(title="선택", control_type="Window")
            error_win.wait("visible", timeout=2)
            error_win.set_focus()
            time.sleep(self.WAIT_TIME)

            # 에러 창이 발견되면 처리
            logging.info("OPT 파일 불러오기 중 에러 발생")
            
            # 에러 메시지 가져오기
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
            logging.info("에러 창 없음 - 정상 진행")
        except Exception as e:
            logging.error(f"에러 창 확인 중 예상치 못한 에러: {e}")
    

    def select_options(self):
        try: 
            self._focus_main_win()
            self._select_water_system()
            self._select_dam()
            self.load_btn.click_input()
            time.sleep(self.WAIT_TIME_LONG_LONG)
            self._check_error_window()
            # ===아래는 OPT에서 안불러와지는 항목들 (필요시 활성화)===
            # self._select_time_interval()
            # self._select_time_picker_start_date()
            # if self.time_interval in ["10분", "30분", "60분"]:
            #     self._select_time_picker_start_hr_min()
            logging.info("OPT 파일 불러오기 완료")

        except Exception as e:
            logging.error(f"옵션 선택 중 에러 발생: {e}")
            raise


    # 옵션 파일 처리
    def _check_opt_file(self): 
        opt_name = self.opt_name_map[self.water_system_name][self.dam_name]
        opt_file_path = os.path.join(self.FILE_DIR, f"{opt_name}.OPT")
        logging.info(f"옵션 파일 경로 {opt_file_path=}")
        # 파일 없으면 생성 
        if not os.path.exists(opt_file_path):
            with open(opt_file_path, "w") as f:
                pass
            logging.info("OPT 파일 생성 완료")
        else:
            logging.info("OPT 파일 존재")
        logging.info(f"{opt_file_path=}")
        return opt_file_path
        
    def _save_opt_file(self, opt_file_path, opt_data):
        if opt_data is None:
            logging.info("OPT 데이터가 제공되지 않음")
            return
        logging.info(f"{opt_data=}")
        with open(opt_file_path, "w") as f:
            f.write(opt_data)

        logging.info("OPT 파일 적용 완료")
        with open(opt_file_path, "r") as f:
            logging.info(f.read())


    def handle_opt_file(self):
        opt_file_path = self._check_opt_file()
        logging.info(f"{self.opt_data=}")
        self._save_opt_file(opt_file_path, self.opt_data)


    
    # 데이터 처리
    def _get_data(self):
        # 실행 
        self.main_win.set_focus()
        time.sleep(self.WAIT_TIME)
        keyboard.send_keys("{F5}")

        # 파일 불러오기
        # main_win.print_control_identifiers()
        graph_win = self.app.window(auto_id="GraphForm", control_type="Window")
        graph_win.wait("visible", timeout=300)
        graph_win.set_focus()
        time.sleep(self.WAIT_TIME)
        table_tap = graph_win.child_window(auto_id="tabControl", control_type="Tab").child_window(title="테이블", control_type="TabItem")
        table_tap.click_input()
        time.sleep(self.WAIT_TIME)

        table_area = graph_win.child_window(title="테이블", auto_id="tabPage_Table", control_type="Pane")
        table_sheet = table_area.child_window(auto_id="sheet_DetailView", control_type="Pane")

        # 첫번째 항목 선택
        pywinauto.mouse.click(coords=(table_sheet.rectangle().left+2, table_sheet.rectangle().top+2))
        time.sleep(self.WAIT_TIME)

        # shift + 오른쪽 8회 
        for _ in range(8):
            pywinauto.keyboard.send_keys("+{RIGHT}")
            time.sleep(self.WAIT_TIME)
        
        # 복사 
        pywinauto.keyboard.send_keys("^c")
        time.sleep(self.WAIT_TIME_LONG)
        table_data = pyperclip.paste()

        # 창 닫기 
        analysis_win = self.app.window(auto_id="AnalysisForm", control_type="Window")
        diagram_win = self.app.window(auto_id="DiagramSlideForm", control_type="Window")

        graph_win.close()
        analysis_win.close()
        diagram_win.close()
        time.sleep(self.WAIT_TIME_LONG)

        # 데이터 저장 
        return table_data

    
    def _save_data(self, clipboard_data):
        try:
            # StringIO를 사용하여 문자열을 파일처럼 읽기
            data_io = StringIO(clipboard_data)
            
            # 탭으로 구분된 데이터를 데이터프레임으로 읽기
            df = pd.read_csv(data_io, sep='\t', encoding='utf-8')
            filtered_df = df[["월일시분", "관측우량(mm)", "유효우량(mm)", "관측유입(㎥/s)", "계산유입(㎥/s)", "댐수위(El. m)", "총방류(㎥/s)"]]
            filtered_df.rename(columns={
                "월일시분": "obsrdt", 
                "관측우량(mm)": "obsrf", 
                "유효우량(mm)": "effrf", 
                "관측유입(㎥/s)": "obsinflow", 
                "계산유입(㎥/s)": "calcinflow", 
                "댐수위(El. m)": "lowlevel", 
                "총방류(㎥/s)": "totdcwtrqy"
            }, inplace=True)

            # 시간 형식 수정 
            filtered_df["obsrdt"] = filtered_df["obsrdt"].apply(lambda x: str(x).replace(" ", "").replace(":", "").replace("-", ""))
            logging.info(f"{filtered_df.head()=}")
            # 데이터프레임을 CSV 파일로 저장
            filtered_df.to_csv("table_data.csv", index=False, encoding='utf-8-sig')
            logging.info(f"\n데이터프레임을 table_data.csv 파일로 저장했습니다.")

        except Exception as e:
            logging.info(f"데이터프레임 변환 중 오류 발생: {e}")
    

    def handle_data(self):
        try:
            clipboard_data = self._get_data()
            self._save_data(clipboard_data)
            self.forwarder.forward(data_path="table_data.csv")
        except TimeoutError as e:
            raise
        except Exception as e:
            logging.error(f"데이터 처리 중 에러 발생: {e}")
            raise


def get_data(cosfim_data: CosfimData):
    forwarder = Forwarder(cosfim_data.api_end_point, cosfim_data.water_system_name, cosfim_data.dam_name, cosfim_data.dam_code, cosfim_data.template_id)
    hdlr = CosfimHandler(forwarder, cosfim_data.water_system_name, cosfim_data.dam_name, cosfim_data.user_id, cosfim_data.user_pw, cosfim_data.opt_data)
    try:
        # forwarder.forward(data_path="table_data.csv")
        if hdlr.opt_data is None or hdlr.opt_data == "":
            raise ValueError("옵션 데이터가 제공되지 않았습니다")

        hdlr.launch_app()
        logging.info("===런치 완료===")

        hdlr.get_elements()
        logging.info("===요소 처리 완료===")

        hdlr.handle_opt_file()
        logging.info("===옵션 처리 완료===")

        hdlr.select_options()
        logging.info("===항목 선택 완료===")

        hdlr.handle_data()
        logging.info("===데이터 처리 완료===")

    except ValueError as e:
        logging.error(f"ValueError: {e}")
        forwarder.forward(succcess=False, err_msg=str(e))
        sys.exit(1)
    except TimeoutError as e:
        logging.error(f"TimeoutError: {e}")
        forwarder.forward(succcess=False, err_msg=str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        logging.error("사용자에 의한 중단")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Exception: {e}")
        # logging.info(f"Exception: {e}", exc_info=True)
        forwarder.forward(succcess=False, err_msg=str(e))
        sys.exit(1)


if __name__ == "__main__":
    get_data(CosfimData())