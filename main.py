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
import argparse


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Forwarder:
    def __init__(self):
        pass

    def forward(self, succcess=True, data_path="table_data.csv", err_msg=""):
        files = None
        if succcess:
            message = "success"
            with open(data_path, "rb") as csv_file:
                files = {"file": (data_path, csv_file, "text/csv")}
                response = requests.post("http://192.168.0.15:8080/file", files=files, json={"message": message})
        else:
            message = f"fail: {err_msg}"
            response = requests.post("http://192.168.0.15:8080/file", json={"message": message}) # 실패하면 그냥 None 보내기

        print(f"{response.status_code=}")
        print(f"{response.text=}")

        if response.status_code == 200:
            logging.info("서버 응답: " + response.text)

        logging.info(f"포워딩 완료: 성공 여부: {succcess}, 메시지: {message}")


class CosfimHandler:
    APP_PATH = r"C:\Program Files (x86)\KWater\댐군 홍수조절 연계 운영 시스템\COSFIM_GUI"
    FILE_DIR = r"C:\COSFIM\WRKSPACE"
    WAIT_TIME = 0.1
    WAIT_TIME_LONG = 0.5
    WAIT_TIME_LONG_LONG = 1
    
    def __init__(self, forwarder):
        self.forwarder = forwarder
        # 인자 및 데이터
        self.args = self.arg_parser()
        if self.args.opt_data is None:
            return
        self.opt_data = self.set_opt_data(self.args.opt_data)
        print(f"======{self.opt_data=}")
        print(f"======{self.args.opt_data=}")
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


    def arg_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--water_system_name", type=str, default="태화강")
        parser.add_argument("--dam_name", type=str, default="대곡댐")
        parser.add_argument("--opt_data", type=str, default=None) # 일단 파일 위치로 설정 # 인자로 받을지?
        parser.add_argument("--id", type=str, default="20052970")
        parser.add_argument("--pw", type=str, default="20052970")
        return parser.parse_args()

    def set_opt_data(self, opt_data):
        if self.args.opt_data is not None and os.path.exists(self.args.opt_data):
            with open(opt_data, "r") as f:
                opt_data = f.read()
            return opt_data
        else:
            return self.args.opt_data

    def get_start_time(self, opt_data):
        # OPT data 파싱
        cnt = 0
        opt_data = opt_data.split("\n")
        for idx, line in enumerate(opt_data):
            if line[:2] in("19","20"):
                cnt += 1
                print(f"{cnt=}, {line=}")
                if cnt == 2:
                    ele = line.split(" ")
                    year, month, day, hr, min = ele[-6:]
                    print(f"{year=} {month=} {day=} {hr=} {min=}")
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
        print(f"{time_interval=}")
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
            self.login_win.child_window(auto_id="textBox_ID", control_type="Edit").type_keys(self.args.id)
            self.login_win.child_window(auto_id="textBox_PWD", control_type="Edit").type_keys(self.args.pw)
            self.login_win.child_window(auto_id="button_Accept", control_type="Button").click_input()
            logging.info("런치 - 로그인 -로그인 성공")
            time.sleep(self.WAIT_TIME_LONG_LONG)
    
    def _update_check(self):
        try: 
            update_win = self.app.window(title_re="선택")
            update_win.wait("visible", timeout=1)
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
        self.water_system_box.child_window(title=self.args.water_system_name, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        logging.info(f"수계 선택 {self.args.water_system_name=}")
    
    def _select_dam(self):
        self._focus_main_win()
        self.dam_box.click_input()
        time.sleep(self.WAIT_TIME)
        self.dam_box.child_window(title=self.args.dam_name, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        logging.info(f"댐 선택 {self.args.dam_name=}")
    
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


    def select_options(self):
        try: 
            self._focus_main_win()
            self._select_water_system()
            self._select_dam()
            self.load_btn.click_input()
            time.sleep(self.WAIT_TIME_LONG_LONG)
            # ===아래는 OPT에서 안불러와지는 항목들 (필요시 활성화)===
            # self._select_time_interval()
            # self._select_time_picker_start_date()
            # if self.time_interval in ["10분", "30분", "60분"]:
            #     self._select_time_picker_start_hr_min()
            logging.info("옵션 선택 완료.")

        except Exception as e:
            logging.error(f"옵션 선택 중 에러 발생: {e}")
            raise


    # 옵션 파일 처리
    def _check_opt_file(self): 
        opt_name = self.opt_name_map[self.args.water_system_name][self.args.dam_name]
        opt_file_path = os.path.join(self.FILE_DIR, f"{opt_name}.OPT")
        logging.info(f"옵션 파일 경로 {opt_file_path=}")
        # 파일 없으면 생성 
        if not os.path.exists(opt_file_path):
            with open(opt_file_path, "w") as f:
                pass
            logging.info("OPT 파일 생성 완료")
        else:
            logging.info("OPT 파일 존재")
        print(f"{opt_file_path=}")
        return opt_file_path
        
    def _save_opt_file(self, opt_file_path, opt_data):
        if opt_data is None:
            logging.info("OPT 데이터가 제공되지 않음")
            return
        print(f"{opt_data=}")
        with open(opt_file_path, "w") as f:
            f.write(opt_data)

        logging.info("OPT 파일 적용 완료")
        with open(opt_file_path, "r") as f:
            print(f.read())


    def handle_opt_file(self):
        opt_file_path = self._check_opt_file()
        print(f"======{self.opt_data=}")
        self._save_opt_file(opt_file_path, self.opt_data)


    
    # 데이터 처리
    def _get_data(self):
             
        # 수정된 파일 불러오기 
        # self.load_btn.click_input()
        time.sleep(self.WAIT_TIME_LONG)

        # F5로 실행 
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
        # =============시연을 위한 지연 추가===================
        time.sleep(10)

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
            
            # 데이터프레임을 CSV 파일로 저장
            df.to_csv("table_data.csv", index=False, encoding='utf-8-sig')
            logging.info(f"\n데이터프레임을 table_data.csv 파일로 저장했습니다.")

        except Exception as e:
            logging.info(f"데이터프레임 변환 중 오류 발생: {e}")
            print(f"fail:None")
    

    

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



if __name__ == "__main__":
    try:
        forwarder = Forwarder()
        hdlr = CosfimHandler(forwarder)
        if hdlr.args.opt_data is None or hdlr.args.opt_data == "":
            raise ValueError("옵션 데이터가 제공되지 않았습니다")

        hdlr.launch_app()
        logging.info("런치 완료")

        hdlr.get_elements()
        logging.info("요소 처리 완료")

        hdlr.handle_opt_file()
        logging.info("옵션 처리 완료")

        hdlr.select_options()
        logging.info("항목 선택 완료")

        hdlr.handle_data()
        logging.info("데이터 처리 완료")

    except ValueError as e:
        logging.info(f"ValueError: {e}")
        forwarder.forward(succcess=False, err_msg=str(e))
        sys.exit(1)
    except TimeoutError as e:
        logging.info(f"TimeoutError: {e}")
        forwarder.forward(succcess=False, err_msg=str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("사용자에 의한 중단")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Exception: {e}", exc_info=True)
        forwarder.forward(succcess=False, err_msg=str(e))
        sys.exit(1)