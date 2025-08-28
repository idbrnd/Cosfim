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
# import requests
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class CosfimHandler:
    APP_PATH = r"C:\Program Files (x86)\KWater\댐군 홍수조절 연계 운영 시스템\COSFIM_GUI"
    FILE_DIR = r"C:\COSFIM\WRKSPACE"
    WAIT_TIME = 0.1
    WAIT_TIME_LONG = 0.5
    WAIT_TIME_LONG_LONG = 1
    
    def __init__(self):
        # 인자 및 데이터
        self.args = self.arg_parser()
        self.file_path = self.get_file_path()
        self.is_new_instance = None
        self.file_data = None
        self.opt_name_map = {
            "낙동강": "NDMF",
            "영산강": "YDM",
            "영산강": "YDM",
        }

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
        parser.add_argument("--water_system_name", type=str, default="낙동강")
        parser.add_argument("--dam_name", type=str, default="안동댐")
        parser.add_argument("--method", type=str, default="latest")
        parser.add_argument("--id", type=str, default="20052970")
        parser.add_argument("--pw", type=str, default="20052970")
        return parser.parse_args()


    def get_file_path(self):
        """args 에 따라 파일 경로 반환/혹은 가장 최신 파일 반환"""
        file_list = os.listdir(self.FILE_DIR)         #현재는 가장 최신 파일 반환중..
        if len(file_list) == 0:
            logging.error("파일 경로 없음")
            return None
        
        file_list.sort(key=lambda x: os.path.getmtime(os.path.join(self.FILE_DIR, x)))
        latest_file = file_list[-1]
        file_path = os.path.join(self.FILE_DIR, latest_file)
        return file_path


    # 엡 시작
    def _login(self):
        # 로그인 (사용자로 부터 직업 입력 받거나 llM단에서 받야야 할 수도)
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
            update_win.child_window(auto_id="6", control_type="Button").click_input()
            logging.info("런치 - 업데이트 - 요청 발생, 업데이트 진행")
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
        self.water_system_box, self.dam_box = self._select_box()

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
        return water_system_box, dam_box



    # 항목 선택 
    def _select_water_system(self):
        # 수계, 구분 선택
        self.water_system_box.click_input()
        time.sleep(self.WAIT_TIME)
        self.water_system_box.child_window(title=self.args.water_system_name, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        logging.info(f"수계 선택 {self.args.water_system_name=}")
    
    def _select_dam(self):
        self.dam_box.click_input()
        time.sleep(self.WAIT_TIME)
        self.dam_box.child_window(title=self.args.dam_name, control_type="ListItem").click_input()
        time.sleep(self.WAIT_TIME_LONG)
        logging.info(f"댐 선택 {self.args.dam_name=}")

    
    def select_options(self):
        self._focus_main_win()
        self._select_water_system()
        self._select_dam()


    
    # 옵션 파일 처리
    def _load_opt_file(self): 
        # 저장된 파일 읽기 -> 매핑으로 수정 예정/ 매핑하면 필요 없을수도? 
        file_path = self.get_file_path()
        logging.info(f"파일 경로 {file_path=}")
        # 데이터 읽기
        with open(file_path, "r") as f:
            self.file_data = f.read()
            logging.info(f"데이터 읽어옴 {self.file_data=}")

    def _make_opt_data_from_args(self):
        opt_data = "뭐가 들어올지 모르겠음"
        logging.info(f"옵션 데이터 생성 {opt_data=}")
        return opt_data
        
    def _save_opt_file(self):
        # with open(self.file_path, "w") as f:
        #     f.write(self.file_data)
        logging.info(f"옵션 데이터 저장 {self.file_data=}")

    def handle_opt_file(self):
        self._load_opt_file() # 필요 없을수도 (매핑하면)
        self._make_opt_data_from_args()
        self._save_opt_file()

    
    # 데이터 처리
    def _get_data(self):
             
        # 수정된 파일 불러오기 
        self.load_btn.click_input()
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

        logging.info("테이블 영역 찾기dma~~~~~~~~~")

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
        logging.info("전체 선택")
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
            
            logging.info("\n=== 판다스 데이터프레임 ===")
            logging.info(f"데이터프레임 크기: {df.shape}")
            logging.info(f"컬럼명: {list(df.columns)}")
            logging.info("\n처음 5행:")
            logging.info(df.head())
            
            # 데이터프레임을 CSV 파일로 저장
            df.to_csv("table_data.csv", index=False, encoding='utf-8-sig')
            logging.info(f"\n데이터프레임을 table_data.csv 파일로 저장했습니다.")

        except Exception as e:
            logging.info(f"데이터프레임 변환 중 오류 발생: {e}")
            print(f"fail:None")
    

    def _send_data(self):
        with open("table_data.csv", "rb") as csv_file:
            files = {"file": ("table_data.csv", csv_file, "text/csv")}
            # response = requests.post("http://test-server:8080/api/v1/data", files=files)
            # if response.status_code == 200:
            #     logging.info("서버 응답: " + response.text)
            logging.info(f"CSV 파일 전송 완료")# . 응답 상태: {response.status_code}")
    
    def handle_data(self):
        clipboard_data = self._get_data()
        self._save_data(clipboard_data)
        self._send_data()



if __name__ == "__main__":
    try:
        hdlr = CosfimHandler()
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
    except KeyboardInterrupt:
        logging.info("내가 중단함!")
        sys.exit(130)
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}", exc_info=True)
        sys.exit(1)



# http://drive.google.com/drive/folders/1NN70LsQGQ21NKf4gD8r40tnADtU7pE3c?usp=sharing

 ### ================================================ 쉘로 만들어야 함!!!! ================================================
 ### ================================================ 쉘로 만들어야 함!!!! ================================================
 ### ================================================ 쉘로 만들어야 함!!!! ================================================
