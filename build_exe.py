import PyInstaller.__main__
import os
import sys

def build_exe():
    """COSFIM 자동화 스크립트를 실행 파일로 빌드"""
    
    # PyInstaller 옵션 설정
    options = [
        'main.py',                     # 메인 스크립트
        '--onefile',                   # 단일 실행 파일로 생성
        # '--windowed',                # 콘솔 창 숨김 (GUI 앱용)
        '--console',                   # 콘솔 창 표시 (디버깅용)
        '--name=COSFIM_Automation',    # 실행 파일 이름
        '--add-data=requirements.txt;.',  # requirements.txt 포함
        '--hidden-import=pywinauto',
        '--hidden-import=pywinauto.application',
        '--hidden-import=pywinauto.keyboard',
        '--hidden-import=pyperclip',
        '--hidden-import=pandas',
        '--hidden-import=requests',
        '--hidden-import=argparse',
        '--hidden-import=logging',
        '--hidden-import=time',
        '--hidden-import=os',
        '--hidden-import=sys',
        '--hidden-import=StringIO',
        '--collect-all=pywinauto',
        '--collect-all=pyautogui',
    ]
    
    # PyInstaller 실행
    PyInstaller.__main__.run(options)
    
    print("빌드 완료 - .\dist\COSFIM_Automation.exe 파일이 생성되었습니다.")

if __name__ == "__main__":
    build_exe()
