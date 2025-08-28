# COSFIM 자동화 프로그램 빌드 가이드

## 1. 환경 준비

### Windows용 .exe 파일 생성
```bash
# PyInstaller 설치
pip install pyinstaller

# 의존성 설치
pip install -r requirements.txt

# 빌드
python build_exe.py
```

## 2. 실행 방법

### Windows (.exe)
```bash
# 기본값
COSFIM_Automation.exe

# + 인자
COSFIM_Automation.exe --water_system_name "영산강" --dam_name "담양댐" --id "user123" --pw "pass123"
```