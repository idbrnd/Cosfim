@echo off
echo COSFIM 자동화 EXE 테스트 시작...
echo.

echo 현재 디렉토리로 이동...
cd /d "%~dp0"

echo 1. 인자 없이 실행 (오류 테스트)
echo ----------------------------------------
dist\COSFIM_Automation.exe
echo.

echo 2. 존재하지 않는 파일로 실행 (오류 테스트)
echo ----------------------------------------
dist\COSFIM_Automation.exe --opt_data nonexistent_file.OPT
echo.

echo 3. 실제 OPT 파일로 실행 (절대 경로)
echo ----------------------------------------
dist\COSFIM_Automation.exe --opt_data "%CD%\sample_opt\태화강-대암댐-0707-0712-60.OPT"
echo.

echo 4. 실제 OPT 파일로 실행 (상대 경로)
echo ----------------------------------------
dist\COSFIM_Automation.exe --opt_data "sample_opt\태화강-대암댐-0707-0712-60.OPT"
echo.

echo 테스트 완료. 아무 키나 누르면 종료됩니다.
pause
