import time, pyautogui
print("3초 뒤 현재 마우스 좌표를 캡처합니다. 원하는 위치에 마우스를 두세요...")
time.sleep(3)
x, y = pyautogui.position()
print("캡처 좌표:", x, y)
pyautogui.moveTo(x, y, duration=0.2)
pyautogui.click()