@echo OFF
REM 
SET PYTHONIOENCODING=utf-8
REM 

REM 
SET BATCH_LOG_FILE=D:\Project_DW\batch_run.log
echo --- BAT DAU QUA TRINH ETL (%DATE% %TIME%) --- > %BATCH_LOG_FILE%

REM 
cd D:\Project_DW
echo Chuyen den thu muc D:\Project_DW >> %BATCH_LOG_FILE%

REM 
SET PYTHON_EXE=D:\Project_DW\venv\Scripts\python.exe
echo Dat duong dan Python la: %PYTHON_EXE% >> %BATCH_LOG_FILE%

echo [1.5] Kiem tra trang thai ETL hom nay... >> %BATCH_LOG_FILE%
REM 
%PYTHON_EXE% check_status.py >> %BATCH_LOG_FILE% 2>&1

if %errorlevel% == 0 (
    echo ETL hom nay DA HOAN TAT. Khong can chay lai. >> %BATCH_LOG_FILE%
    goto :success
)
echo ETL hom nay CHUA HOAN TAT. Bat dau chay... >> %BATCH_LOG_FILE%

echo [2] Bat dau CRAWL >> %BATCH_LOG_FILE%
%PYTHON_EXE% crawl.py >> %BATCH_LOG_FILE% 2>&1
if %errorlevel% neq 0 (
    echo LOI: crawl.py da that bai. >> %BATCH_LOG_FILE%
    goto :error
)

echo [3] Bat dau LOAD STAGING >> %BATCH_LOG_FILE%
%PYTHON_EXE% load_staging.py >> %BATCH_LOG_FILE% 2>&1
if %errorlevel% neq 0 (
    echo LOI: load_staging.py da that bai. >> %BATCH_LOG_FILE%
    goto :error
)

echo [4] Bat dau TRANSFORM >> %BATCH_LOG_FILE%
%PYTHON_EXE% transform_staging.py >> %BATCH_LOG_FILE% 2>&1
if %errorlevel% neq 0 (
    echo LOI: transform_staging.py da that bai. >> %BATCH_LOG_FILE%
    goto :error
)

echo [5] Bat dau LOAD DWH >> %BATCH_LOG_FILE%
%PYTHON_EXE% load_dwh.py >> %BATCH_LOG_FILE% 2>&1
if %errorlevel% neq 0 (
    echo LOI: load_dwh.py da that bai. >> %BATCH_LOG_FILE%
    goto :error
)

echo --- HOAN TAT TOAN BO QUA TRINH ETL --- >> %BATCH_LOG_FILE%
goto :success

:error
echo !!! QUA TRINH ETL DA THAT BAI !!! >> %BATCH_LOG_FILE%
goto :end

:success
echo (Ket thuc tac vu) >> %BATCH_LOG_FILE%

:end
echo --- KET THUC SCRIPT BATCH (%TIME%) --- >> %BATCH_LOG_FILE%