:: First install package
IF EXIST "%DEPLOYMENT_TARGET%\requirements.txt" (
  pushd "%DEPLOYMENT_TARGET%"
  call :ExecuteCmd "D:\home\python364x64\pip install -r requirements.txt"
  popd
)

:: Then run
D:\home\python364x64\python.exe bitfinex/main.py --usemssql --includefundings