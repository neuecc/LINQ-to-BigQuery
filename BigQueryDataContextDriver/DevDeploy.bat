rem  sn -T assmname
rem external program - C:\Program Files (x86)\LINQPad5\LINQPad.exe

IF NOT EXIST %programdata%\LINQPad GOTO NOLINQPAD
taskkill /f /im:LINQPad.exe

rem LINQPad 4
rem xcopy /i/y *.* "%programdata%\LINQPad\Drivers\DataContext\4.0\BigQueryDataContextDriver (bacdf45c453bd173)\"

rem LINQPad 5
xcopy /i/y *.* "%LOCALAPPDATA%\LINQPad\Drivers\DataContext\4.6\BigQueryDataContextDriver (bacdf45c453bd173)\"

:NOLINQPAD