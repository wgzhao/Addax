@echo off
setlocal

:: Addax command line script
:: Author wgzhao<wgzhao@gmail.com>
:: Created at 2024-08-20

:: ------------------------------ constant ----------------------------------------
set SCRIPT_PATH=%~dp0
set ADDAX_HOME=%SCRIPT_PATH%..
if "%ADDAX_HOME%"=="" exit /b 2

set CLASS_PATH=.;%ADDAX_HOME%\lib\*
set LOGBACK_FILE=%ADDAX_HOME%\conf\logback.xml
set DEFAULT_JVM=-Xms64m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -XX:+ExitOnOutOfMemoryError -XX:HeapDumpPath=%ADDAX_HOME%
set DEFAULT_PROPERTY_CONF=-Dfile.encoding=UTF-8 -Djava.security.egd=file:///dev/urandom -Daddax.home=%ADDAX_HOME% -Dlogback.configurationFile=%LOGBACK_FILE%
set ENGINE_COMMAND=java -server %DEFAULT_JVM% %DEFAULT_PROPERTY_CONF% -classpath %CLASS_PATH%
set REMOTE_DEBUG_CONFIG=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=0.0.0.0:9999

:: ------------------------- global variables ---------------------------
set CUST_JVM=
set LOG_DIR=%ADDAX_HOME%\log
set DEBUG=0
set LOG_LEVEL=info
set JOB_FILE=
set LOG_FILE=

if "%1" == "" goto usage

set job_name=%~n1
set job_escaped_name=%job_name:.=_%
set curr_time=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set LOG_FILE=addax_%job_escaped_name%_%curr_time%.log



:: ------------------------------------ main -----------------------------

:: combine command
set cmd=%ENGINE_COMMAND% %CUST_JVM% %PARAMS% -Dloglevel=%LOG_LEVEL% -Daddax.log=%LOG_DIR% -Dlog.file.name=%LOG_FILE%

if %DEBUG% equ 1 (
    set cmd=%cmd% %REMOTE_DEBUG_CONFIG%
)

:: attach main class
set cmd=%cmd% com.wgzhao.addax.core.Engine -job %1

:: run it
%cmd%

:usage
echo Usage: %~nx0 [options] job-url-or-path
echo.
echo Options:
echo /h,             This help text
echo /v,             Show version number and quit
echo /j, Setup extra java jvm parameters if necessary.
echo /p,    Setup job parameter, eg: the item 'tableName' which you want to specify by command,
echo                                you can use pass -p"-DtableName=your-table-name".
echo                                If you want to multiple parameters, you can use
echo                                -p"-DtableName=your-table-name -DcolumnName=your-column-name".
echo                                Note: you should config in you job tableName with \${tableName}.
echo /l, the directory which log writes to
echo /d,                  Setup to remote debug mode.
echo /L, Setup log level such as: debug, info, warn, error, all etc.
exit /b 1