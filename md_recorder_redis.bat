@echo off
if not "%~1"=="p" start /min cmd.exe /c %0 p&exit
set base_dir=%~dp0
%base_dir:~0,2%
pushd %base_dir%
title md_recorder_redis
python.exe md_recorder_redis.py
popd


