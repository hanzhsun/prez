@echo off
cd %~dp0
start cmd /k "poetry run python main.py"
