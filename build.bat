@echo off
cd %~dp0
start cmd /k "docker build -t p3814 ."
