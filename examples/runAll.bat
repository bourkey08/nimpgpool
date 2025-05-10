@echo off
::Build Everything
nim c -d:release --opt:speed --threads:on --mm:refc --out="bin/basic.exe" basic.nim


::Run All examples/tests
cls
echo                Running Examples/Tests
echo Running Basic.nim
call "bin/basic.exe"

::Remove the binars
rmdir /S /Q bin
pause