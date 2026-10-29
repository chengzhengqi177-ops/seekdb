# Builds and runs the dependency-free parquet reader test with MSVC (cl.exe).
# The reader itself needs no third-party libraries; only a C++17 compiler.
#
# Usage (from anywhere):   powershell -ExecutionPolicy Bypass -File run_test_parquet.ps1
# Optional environment variable VCVARS64 may point at vcvars64.bat.

$ErrorActionPreference = 'Stop'

$vcvars = $env:VCVARS64
if (-not $vcvars) {
  $vcvars = 'C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat'
}
if (-not (Test-Path $vcvars)) {
  Write-Error "vcvars64.bat not found: $vcvars (set VCVARS64 to override)"
  exit 1
}

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
  $cmd = '"' + $vcvars + '" >nul 2>&1 && cl /nologo /std:c++17 /EHsc /O2 /I. ' +
         'file_sql_parquet.cpp test\test_parquet_standalone.cpp ' +
         '/Fe:test\test_parquet_standalone.exe && test\test_parquet_standalone.exe'
  cmd /c $cmd
  $code = $LASTEXITCODE
} finally {
  Pop-Location
}
exit $code
