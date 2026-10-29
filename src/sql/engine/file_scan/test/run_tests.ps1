# Copyright (c) 2025 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Builds and runs the standalone unit test of the file-as-database readers with
# the MSVC 2022 BuildTools toolchain.  Nothing here depends on the seekdb cmake
# build, so it can be used while the engine integration is being developed.
#
#   pwsh -File src\sql\engine\file_scan\test\run_tests.ps1
#   pwsh -File ...\run_tests.ps1 -DataDir D:\path\to\file_sql\data -KeepArtifacts
#
# Exit code 0 means every check passed.

[CmdletBinding()]
param(
  [string]$DataDir = '',
  [switch]$KeepArtifacts
)

$ErrorActionPreference = 'Stop'

$testDir = $PSScriptRoot
$fileScanDir = Split-Path -Parent $testDir
$repoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $fileScanDir)))

$vcvars = 'C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat'
if (-not (Test-Path -LiteralPath $vcvars)) {
  throw "vcvars64.bat not found: $vcvars"
}

if ([string]::IsNullOrEmpty($DataDir)) {
  $DataDir = Join-Path $repoRoot 'tools\deploy\mysql_test\test_suite\file_sql\data'
}
if (-not (Test-Path -LiteralPath (Join-Path $DataDir 'basic.csv'))) {
  throw "basic.csv not found under '$DataDir'; pass -DataDir <dir>"
}

$objDir = Join-Path $testDir 'obj'
$exe = Join-Path $testDir 'test_file_sql.exe'
$bat = Join-Path $objDir 'build_and_run.bat'
New-Item -ItemType Directory -Force -Path $objDir | Out-Null

$sources = @(
  (Join-Path $fileScanDir 'file_sql_infer.cpp'),
  (Join-Path $fileScanDir 'file_sql_csv.cpp'),
  (Join-Path $fileScanDir 'file_sql_jsonl.cpp'),
  (Join-Path $testDir 'test_main.cpp')
) | ForEach-Object { '"' + $_ + '"' }

$batLines = @(
  '@echo off',
  ('call "' + $vcvars + '" >nul'),
  ('pushd "' + $objDir + '"'),
  ('cl /nologo /std:c++17 /EHsc /O2 /W4 /utf-8 /I"' + $fileScanDir + '" ' + ($sources -join ' ') + ' /Fe:"' + $exe + '"'),
  'set BUILD_RC=%ERRORLEVEL%',
  'popd',
  'exit /b %BUILD_RC%'
)
[System.IO.File]::WriteAllText($bat, (($batLines -join "`r`n") + "`r`n"), (New-Object System.Text.UTF8Encoding($false)))

Write-Host "== building standalone reader tests ==" -ForegroundColor Cyan
& cmd.exe /d /c $bat
$buildRc = $LASTEXITCODE
if ($buildRc -ne 0) {
  throw "compilation failed (exit $buildRc)"
}

Write-Host "== running standalone reader tests ==" -ForegroundColor Cyan
& $exe $DataDir
$runRc = $LASTEXITCODE

if (-not $KeepArtifacts) {
  Get-ChildItem -Path $objDir -Filter '*.obj' -ErrorAction SilentlyContinue | Remove-Item -Force
  if (Test-Path -LiteralPath $bat) { Remove-Item -LiteralPath $bat -Force }
}
if (-not $KeepArtifacts -and $runRc -eq 0 -and (Test-Path -LiteralPath $exe)) {
  Remove-Item -LiteralPath $exe -Force
}
if (-not $KeepArtifacts) {
  if ((Test-Path -LiteralPath $objDir) -and
      -not (Get-ChildItem -Path $objDir -Force -ErrorAction SilentlyContinue)) {
    Remove-Item -LiteralPath $objDir -Force
  }
}

if ($runRc -ne 0) {
  throw "unit tests failed (exit $runRc)"
}
Write-Host "== standalone reader tests passed ==" -ForegroundColor Green
exit 0
