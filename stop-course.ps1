$ErrorActionPreference = 'Stop'
$root=$PSScriptRoot
$nginx=Join-Path $root 'src\main\resources\nginx-1.18.0'
if (Test-Path -LiteralPath (Join-Path $nginx 'logs\nginx.pid')) {
    Push-Location $nginx
    try { & .\nginx.exe -s quit } finally { Pop-Location }
}
$pidFile=Join-Path $root 'logs\backend.pid'
if (Test-Path -LiteralPath $pidFile) {
    $backendId=[int](Get-Content -LiteralPath $pidFile)
    $process=Get-CimInstance Win32_Process -Filter "ProcessId=$backendId"
    if ($process -and $process.Name -eq 'java.exe' -and $process.CommandLine.Contains($root)) {
        Stop-Process -Id $backendId
    }
    Remove-Item -LiteralPath $pidFile
}
