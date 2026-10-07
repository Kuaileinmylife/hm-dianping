$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$nginx = Join-Path $root 'src\main\resources\nginx-1.18.0'
$java = 'C:\Program Files\Eclipse Adoptium\jdk-8.0.504.1-hotspot\bin\java.exe'
if (!(Test-Path -LiteralPath $java)) { throw 'Java 8 executable not found. Update the java path in this script.' }
New-Item -ItemType Directory -Force -Path (Join-Path $root 'logs') | Out-Null
foreach ($port in @(8080,8081)) {
    $client = New-Object Net.Sockets.TcpClient
    try {
        if ($client.ConnectAsync('127.0.0.1',$port).Wait(500) -and $client.Connected) {
            throw "Port $port is already in use. Stop the previous service first."
        }
    } catch [System.AggregateException] { } finally { $client.Dispose() }
}
Push-Location $nginx
try {
    & .\nginx.exe -t
    if ($LASTEXITCODE -ne 0) { throw 'Nginx configuration check failed.' }
} finally { Pop-Location }
$jar = Get-ChildItem -LiteralPath (Join-Path $root 'target') -Filter '*.jar' | Select-Object -First 1
if (!$jar) { throw 'Build first: mvn package -DskipTests' }
$backend = Start-Process -FilePath $java -ArgumentList @('-jar', ('"' + $jar.FullName + '"')) -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput (Join-Path $root 'logs\backend.log') -RedirectStandardError (Join-Path $root 'logs\backend-error.log') -PassThru
$backend.Id | Set-Content (Join-Path $root 'logs\backend.pid')
$ready = $false
for ($i=0; $i -lt 45; $i++) {
    if ($backend.HasExited) { throw 'Backend exited. Read logs/backend.log and backend-error.log.' }
    try {
        $response = Invoke-WebRequest 'http://127.0.0.1:8081/shop-type/list' -UseBasicParsing -TimeoutSec 2
        if ($response.StatusCode -eq 200) { $ready=$true; break }
    } catch { }
    Start-Sleep -Seconds 1
}
if (!$ready) { throw 'Backend did not become ready. Check logs before retrying.' }
Start-Process -FilePath (Join-Path $nginx 'nginx.exe') -WorkingDirectory $nginx -WindowStyle Hidden
Write-Host 'Course frontend: http://localhost:8080'
Write-Host 'Backend: http://localhost:8081'
Write-Host 'Initial code intentionally returns unfinished for login/code endpoints.'
