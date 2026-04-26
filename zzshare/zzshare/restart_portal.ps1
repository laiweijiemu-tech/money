$ErrorActionPreference = "Stop"
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

function Get-PythonCommand {
    foreach ($candidate in @(
        (Join-Path $projectRoot ".venv2\Scripts\python.exe"),
        (Join-Path $projectRoot ".venv\Scripts\python.exe")
    )) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    return "python"
}

function Get-TokenFromTestFile {
    $testFile = Join-Path $projectRoot "test_zzshare.py"
    if (-not (Test-Path $testFile)) {
        return $null
    }

    $content = Get-Content $testFile -Raw -ErrorAction SilentlyContinue
    if (-not $content) {
        return $null
    }

    $match = [regex]::Match($content, 'ZZSHARE_TOKEN", "([^"]+)"')
    if ($match.Success) {
        return $match.Groups[1].Value
    }

    return $null
}

function Stop-PortalProcess {
    Write-Host "检查 8000 端口占用..." -ForegroundColor Cyan
    $processIds = @(Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique)

    foreach ($processId in $processIds) {
        if ($processId -and $processId -ne 0) {
            Write-Host "终止旧进程 PID=$processId" -ForegroundColor Yellow
            Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
        }
    }
}

Stop-PortalProcess

if (-not $env:ZZSHARE_TOKEN) {
    $fallbackToken = Get-TokenFromTestFile
    if ($fallbackToken) {
        $env:ZZSHARE_TOKEN = $fallbackToken
        Write-Host "已从 test_zzshare.py 读取 ZZSHARE_TOKEN。" -ForegroundColor Green
    } else {
        Write-Host "未检测到 ZZSHARE_TOKEN，热点或实时接口可能不可用。" -ForegroundColor Yellow
    }
}

$pythonCmd = Get-PythonCommand
Write-Host "启动股票门户..." -ForegroundColor Cyan
Write-Host "访问地址: http://127.0.0.1:8000" -ForegroundColor Green
Write-Host "停止服务: Ctrl + C" -ForegroundColor DarkGray
Write-Host "使用 Python: $pythonCmd" -ForegroundColor DarkGray

& $pythonCmd -m zzshare.portal
