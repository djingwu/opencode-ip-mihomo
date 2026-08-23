# Build a Python/Docker-free Windows x64 portable ZIP.
[CmdletBinding()]
param(
    [string]$PythonExe = "python",
    [string]$Version = "1.0.0"
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$BuildVenv = Join-Path $RepoRoot ".portable-build-venv"
$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$Staging = Join-Path $RepoRoot ".portable-staging\$Stamp"
$OutputDir = Join-Path $RepoRoot "dist"
$PackageName = "OpenCode-IP-Mihomo-Windows-Portable-v$Version-x64"
$PackageRoot = Join-Path $Staging $PackageName
$PyInstallerWork = Join-Path $Staging "pyinstaller-work"
$PyInstallerSpec = Join-Path $Staging "pyinstaller-spec"

function Invoke-Checked {
    param([string]$FilePath, [string[]]$Arguments)
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed (ExitCode=$LASTEXITCODE): $FilePath $($Arguments -join ' ')"
    }
}

New-Item -ItemType Directory -Force -Path $Staging, $OutputDir | Out-Null
Write-Host "==> Creating isolated build environment"
if (-not (Test-Path -LiteralPath (Join-Path $BuildVenv "Scripts\python.exe"))) {
    Invoke-Checked $PythonExe @("-m", "venv", $BuildVenv)
}
$Py = Join-Path $BuildVenv "Scripts\python.exe"
Invoke-Checked $Py @("-m", "pip", "install", "--upgrade", "pip")
Invoke-Checked $Py @("-m", "pip", "install", "-r", (Join-Path $RepoRoot "requirements.txt"), "-r", (Join-Path $PSScriptRoot "requirements-build.txt"))

Write-Host "==> Downloading official mihomo Windows x64 compatible build"
$Release = Invoke-RestMethod -Headers @{ "User-Agent" = "opencode-ip-mihomo-portable-builder" } -Uri "https://api.github.com/repos/MetaCubeX/mihomo/releases/latest"
$Asset = $Release.assets | Where-Object { $_.name -match '^mihomo-windows-amd64-compatible-v.+\.zip$' } | Select-Object -First 1
if (-not $Asset) { throw "No compatible Windows x64 mihomo ZIP found." }
$MihomoZip = Join-Path $Staging $Asset.name
Invoke-WebRequest -UseBasicParsing -Uri $Asset.browser_download_url -OutFile $MihomoZip
$MihomoExtract = Join-Path $Staging "mihomo-release"
Expand-Archive -LiteralPath $MihomoZip -DestinationPath $MihomoExtract -Force
$MihomoExe = Get-ChildItem -LiteralPath $MihomoExtract -Filter "*.exe" -Recurse | Select-Object -First 1
if (-not $MihomoExe) { throw "mihomo Windows executable not found in release archive." }

Write-Host "==> Packaging Python services"
$ExeDir = Join-Path $PackageRoot "app"
New-Item -ItemType Directory -Force -Path $ExeDir | Out-Null
$BaseArgs = @("-m", "PyInstaller", "--noconfirm", "--clean", "--onefile", "--console", "--distpath", $ExeDir, "--workpath", $PyInstallerWork, "--specpath", $PyInstallerSpec)
Invoke-Checked $Py ($BaseArgs + @("--name", "opencode-gateway", "--add-data", "$RepoRoot\templates;templates", "--collect-all", "curl_cffi", "--hidden-import", "yaml", (Join-Path $RepoRoot "server.py")))
Invoke-Checked $Py ($BaseArgs + @("--name", "opencode-rotator", "--collect-all", "curl_cffi", (Join-Path $RepoRoot "rotator.py")))
Invoke-Checked $Py ($BaseArgs + @("--name", "opencode-free-nodes", "--hidden-import", "yaml", (Join-Path $RepoRoot "free_nodes.py")))
Invoke-Checked $Py ($BaseArgs + @("--name", "opencode-launcher", (Join-Path $PSScriptRoot "portable_launcher.py")))

Write-Host "==> Assembling portable package"
foreach ($relative in @("bin", "data", "logs", "mihomo\providers", "runtime")) {
    New-Item -ItemType Directory -Force -Path (Join-Path $PackageRoot $relative) | Out-Null
}
Copy-Item -LiteralPath $MihomoExe.FullName -Destination (Join-Path $PackageRoot "bin\mihomo.exe") -Force
Copy-Item -LiteralPath (Join-Path $RepoRoot "mihomo\config.example.yaml") -Destination (Join-Path $PackageRoot "mihomo\config.example.yaml") -Force
foreach ($name in @("start-windows.bat", "stop-windows.bat", "README-Windows-Portable.md", "portable.env.example")) {
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot $name) -Destination $PackageRoot -Force
}
Copy-Item -LiteralPath (Join-Path $RepoRoot "LICENSE") -Destination $PackageRoot -Force
New-Item -ItemType File -Force -Path (Join-Path $PackageRoot "data\proxies.txt") | Out-Null

[ordered]@{
    product = "OpenCode IP Mihomo"
    version = $Version
    architecture = "windows-amd64"
    built_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    mihomo_version = $Release.tag_name
    mihomo_asset = $Asset.name
    python_builder = (& $Py --version)
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $PackageRoot "build-info.json") -Encoding utf8

$ZipPath = Join-Path $OutputDir "$PackageName-$Stamp.zip"
Compress-Archive -LiteralPath $PackageRoot -DestinationPath $ZipPath -CompressionLevel Optimal
$ZipSize = [Math]::Round((Get-Item -LiteralPath $ZipPath).Length / 1MB, 1)
Write-Host "Portable ZIP: $ZipPath" -ForegroundColor Green
Write-Host "ZIP size: $ZipSize MB"




