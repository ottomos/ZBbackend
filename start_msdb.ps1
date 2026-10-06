param(
    [string]$IpAddress
)

$ErrorActionPreference = 'Stop'
$envPath = Join-Path $PSScriptRoot '.env'

if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
    throw "Missing $envPath. Create it from .env.example first."
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    throw 'Node.js is required but was not found in PATH.'
}

if (-not $IpAddress) {
    $IpAddress = (Find-NetRoute -RemoteIPAddress '1.1.1.1' |
        Where-Object { $_.IPAddress } | Select-Object -First 1).IPAddress
}

$parsedAddress = $null
if (-not [System.Net.IPAddress]::TryParse($IpAddress, [ref]$parsedAddress) -or
    $parsedAddress.AddressFamily -ne [System.Net.Sockets.AddressFamily]::InterNetwork) {
    throw 'No valid IPv4 address found. Run with -IpAddress <your-ip>.'
}
$IpAddress = $parsedAddress.ToString()

$content = [System.IO.File]::ReadAllText($envPath)
$pattern = '(?m)^[ \t]*(?:export[ \t]+)?DB_SERVER[ \t]*=[^\r\n]*'
if ([regex]::IsMatch($content, $pattern)) {
    $content = [regex]::Replace($content, $pattern, "DB_SERVER=$IpAddress")
} else {
    $content += "`r`nDB_SERVER=$IpAddress`r`n"
}
[System.IO.File]::WriteAllText($envPath, $content, [System.Text.UTF8Encoding]::new($false))
Write-Host "Updated DB_SERVER=$IpAddress in $envPath"

$previousDbServer = $env:DB_SERVER
Push-Location $PSScriptRoot
try {
    $env:DB_SERVER = $IpAddress
    & node msdb-api.js
    $nodeExitCode = $LASTEXITCODE
} finally {
    $env:DB_SERVER = $previousDbServer
    Pop-Location
}
exit $nodeExitCode