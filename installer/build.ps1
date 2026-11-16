# Builds the MSI with WiX Toolset v5.
#
#   installer/build.ps1 -Version 1.2.3[-rc.1] -Exe dist\amqp-ftp-bridge.exe -Out dist\amqp-ftp-bridge.msi
#
# Needs the wix .NET tool and its firewall and util extensions:
#   dotnet tool install --global wix --version 5.0.2
#   wix extension add --global WixToolset.Firewall.wixext/5.0.2 WixToolset.Util.wixext/5.0.2
param(
    [Parameter(Mandatory)][string]$Version,
    [Parameter(Mandatory)][string]$Exe,
    [Parameter(Mandatory)][string]$Out
)
$ErrorActionPreference = 'Stop'

# MSI versions have three numeric fields; a pre-release suffix is dropped.
if ($Version -notmatch '^(\d+)\.(\d+)\.(\d+)(-[0-9A-Za-z.-]+)?$') {
    throw "version must look like 1.2.3 or 1.2.3-rc.1: $Version"
}
if ([int]$Matches[1] -gt 255 -or [int]$Matches[2] -gt 255 -or [int]$Matches[3] -gt 65535) {
    throw "MSI versions allow 255.255.65535 at most: $Version"
}
$msiVersion = "$($Matches[1]).$($Matches[2]).$($Matches[3])"

$wxs = Join-Path $PSScriptRoot 'amqp-ftp-bridge.wxs'
wix build $wxs -arch x64 -ext WixToolset.Firewall.wixext -ext WixToolset.Util.wixext `
    -d "ProductVersion=$msiVersion" -d "ExePath=$((Resolve-Path $Exe).Path)" `
    -o $Out
if ($LASTEXITCODE -ne 0) { throw "wix build failed with exit code $LASTEXITCODE" }
Write-Host "built $Out (MSI version $msiVersion)"
