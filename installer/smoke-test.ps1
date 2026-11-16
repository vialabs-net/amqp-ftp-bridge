# Install, upgrade, service start and uninstall test of the MSI. Run as an
# administrator on a disposable Windows machine (CI runner): it installs a
# service and changes the firewall.
#
#   installer/smoke-test.ps1 -OldMsi dist\old.msi -NewMsi dist\new.msi -NewVersion 0.0.2
param(
    [Parameter(Mandatory)][string]$OldMsi,
    [Parameter(Mandatory)][string]$NewMsi,
    [Parameter(Mandatory)][string]$NewVersion,
    [string]$LogDir = 'msi-logs'
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$OldMsi = (Resolve-Path $OldMsi).Path
$NewMsi = (Resolve-Path $NewMsi).Path
New-Item -ItemType Directory -Force $LogDir | Out-Null
$LogDir = (Resolve-Path $LogDir).Path

$name = 'amqp-ftp-bridge'
$exe = Join-Path $env:ProgramFiles "$name\$name.exe"
$data = Join-Path $env:ProgramData $name
$ruleName = "$name metrics"
$allowFrom = '192.0.2.10'
$config = Resolve-Path (Join-Path $PSScriptRoot 'smoke-config.yaml')
$step = 0

function Check($ok, $message) {
    if (-not $ok) { throw "FAILED: $message" }
    Write-Host "ok: $message"
}

function Invoke-Msi([string[]]$arguments) {
    $script:step++
    $log = Join-Path $LogDir "msi-$script:step.log"
    $p = Start-Process msiexec.exe -ArgumentList ($arguments + @('/qn', '/norestart', '/l*v', "`"$log`"")) -Wait -PassThru
    Write-Host "msiexec $($arguments -join ' ') -> $($p.ExitCode)"
    if ($p.ExitCode -notin 0, 3010) {
        Get-Content $log -Tail 80 | Write-Host
        throw "msiexec failed with exit code $($p.ExitCode); log: $log"
    }
}

function Get-Products {
    @(Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*' -ErrorAction SilentlyContinue |
        Where-Object { $_.PSObject.Properties['DisplayName'] -and $_.DisplayName -eq $name })
}

function Get-Rule { @(Get-NetFirewallRule -ErrorAction SilentlyContinue | Where-Object DisplayName -eq $ruleName) }

function Assert-PrivateAcl([string]$path) {
    $svcSid = ([Security.Principal.NTAccount]"NT SERVICE\$name").Translate([Security.Principal.SecurityIdentifier]).Value
    $allowed = @('S-1-5-18', 'S-1-5-32-544', $svcSid)
    $acl = Get-Acl $path
    Check $acl.AreAccessRulesProtected "$path does not inherit permissions"
    foreach ($rule in $acl.Access) {
        $sid = $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value
        Check ($allowed -contains $sid) "$path grants $($rule.IdentityReference) ($($rule.FileSystemRights)) only as an allowed principal"
    }
}

function Wait-Healthy {
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 'http://127.0.0.1:9464/healthz'
            if ($r.StatusCode -eq 200) { return $true }
        } catch { Start-Sleep -Seconds 2 }
    }
    return $false
}

Check (-not (Get-Service $name -ErrorAction SilentlyContinue)) 'no service before the install'

try {

# 1. Fresh install without configuration: service registered, not started.
Invoke-Msi @('/i', "`"$OldMsi`"")
$svc = Get-Service $name
$cim = Get-CimInstance Win32_Service -Filter "Name='$name'"
Check (Test-Path $exe) "installed to $exe"
Check ($svc.StartType -eq 'Automatic') 'automatic start'
Check ($svc.Status -eq 'Stopped') 'not started without a configuration'
Check ($svc.DisplayName -eq 'AMQP FTP Bridge') 'display name'
Check ($cim.Description -like 'Consumes RabbitMQ queues*') 'description'
Check ($cim.StartName -eq "NT SERVICE\$name") "virtual service account (got $($cim.StartName))"
Check ($cim.PathName -like "*run --config*$name\config.yaml*") "service arguments (got $($cim.PathName))"
$failure = (sc.exe qfailure $name) -join "`n"
Check (([regex]::Matches($failure, 'RESTART -- Delay = 10000')).Count -eq 3) "restart after 10 s on failure`n$failure"
Check ((sc.exe qfailureflag $name) -join ' ' -match 'TRUE') 'recovery also on non-crash failures'
Check ((sc.exe qsidtype $name) -join ' ' -match 'UNRESTRICTED') 'unrestricted service SID'
$info = (Get-Item $exe).VersionInfo
Check ($info.CompanyName -eq 'vialabs' -and $info.ProductName -eq $name) "version resource ($($info.CompanyName) / $($info.ProductName) $($info.ProductVersion))"
foreach ($dir in @($data, "$data\logs", "$data\credentials", "$data\tmp")) {
    Check (Test-Path $dir) "$dir exists"
    Assert-PrivateAcl $dir
}
Check (@(Get-Rule).Count -eq 0) 'no firewall rule without METRICS_ALLOW_FROM'
Check (Test-Path "HKLM:\SYSTEM\CurrentControlSet\Services\EventLog\Application\$name") 'event log source registered'

# 2. Credentials, as an administrator would store them.
"smoke`nsmoke-password" | & $exe creds set rabbitmq
Check ($LASTEXITCODE -eq 0) 'creds set rabbitmq'
"smoke`nsmoke-password" | & $exe creds set ftp
Check ($LASTEXITCODE -eq 0) 'creds set ftp'
Assert-PrivateAcl "$data\credentials\rabbitmq.bin"

# 3. Major upgrade with CONFIG_FILE and METRICS_ALLOW_FROM: service starts.
Invoke-Msi @('/i', "`"$NewMsi`"", "CONFIG_FILE=`"$config`"", "METRICS_ALLOW_FROM=$allowFrom")
$products = @(Get-Products)
Check ($products.Count -eq 1) "one installed product after the upgrade (got $($products.Count))"
Check ($products[0].DisplayVersion -eq $NewVersion) "upgraded to $NewVersion (got $($products[0].DisplayVersion))"
Check ((Get-Content "$data\config.yaml" -Raw) -eq (Get-Content $config -Raw)) 'CONFIG_FILE copied'
Check (Test-Path "$data\credentials\rabbitmq.bin") 'credentials kept by the upgrade'
Check ((Get-CimInstance Win32_Service -Filter "Name='$name'").StartName -eq "NT SERVICE\$name") 'service account kept'
(Get-Service $name).WaitForStatus('Running', [TimeSpan]::FromSeconds(60))
Check (Wait-Healthy) 'service answers /healthz (configuration and credentials readable)'
$servicePid = (Get-CimInstance Win32_Service -Filter "Name='$name'").ProcessId
Start-Sleep -Seconds 15
$after = Get-CimInstance Win32_Service -Filter "Name='$name'"
Check ($after.State -eq 'Running' -and $after.ProcessId -eq $servicePid) 'service keeps running'
Check (@(Get-ChildItem "$data\logs" -Filter 'service-*.log').Count -ge 1) 'service writes its log'
$rule = @(Get-Rule)
Check ($rule.Count -eq 1) 'firewall rule created'
$addr = ($rule[0] | Get-NetFirewallAddressFilter).RemoteAddress
$port = ($rule[0] | Get-NetFirewallPortFilter)
Check (($addr -join ',') -eq $allowFrom) "firewall rule remote address (got $($addr -join ','))"
Check ($port.LocalPort -eq '9464' -and $port.Protocol -eq 'TCP') "firewall rule port (got $($port.Protocol) $($port.LocalPort))"
Check ($rule[0].Direction -eq 'Inbound' -and $rule[0].Action -eq 'Allow') 'firewall rule allows inbound'

# 4. Uninstall: service, program and rule removed; data kept.
Invoke-Msi @('/x', "`"$NewMsi`"")
Check (-not (Get-Service $name -ErrorAction SilentlyContinue)) 'service removed'
Check (-not (Test-Path $exe)) 'program removed'
Check (@(Get-Rule).Count -eq 0) 'firewall rule removed'
Check (@(Get-Products).Count -eq 0) 'product removed'
Check (-not (Test-Path "HKLM:\SYSTEM\CurrentControlSet\Services\EventLog\Application\$name")) 'event log source removed'
Check (Test-Path "$data\config.yaml") 'configuration kept'
Check (Test-Path "$data\credentials\rabbitmq.bin") 'credentials kept'
Check (@(Get-ChildItem "$data\logs" -Filter 'service-*.log').Count -ge 1) 'logs kept'
Write-Host 'MSI smoke test passed'
} finally {
    # Service logs next to the msiexec logs, for the CI artifact.
    Copy-Item "$data\logs\*.log" $LogDir -ErrorAction SilentlyContinue
}
