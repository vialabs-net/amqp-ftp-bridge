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
$httpRuleName = "$name http"
$allowFrom = '192.0.2.10'
$httpAllowFrom = '192.0.2.20'
$config = Resolve-Path (Join-Path $PSScriptRoot 'smoke-config.yaml')
$step = 0

function Check($ok, $message) {
    if (-not $ok) { throw "FAILED: $message" }
    Write-Host "ok: $message"
}

function Invoke-Msi([string[]]$arguments, [int[]]$allowedExitCodes = @(0, 3010)) {
    $script:step++
    $log = Join-Path $LogDir "msi-$script:step.log"
    $p = Start-Process msiexec.exe -ArgumentList ($arguments + @('/qn', '/norestart', '/l*v', "`"$log`"")) -Wait -PassThru
    Write-Host "msiexec $($arguments -join ' ') -> $($p.ExitCode)"
    if ($p.ExitCode -notin $allowedExitCodes) {
        Get-Content $log -Tail 80 | Write-Host
        throw "msiexec failed with exit code $($p.ExitCode); log: $log"
    }
}

function Get-Products {
    @(Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*' -ErrorAction SilentlyContinue |
        Where-Object { $_.PSObject.Properties['DisplayName'] -and $_.DisplayName -eq $name })
}

function Get-Rule([string]$display = $ruleName) { @(Get-NetFirewallRule -ErrorAction SilentlyContinue | Where-Object DisplayName -eq $display) }

function Test-Listening([int]$port) {
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        $client = New-Object System.Net.Sockets.TcpClient
        try { $client.Connect('127.0.0.1', $port); return $true } catch { Start-Sleep -Seconds 2 } finally { $client.Dispose() }
    }
    return $false
}

# Starts a local FTP server (pyftpdlib) with the smoke credentials. Returns
# the process and its root directory, or $null when the runner cannot run it.
function Start-LocalFtp {
    try {
        python -m pip install --quiet --disable-pip-version-check pyftpdlib 2>&1 | Write-Host
        if ($LASTEXITCODE -ne 0) { return $null }
        $tmp = if ($env:RUNNER_TEMP) { $env:RUNNER_TEMP } else { $env:TEMP }
        $root = Join-Path $tmp 'afb-ftp-root'
        New-Item -ItemType Directory -Force $root | Out-Null
        $p = Start-Process python -ArgumentList @('-m', 'pyftpdlib', '-i', '127.0.0.1', '-p', '2121', '-w', '-u', 'smoke', '-P', 'smoke-password', '-d', "`"$root`"") -PassThru -WindowStyle Hidden
        if (-not (Test-Listening 2121)) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue; return $null }
        return @{ Process = $p; Root = $root }
    } catch {
        Write-Host "local FTP server unavailable: $_"
        return $null
    }
}

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
Check (@(Get-Rule $httpRuleName).Count -eq 0) 'no HTTP firewall rule without HTTP_ALLOW_FROM'
Check (Test-Path "HKLM:\SYSTEM\CurrentControlSet\Services\EventLog\Application\$name") 'event log source registered'

# 2. Credentials, as an administrator would store them.
"smoke`nsmoke-password" | & $exe creds set rabbitmq
Check ($LASTEXITCODE -eq 0) 'creds set rabbitmq'
"smoke`nsmoke-password" | & $exe creds set ftp
Check ($LASTEXITCODE -eq 0) 'creds set ftp'
"smoke`nsmoke-password" | & $exe creds set http-client
Check ($LASTEXITCODE -eq 0) 'creds set http-client'
Assert-PrivateAcl "$data\credentials\rabbitmq.bin"

# A failed upgrade must roll back the removal of the working product.
Invoke-Msi -arguments @('/i', "`"$NewMsi`"", 'SERVICE_ACCOUNT=.\AFBMissingSmokeAccount') -allowedExitCodes @(1603)
$products = @(Get-Products)
Check ($products.Count -eq 1 -and $products[0].DisplayVersion -ne $NewVersion) 'failed upgrade restored the old product'
Check ((Get-CimInstance Win32_Service -Filter "Name='$name'").StartName -eq "NT SERVICE\$name") 'rollback restored the service account'
Copy-Item $config "$data\config.yaml"
Start-Service $name
Check (Wait-Healthy) 'rolled-back service answers /healthz'
Stop-Service $name
(Get-Service $name).WaitForStatus('Stopped', [TimeSpan]::FromSeconds(60))

# Simulate credentials written by the previous manual installer: each file
# has its own protected SYSTEM/Administrators-only ACL. Folder ACLs cannot
# migrate them through inheritance.
foreach ($file in Get-ChildItem "$data\credentials" -Filter '*.bin') {
    $acl = Get-Acl $file.FullName
    $acl.SetSecurityDescriptorSddlForm('D:P(A;;FA;;;SY)(A;;FA;;;BA)')
    Set-Acl $file.FullName $acl
}

# 3. Major upgrade with CONFIG_FILE, METRICS_ALLOW_FROM and HTTP_ALLOW_FROM: service starts.
Invoke-Msi @('/i', "`"$NewMsi`"", "CONFIG_FILE=`"$config`"", "METRICS_ALLOW_FROM=$allowFrom", "HTTP_ALLOW_FROM=$httpAllowFrom")
$products = @(Get-Products)
Check ($products.Count -eq 1) "one installed product after the upgrade (got $($products.Count))"
Check ($products[0].DisplayVersion -eq $NewVersion) "upgraded to $NewVersion (got $($products[0].DisplayVersion))"
Check ((Get-Content "$data\config.yaml" -Raw) -eq (Get-Content $config -Raw)) 'CONFIG_FILE copied'
Check (Test-Path "$data\credentials\rabbitmq.bin") 'credentials kept by the upgrade'
Assert-PrivateAcl "$data\credentials\rabbitmq.bin"
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
$httpRule = @(Get-Rule $httpRuleName)
Check ($httpRule.Count -eq 1) 'HTTP firewall rule created'
$httpAddr = ($httpRule[0] | Get-NetFirewallAddressFilter).RemoteAddress
$httpPort = ($httpRule[0] | Get-NetFirewallPortFilter)
Check (($httpAddr -join ',') -eq $httpAllowFrom) "HTTP firewall rule remote address (got $($httpAddr -join ','))"
Check ($httpPort.LocalPort -eq '8080' -and $httpPort.Protocol -eq 'TCP') "HTTP firewall rule port (got $($httpPort.Protocol) $($httpPort.LocalPort))"

# HTTP routes: the service listens; with a local FTP server, one upload and one download.
Check (Test-Listening 8080) 'service listens on the HTTP port'
$ftpServer = Start-LocalFtp
if ($ftpServer) {
    try {
        $auth = @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('smoke:smoke-password')) }
        $headers = $auth + @{ 'X-Folder' = 'smoke/in'; 'X-File-Name' = 'hello.txt' }
        $up = Invoke-WebRequest -UseBasicParsing -Method Post -Uri 'http://127.0.0.1:8080/api/upload/' -Headers $headers -Body 'hello from the smoke test' -ContentType 'application/octet-stream'
        Check ($up.StatusCode -eq 200) 'HTTP upload answers 200'
        $stored = Join-Path $ftpServer.Root 'smoke\in\hello.txt'
        Check ((Get-Content $stored -Raw) -eq 'hello from the smoke test') 'HTTP upload stored the file on the FTP server'
        $down = Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:8080/api/file/L:smoke/in/hello.txt' -Headers $auth
        $content = $down.Content
        if ($content -is [byte[]]) { $content = [Text.Encoding]::UTF8.GetString($content) }
        Check ($down.StatusCode -eq 200 -and $content -eq 'hello from the smoke test') "HTTP download returns the file (got $($down.StatusCode))"
        $missing = try { Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:8080/api/file/L:smoke/in/missing.txt' -Headers $auth | Out-Null; 200 } catch { [int]$_.Exception.Response.StatusCode }
        Check ($missing -eq 404) "HTTP download of a missing file answers 404 (got $missing)"
    } finally {
        Stop-Process -Id $ftpServer.Process.Id -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host 'skip: no local FTP server on this runner, only the HTTP port was checked'
}

# 4. Uninstall: service, program and rule removed; data kept.
Invoke-Msi @('/x', "`"$NewMsi`"")
Check (-not (Get-Service $name -ErrorAction SilentlyContinue)) 'service removed'
Check (-not (Test-Path $exe)) 'program removed'
Check (@(Get-Rule).Count -eq 0) 'firewall rule removed'
Check (@(Get-Rule $httpRuleName).Count -eq 0) 'HTTP firewall rule removed'
Check (@(Get-Products).Count -eq 0) 'product removed'
Check (-not (Test-Path "HKLM:\SYSTEM\CurrentControlSet\Services\EventLog\Application\$name")) 'event log source removed'
Check (Test-Path "$data\config.yaml") 'configuration kept'
Check (Test-Path "$data\credentials\rabbitmq.bin") 'credentials kept'
Check (@(Get-ChildItem "$data\logs" -Filter 'service-*.log').Count -ge 1) 'logs kept'

# A new local user has no service-logon right; the MSI must grant it.
$user = 'AFBServiceSmoke'
$password = [Guid]::NewGuid().ToString('N') + '!aA1'
Write-Host "::add-mask::$password"
New-LocalUser -Name $user -Password (ConvertTo-SecureString $password -AsPlainText -Force) | Out-Null
try {
    $account = "$env:COMPUTERNAME\$user"
    Invoke-Msi @('/i', "`"$NewMsi`"", "SERVICE_ACCOUNT=$account", "SERVICE_PASSWORD=$password")
    $configuredAccount = (Get-CimInstance Win32_Service -Filter "Name='$name'").StartName
    # SCM may canonicalize COMPUTER\user to .\user. Compare the actual identity.
    $lookupAccount = $configuredAccount -replace '^\.\\', "$env:COMPUTERNAME\"
    $configuredSid = ([Security.Principal.NTAccount]$lookupAccount).Translate([Security.Principal.SecurityIdentifier]).Value
    Check ($configuredSid -eq (Get-LocalUser -Name $user).SID.Value) "custom account configured (got $configuredAccount)"
    Check (Wait-Healthy) 'custom account without prior service-logon rights answers /healthz'
    Invoke-Msi @('/x', "`"$NewMsi`"")
} finally {
    Remove-LocalUser -Name $user
}
Write-Host 'MSI smoke test passed'
} finally {
    # Service logs next to the msiexec logs, for the CI artifact.
    Copy-Item "$data\logs\*.log" $LogDir -ErrorAction SilentlyContinue
}
