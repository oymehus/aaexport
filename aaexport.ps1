param(
    [Parameter(Mandatory=$true)][string]$Org,
    [Parameter(Mandatory=$true)][string]$Project,
    [Parameter(Mandatory=$true)][string]$Team,
    [Parameter(Mandatory=$true)][string]$Board,
    [Parameter(Mandatory=$true)][string]$Pat,
    [Parameter(Mandatory=$true)][string]$Output,
    [string[]]$WorkItemTypes, 
    [string[]]$AreaPaths,
    [string[]]$AdditionalFields,
    [switch]$FixDecreasingDates,
    [int]$HistoryLimit = 1000,
    [int]$ThrottleLimit = 8
)

# --- 0. Constants & Helpers ---
$apiVersion = "7.0"
$encodedProject = [Uri]::EscapeDataString($Project)
$encodedTeam    = [Uri]::EscapeDataString($Team)

$baseUrl = "https://dev.azure.com/$Org/$encodedProject"
$base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(":$($Pat)"))
$headers = @{Authorization=("Basic {0}" -f $base64AuthInfo)}

function Invoke-AdoRest {
    param([string]$Url, $Headers)
    $maxRetries = 5
    $retryCount = 0
    $completed = $false
    while (-not $completed) {
        try {
            return Invoke-RestMethod -Uri $Url -Method Get -Headers $Headers -ContentType "application/json" -ErrorAction Stop
        } catch {
            $statusCode = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
            if ($retryCount -ge $maxRetries) { Write-Error "API Call Failed ($statusCode): $($_.Exception.Message) | URL: $Url"; exit 1 }
            $wait = [Math]::Pow(2, $retryCount)
            Start-Sleep -Seconds $wait
            $retryCount++
        }
    }
}

function Get-FlowMetricsRow {
    param(
        $Id,
        $BaseUrl,
        $ApiVersion,
        $BoardColumns,
        $SplitMap,
        $FieldRefMap,
        $CalcFlags,
        $FixDecreasingDates,
        $Headers
    )

    # 1. Fetch Current State
    $wiDetail = Invoke-AdoRest -Url "$BaseUrl/_apis/wit/workitems/$($Id)?`$expand=None&api-version=$ApiVersion" -Headers $Headers
    
    $rawTags = $wiDetail.fields."System.Tags"
    $formattedTags = ""
    if (-not [string]::IsNullOrWhiteSpace($rawTags)) {
        $tagList = $rawTags -split ";" | ForEach-Object { $_.Trim() }
        $formattedTags = "[" + ($tagList -join "|") + "]"
    }

    $rowMap = [ordered]@{
        "ID" = $wiDetail.id
        "Link" = $wiDetail._links.html.href
        "Title" = $wiDetail.fields."System.Title"
        "Work Item Type" = $wiDetail.fields."System.WorkItemType"
        "Tags" = $formattedTags
    }

    # 2. Dynamic Field Injection
    $fullAreaPath = $wiDetail.fields."System.AreaPath"
    $areaParts = $fullAreaPath -split '\\'

    if ($CalcFlags["NodeName"]) { $rowMap["Node Name"] = $areaParts[-1] }

    if ($CalcFlags["AreaHierarchy"]) {
        $rowMap["Area Level 1"] = if ($areaParts.Count -gt 0) { $areaParts[0] } else { "" }
        $rowMap["Area Level 2"] = if ($areaParts.Count -gt 1) { $areaParts[1] } else { "" }
        $rowMap["Area Level 3"] = if ($areaParts.Count -gt 2) { $areaParts[2] } else { "" }
        $rowMap["Area Level 4"] = if ($areaParts.Count -gt 3) { $areaParts[3] } else { "" }
        $rowMap["Area Level 5"] = if ($areaParts.Count -gt 4) { $areaParts[4] } else { "" }
        $rowMap["Area Level 6"] = if ($areaParts.Count -gt 5) { $areaParts[5] } else { "" }
        $rowMap["Area Level 7"] = if ($areaParts.Count -gt 6) { $areaParts[6] } else { "" }
    }

    foreach ($h in $FieldRefMap.Keys) {
        $rowMap[$h] = $wiDetail.fields."$($FieldRefMap[$h])"
    }

    $rowMap["State"] = $wiDetail.fields."System.State"
    $rowMap["Area Path"] = $fullAreaPath
    $rowMap["Blocked"] = $wiDetail.fields."Microsoft.VSTS.CMMI.Blocked"
    $rowMap["Blocked Days"] = 0

    foreach ($col in $BoardColumns) { $rowMap[$col] = $null }

    # 3. History Replay
    $updates = Invoke-AdoRest -Url "$BaseUrl/_apis/wit/workitems/$($Id)/updates?api-version=$ApiVersion" -Headers $Headers
    $currentColName = $null; $currentIsDone = $false; $totalBlockedDays = 0; $blockedStartDate = $null; $isCurrentlyBlocked = $false

    $updates.value | Sort-Object -Property "rev" | ForEach-Object {
        $update = $_
        $changeDateVal = $update.fields."System.ChangedDate".newValue
        $currentDate = if ($changeDateVal) { [DateTime]$changeDateVal } else { $null }

        if ($update.fields -and $update.fields."Microsoft.VSTS.CMMI.Blocked") {
            $blockedStatus = $update.fields."Microsoft.VSTS.CMMI.Blocked".newValue
            if ($blockedStatus -eq "Yes") { $blockedStartDate = $currentDate; $isCurrentlyBlocked = $true } 
            elseif ($blockedStatus -eq "No" -and $isCurrentlyBlocked -and $blockedStartDate) {
                if ($currentDate) {
                    $days = ($currentDate.Date - $blockedStartDate.Date).Days
                    if ($days -gt 0) { $totalBlockedDays += $days }
                }
                $isCurrentlyBlocked = $false; $blockedStartDate = $null
            }
        }
        $hasColChange = $false
        if ($update.fields -and $update.fields."System.BoardColumn") { $currentColName = $update.fields."System.BoardColumn".newValue; $currentIsDone = $false; $hasColChange = $true }
        if ($update.fields -and $update.fields."System.BoardColumnDone") { $currentIsDone = [bool]$update.fields."System.BoardColumnDone".newValue; $hasColChange = $true }

        if ($hasColChange -and $currentColName) {
            $targetHeader = $currentColName
            if ($currentIsDone -and $SplitMap[$currentColName]) { $targetHeader = "$currentColName Done" }
            if ($BoardColumns -contains $targetHeader -and -not $rowMap[$targetHeader]) {
                if ($currentDate) { $rowMap[$targetHeader] = $currentDate.ToString("yyyy-MM-dd") } 
            }
        }
    }

    if ($isCurrentlyBlocked -and $blockedStartDate) {
        $now = Get-Date
        $days = ($now.Date - $blockedStartDate.Date).Days
        if ($days -gt 0) { $totalBlockedDays += $days }
    }
    $rowMap["Blocked Days"] = $totalBlockedDays

    $createdDateVal = $wiDetail.fields."System.CreatedDate"
    if ($createdDateVal) {
        $createdDate = [DateTime]$createdDateVal
        $firstCol = $BoardColumns[0]
        if (-not $rowMap[$firstCol]) { $rowMap[$firstCol] = $createdDate.ToString("yyyy-MM-dd") }
    }

    if ($FixDecreasingDates) {
        $maxDate = [DateTime]::MinValue
        foreach ($col in $BoardColumns) {
            $dateStr = $rowMap[$col]
            if (-not [string]::IsNullOrWhiteSpace($dateStr)) {
                $colDate = [DateTime]$dateStr
                if ($colDate -lt $maxDate) { $rowMap[$col] = "" } else { $maxDate = $colDate }
            }
        }
    }
    return $rowMap
}

Write-Host "Connecting to Azure DevOps organization: $Org" -ForegroundColor Cyan

# --- 1. Get Board Configuration ---
Write-Host "Fetching board configuration for '$Board'..."
$boards = Invoke-AdoRest -Url "$baseUrl/$encodedTeam/_apis/work/boards?api-version=$apiVersion" -Headers $headers
$boardConfig = $boards.value | Where-Object { $_.name -eq $Board }

if (-not $boardConfig) { Write-Error "Board '$Board' not found."; exit 1 }

$columns = Invoke-AdoRest -Url "$($boardConfig.url)/columns?api-version=$apiVersion" -Headers $headers
$boardColumns = @(); $splitMap = @{}
foreach ($col in $columns.value) {
    $boardColumns += $col.name
    if ($col.isSplit) { $splitMap[$col.name] = $true; $boardColumns += "$($col.name) Done" }
}
Write-Host "Found Columns: $($boardColumns -join ' -> ')" -ForegroundColor Green

# --- 2. Configure Headers ---
$extraHeaders = @(); $fieldRefMap = @{}; $calcFlags = @{ "AreaHierarchy" = $false; "NodeName" = $false }
foreach ($fieldDef in $AdditionalFields) {
    if ($fieldDef -eq "AreaHierarchy") { $calcFlags["AreaHierarchy"] = $true; $extraHeaders += @("Area Level 1", "Area Level 2", "Area Level 3", "Area Level 4", "Area Level 5", "Area Level 6", "Area Level 7"); continue }
    if ($fieldDef -eq "NodeName") { $calcFlags["NodeName"] = $true; $extraHeaders += "Node Name"; continue }
    if ($fieldDef -match "=") { $p = $fieldDef -split "="; $h = $p[0].Trim(); $r = $p[1].Trim(); $extraHeaders += $h; $fieldRefMap[$h] = $r }
    else { $extraHeaders += $fieldDef; $fieldRefMap[$fieldDef] = $fieldDef }
}

# --- 3. Fetch Work Items ---
Write-Host "Fetching work items..."
$typeWhere = ""
$cleanTypes = $WorkItemTypes | ForEach-Object { $_ -split "," } | ForEach-Object { $_.Trim() } | Where-Object { $_ }
if ($cleanTypes.Count -gt 0) { $formattedList = ($cleanTypes | ForEach-Object { "'$_'" }) -join ","; $typeWhere = "AND [System.WorkItemType] IN ($formattedList)" } 
else {
    $backlogs = Invoke-AdoRest -Url "$baseUrl/$encodedTeam/_apis/work/backlogs?api-version=$apiVersion" -Headers $headers
    $backlogLevel = $backlogs.value | Where-Object { $_.name -eq $Board }
    if (-not $backlogLevel) { $backlogLevel = $backlogs.value | Where-Object { $_.id -eq $boardConfig.id } }
    if ($backlogLevel) { $category = $backlogLevel.categoryReferenceName; $typeWhere = "AND [System.WorkItemType] IN GROUP '$category'" } 
    else { Write-Error "Backlog Level not found."; exit 1 }
}

$areaWhere = ""; $targetAreas = @(); $cleanAreas = $AreaPaths | ForEach-Object { $_ -split "," } | ForEach-Object { $_.Trim().TrimStart('\') } | Where-Object { $_ }
if ($cleanAreas.Count -gt 0) { $targetAreas = $cleanAreas }
else {
    $teamSettings = Invoke-AdoRest -Url "$baseUrl/$encodedTeam/_apis/work/teamsettings/teamfieldvalues?api-version=$apiVersion" -Headers $headers
    foreach ($val in $teamSettings.values) { $targetAreas += $val.value }
}
if ($targetAreas.Count -gt 0) { $areaClauses = $targetAreas | ForEach-Object { "[System.AreaPath] UNDER '$_'" }; $areaWhere = "AND ( " + ($areaClauses -join " OR ") + " )" }

$wiql = "SELECT [System.Id] FROM WorkItems WHERE [System.TeamProject] = '$Project' $typeWhere $areaWhere ORDER BY [System.ChangedDate] DESC"
$queryResponse = Invoke-RestMethod -Uri "$baseUrl/_apis/wit/wiql?api-version=$apiVersion" -Method Post -Headers $headers -Body (@{ query = $wiql } | ConvertTo-Json) -ContentType "application/json"
$workItems = $queryResponse.workItems | Select-Object -First $HistoryLimit


# --- 4. Process Loop (Conditional Parallelism) ---
$psVersion = $PSVersionTable.PSVersion.Major
$results = @()

if ($psVersion -ge 7) {
    Write-Host "Found $($workItems.Count) work items. Processing in Parallel (PS v$psVersion, $ThrottleLimit threads)..." -ForegroundColor Yellow
    
    # Capture functions for thread hydration
    $funcInvokeRest = ${function:Invoke-AdoRest}.ToString()
    $funcGetRow = ${function:Get-FlowMetricsRow}.ToString()

    $results = $workItems | ForEach-Object -Parallel {
        ${function:Invoke-AdoRest} = $using:funcInvokeRest
        ${function:Get-FlowMetricsRow} = $using:funcGetRow
        
        $row = Get-FlowMetricsRow `
            -Id $_.id `
            -BaseUrl $using:baseUrl `
            -ApiVersion $using:apiVersion `
            -BoardColumns $using:boardColumns `
            -SplitMap $using:splitMap `
            -FieldRefMap $using:fieldRefMap `
            -CalcFlags $using:calcFlags `
            -FixDecreasingDates $using:FixDecreasingDates `
            -Headers $using:headers

        return [PSCustomObject]$row
    } -ThrottleLimit $ThrottleLimit
} 
else {
    Write-Host "Found $($workItems.Count) work items. Processing sequentially (PS v$psVersion)..." -ForegroundColor Yellow
    $current = 0
    $total = $workItems.Count
    
    foreach ($item in $workItems) {
        $current++
        # Direct call (Sequential)
        $row = Get-FlowMetricsRow `
            -Id $item.id `
            -BaseUrl $baseUrl `
            -ApiVersion $apiVersion `
            -BoardColumns $boardColumns `
            -SplitMap $splitMap `
            -FieldRefMap $fieldRefMap `
            -CalcFlags $calcFlags `
            -FixDecreasingDates $FixDecreasingDates `
            -Headers $headers

        $results += [PSCustomObject]$row
        Write-Progress -Activity "Processing Work Items" -Status "ID: $($item.id)" -PercentComplete (($current / $total) * 100)
    }
}


# --- 5. Export JSON ---
Write-Host "Constructing JSON Matrix..." -ForegroundColor Cyan
$finalHeaders = @("ID", "Link", "Title", "Work Item Type", "Tags") + $extraHeaders + @("State", "Area Path") + $boardColumns + @("Blocked", "Blocked Days")
$jsonRows = @()
function Format-JsonStr { param($s) return '"' + $s.ToString().Replace('\', '\\').Replace('"', '\"') + '"' }

$headerStr = "[" + (($finalHeaders | ForEach-Object { Format-JsonStr $_ }) -join ",") + "]"
$jsonRows += $headerStr

foreach ($item in $results) {
    $rowValues = @()
    foreach ($h in $finalHeaders) {
        $val = $item.$h
        if ($null -eq $val) { $val = "" }
        $rowValues += Format-JsonStr $val
    }
    $jsonRows += "[" + ($rowValues -join ",") + "]"
}

$finalJson = "[" + [Environment]::NewLine + ($jsonRows -join "," + [Environment]::NewLine) + [Environment]::NewLine + "]"
$finalJson | Set-Content -Path $Output -Encoding UTF8
Write-Host "Export complete! JSON saved to: $Output" -ForegroundColor Green