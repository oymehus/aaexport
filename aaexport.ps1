param(
    [Parameter(Mandatory=$true)][string]$Org,
    [Parameter(Mandatory=$true)][string]$Project,
    [Parameter(Mandatory=$true)][string]$Team,
    [Parameter(Mandatory=$true)][string]$Board,
    [Parameter(Mandatory=$true)][string]$Pat,
    [Parameter(Mandatory=$true)][string]$Output,
    [ValidateSet('json', 'csv', 'excel')][string]$Format = 'json',
    [string[]]$WorkItemTypes, 
    [string[]]$AreaPaths,
    [string[]]$AdditionalFields,
    [switch]$FixDecreasingDates,
    [switch]$IncrementalUpdate,
    [switch]$ChildCount,
    [int]$HistoryLimit = 1000,
    [int]$ThrottleLimit = 8
)

# --- Auto-Correct File Extensions based on Format ---
if ($Format -eq 'excel' -and $Output -match '\.(json|csv)$') { $Output = $Output -replace '\.(json|csv)$', '.xlsx' }
elseif ($Format -eq 'csv' -and $Output -match '\.(json|xlsx)$') { $Output = $Output -replace '\.(json|xlsx)$', '.csv' }
elseif ($Format -eq 'json' -and $Output -match '\.(csv|xlsx)$') { $Output = $Output -replace '\.(csv|xlsx)$', '.json' }

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

function Add-AreaNodePaths {
    param(
        $Node,
        [hashtable]$AreaPathMap
    )

    # Paths are composed from node names rather than taken from node.path,
    # because the classification API returns paths that include the "Area" root
    # segment (\Project\Area\Team) which work item System.AreaPath values omit
    # (Project\Team). Composing from names also avoids localisation of that
    # root segment.
    # The walk is iterative to avoid recursion limits on deep trees and the
    # PowerShell pitfall where @($node.children) on a leaf yields one $null item.
    if ($null -eq $Node) { return }

    $stack = [System.Collections.Stack]::new()
    $stack.Push([pscustomobject]@{ Node = $Node; Path = $null })

    while ($stack.Count -gt 0) {
        $entry = $stack.Pop()
        $current = $entry.Node
        if ($null -eq $current) { continue }

        $name = [string]$current.name
        $path = if ([string]::IsNullOrEmpty($entry.Path)) { $name } else { "$($entry.Path)\$name" }

        if ($null -ne $current.id -and -not [string]::IsNullOrWhiteSpace($path)) {
            $AreaPathMap[[string]$current.id] = $path
        }

        $children = $current.children
        if ($null -ne $children) {
            foreach ($child in $children) {
                if ($null -ne $child) {
                    $stack.Push([pscustomobject]@{ Node = $child; Path = $path })
                }
            }
        }
    }
}

function Get-AreaPathMap {
    param(
        $BaseUrl,
        $ApiVersion,
        $Headers
    )

    # Resolved directly (not via Invoke-AdoRest) so that a failure here degrades
    # gracefully instead of terminating the whole export: Invoke-AdoRest calls
    # exit after retries, which would abort the run before any file is written.
    $areaPathMap = @{}
    try {
        $areaTree = Invoke-RestMethod -Uri "$BaseUrl/_apis/wit/classificationnodes/areas?`$depth=14&api-version=$ApiVersion" -Method Get -Headers $Headers -ContentType "application/json" -ErrorAction Stop
        Add-AreaNodePaths -Node $areaTree -AreaPathMap $areaPathMap
    }
    catch {
        Write-Warning "Could not load the Area Path classification tree ($($_.Exception.Message)). Area Path values will use each work item's System.AreaPath as-is."
        return @{}
    }

    return $areaPathMap
}

function Resolve-AreaPath {
    param(
        $Fields,
        [hashtable]$AreaPathMap,
        $WorkItemId
    )

    # System.AreaId is only returned when explicitly requested via the fields=
    # query option, which cannot be combined with $expand. When it is absent the
    # work item was read live, so System.AreaPath is already canonical and is
    # used as-is. The id lookup matters for rows reused from the incremental
    # cache, where a moved classification node leaves the stored path stale.
    $areaId = [string]$Fields."System.AreaId"
    if ($areaId) {
        if ($AreaPathMap.ContainsKey($areaId)) {
            return $AreaPathMap[$areaId]
        }
        if ($AreaPathMap.Count -gt 0) {
            Write-Warning "Could not resolve Area ID '$areaId' for work item $WorkItemId from the current classification tree. Using System.AreaPath."
        }
    }

    return $Fields."System.AreaPath"
}

function Set-RowValue {
    param(
        $Row,
        [string]$Name,
        $Value
    )

    if ($Row -is [System.Collections.IDictionary]) {
        $Row[$Name] = $Value
        return
    }

    $property = $Row.PSObject.Properties[$Name]
    if ($property) {
        $property.Value = $Value
    }
}

function Set-AreaPathMetadata {
    param(
        $RowMap,
        [string]$AreaPath,
        [System.Collections.IDictionary]$CalcFlags
    )

    $areaParts = $AreaPath -split '\\'

    if ($CalcFlags["NodeName"]) { Set-RowValue -Row $RowMap -Name "Node Name" -Value $areaParts[-1] }

    if ($CalcFlags["AreaHierarchy"]) {
        for ($i = 0; $i -lt 7; $i++) {
            $value = if ($areaParts.Count -gt $i) { $areaParts[$i] } else { "" }
            Set-RowValue -Row $RowMap -Name "Area Level $($i + 1)" -Value $value
        }
    }

    Set-RowValue -Row $RowMap -Name "Area Path" -Value $AreaPath
}

function Get-ExistingCache {
    param($Path, $TargetHeaders, $Format)
    
    if (-not (Test-Path $Path)) { return $null }

    try {
        Write-Host "Reading existing file for incremental comparison..." -ForegroundColor Cyan
        $cache = @{}

        if ($Format -eq 'json') {
            $jsonContent = Get-Content -Path $Path -Raw -Encoding UTF8 | ConvertFrom-Json
            if ($jsonContent.Count -lt 2) { return $null } 

            $fileHeaders = $jsonContent[0]
            if (($fileHeaders -join "|") -ne ($TargetHeaders -join "|")) { 
                Write-Warning "Schema mismatch detected. Forcing full reload."
                return $null 
            }

            $idIndex = $fileHeaders.IndexOf("ID")
            $changeIndex = $fileHeaders.IndexOf("Changed Date")

            if ($idIndex -eq -1 -or $changeIndex -eq -1) {
                Write-Warning "Existing file missing ID or Changed Date. Forcing full reload."
                return $null
            }

            for ($i = 1; $i -lt $jsonContent.Count; $i++) {
                $row = $jsonContent[$i]
                $objMap = [ordered]@{}
                for ($c = 0; $c -lt $fileHeaders.Count; $c++) { $objMap[$fileHeaders[$c]] = $row[$c] }
                $cache[$row[$idIndex]] = @{ Data = [PSCustomObject]$objMap; ChangedDate = $row[$changeIndex] }
            }
        }
        elseif ($Format -eq 'csv') {
            $csvContent = Import-Csv -Path $Path -Encoding UTF8
            if (-not $csvContent -or $csvContent.Count -eq 0) { return $null }
            
            $fileHeaders = $csvContent[0].psobject.properties.name
            if (($fileHeaders -join "|") -ne ($TargetHeaders -join "|")) { 
                Write-Warning "Schema mismatch detected. Forcing full reload."
                return $null 
            }

            foreach ($row in $csvContent) {
                $cache[$row.ID] = @{ Data = $row; ChangedDate = $row."Changed Date" }
            }
        }
        else {
            Write-Warning "Incremental updates cannot read existing Excel (.xlsx) files directly. Starting fresh."
            return $null
        }

        return $cache
    }
    catch {
        Write-Warning "Could not parse existing file. Starting fresh. Error: $($_.Exception.Message)"
        return $null
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
        $ColFieldRef,
        $DoneFieldRef,
        $AreaPathMap,
        $IncludeChildCount,
        $Headers
    )

    # 1. Fetch Current State
    $expandParam = if ($IncludeChildCount) { "Relations" } else { "None" }
    $wiDetail = Invoke-AdoRest -Url "$BaseUrl/_apis/wit/workitems/$($Id)?`$expand=$expandParam&api-version=$ApiVersion" -Headers $Headers
    
    $rawTags = $wiDetail.fields."System.Tags"
    $formattedTags = ""
    if (-not [string]::IsNullOrWhiteSpace($rawTags)) {
        $tagList = $rawTags -split ";" | ForEach-Object { $_.Trim() }
        $formattedTags = "[" + ($tagList -join "|") + "]"
    }

    $changedDateStr = ""
    if ($wiDetail.fields."System.ChangedDate") {
        try { 
            $changedDateStr = ([DateTime]$wiDetail.fields."System.ChangedDate").ToString("yyyy-MM-dd") 
        } catch { 
            $changedDateStr = $wiDetail.fields."System.ChangedDate" 
        }
    }

    $rowMap = [ordered]@{
        "ID" = $wiDetail.id
        "Link" = $wiDetail._links.html.href
        "Title" = $wiDetail.fields."System.Title"
        "Work Item Type" = $wiDetail.fields."System.WorkItemType"
        "Tags" = $formattedTags
        "Changed Date" = $changedDateStr 
    }

    # 2. Dynamic Field Injection
    $fullAreaPath = Resolve-AreaPath -Fields $wiDetail.fields -AreaPathMap $AreaPathMap -WorkItemId $wiDetail.id
    Set-AreaPathMetadata -RowMap $rowMap -AreaPath $fullAreaPath -CalcFlags $CalcFlags

    foreach ($h in $FieldRefMap.Keys) {
        $val = $wiDetail.fields."$($FieldRefMap[$h])"
        if ($val -match '^\d{4}-\d{2}-\d{2}T') {
            try { $val = ([DateTime]$val).ToString("yyyy-MM-dd") } catch {}
        }
        $rowMap[$h] = $val
    }

    $rowMap["State"] = $wiDetail.fields."System.State"
    $rowMap["Blocked"] = $wiDetail.fields."Microsoft.VSTS.CMMI.Blocked"
    $rowMap["Blocked Days"] = 0

    if ($IncludeChildCount) {
        $cCount = 0
        if ($wiDetail.relations) {
            $cCount = @($wiDetail.relations | Where-Object { $_.rel -eq 'System.LinkTypes.Hierarchy-Forward' }).Count
        }
        $rowMap["ChildCount"] = $cCount
    }

    foreach ($col in $BoardColumns) { $rowMap[$col] = $null }

    # 3. History Replay (With WEF Field Detection)
    $updates = Invoke-AdoRest -Url "$BaseUrl/_apis/wit/workitems/$($Id)/updates?api-version=$ApiVersion" -Headers $Headers
    
    $currentColName = $null
    $currentIsDone = $false
    $totalBlockedDays = 0
    $blockedStartDate = $null
    $isCurrentlyBlocked = $false
    $maxColIndexReached = -1

    $updates.value | Sort-Object -Property "rev" | ForEach-Object {
        $update = $_
        $changeDateVal = $update.fields."System.ChangedDate".newValue
        $currentDate = if ($changeDateVal) { [DateTime]$changeDateVal } else { $null }

        # Blocked Logic
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

        # Column Logic - Checking both WEF fields and System fields
        $hasColChange = $false
        
        $colUpdate = if ($update.fields."$ColFieldRef") { $update.fields."$ColFieldRef" } else { $update.fields."System.BoardColumn" }
        if ($colUpdate) { 
            $currentColName = $colUpdate.newValue
            $currentIsDone = $false
            $hasColChange = $true 
        }
        
        $doneUpdate = if ($update.fields."$DoneFieldRef") { $update.fields."$DoneFieldRef" } else { $update.fields."System.BoardColumnDone" }
        if ($doneUpdate) { 
            $currentIsDone = [bool]$doneUpdate.newValue
            $hasColChange = $true 
        }

        if ($hasColChange -and $currentColName) {
            $targetHeader = $currentColName
            if ($currentIsDone -and $SplitMap[$currentColName]) { $targetHeader = "$currentColName Done" }
            
            $targetIndex = $BoardColumns.IndexOf($targetHeader)

            if ($targetIndex -ge 0) {
                if (-not $rowMap[$targetHeader]) {
                    if ($currentDate) { $rowMap[$targetHeader] = $currentDate.ToString("yyyy-MM-dd") } 
                }
                if ($targetIndex -lt $maxColIndexReached) {
                    for ($i = $targetIndex + 1; $i -le $maxColIndexReached; $i++) {
                        $colToClear = $BoardColumns[$i]
                        $rowMap[$colToClear] = $null
                    }
                    $maxColIndexReached = $targetIndex
                } 
                else {
                    $maxColIndexReached = $targetIndex
                }
            }
        }
    }

    # 3.5. Live State Anchor
    # If the history replay missed the final column placement (e.g. State change bypass), force it here.
    $liveCol = if ($wiDetail.fields."$ColFieldRef") { $wiDetail.fields."$ColFieldRef" } else { $wiDetail.fields."System.BoardColumn" }
    $liveDone = if ($null -ne $wiDetail.fields."$DoneFieldRef") { $wiDetail.fields."$DoneFieldRef" } else { $wiDetail.fields."System.BoardColumnDone" }
    
    if ($liveCol) {
        $liveTarget = $liveCol
        if ($liveDone -and $SplitMap[$liveCol]) { $liveTarget = "$liveCol Done" }
        
        $liveTargetIndex = $BoardColumns.IndexOf($liveTarget)
        if ($liveTargetIndex -ge 0 -and -not $rowMap[$liveTarget]) {
            $anchorDateVal = $wiDetail.fields."Microsoft.VSTS.Common.StateChangeDate"
            if (-not $anchorDateVal) { $anchorDateVal = $wiDetail.fields."System.ChangedDate" }
            if ($anchorDateVal) {
                try {
                    $rowMap[$liveTarget] = ([DateTime]$anchorDateVal).ToString("yyyy-MM-dd")
                } catch {
                    $rowMap[$liveTarget] = $anchorDateVal
                }
                if ($liveTargetIndex -gt $maxColIndexReached) { $maxColIndexReached = $liveTargetIndex }
            }
        }
    }

    if ($isCurrentlyBlocked -and $blockedStartDate) {
        $now = Get-Date
        $days = ($now.Date - $blockedStartDate.Date).Days
        if ($days -gt 0) { $totalBlockedDays += $days }
    }
    $rowMap["Blocked Days"] = $totalBlockedDays


    # 4. Date Fix: Backward-Fill (ActionableAgile Compliance)
    if ($FixDecreasingDates) {
        $runningMinDate = [DateTime]::MaxValue

        # Sweep right-to-left
        for ($i = $BoardColumns.Count - 1; $i -ge 0; $i--) {
            $colName = $BoardColumns[$i]
            $thisDateStr = $rowMap[$colName]

            if ([string]::IsNullOrWhiteSpace($thisDateStr)) {
                # If column is empty but we have a downstream date, back-fill it
                if ($runningMinDate -lt [DateTime]::MaxValue) {
                    $rowMap[$colName] = $runningMinDate.ToString("yyyy-MM-dd")
                }
            }
            else {
                $thisDate = [DateTime]$thisDateStr
                # Enforce monotonicity: An upstream date cannot be newer than a downstream date
                if ($runningMinDate -lt [DateTime]::MaxValue -and $thisDate -gt $runningMinDate) {
                    $rowMap[$colName] = $runningMinDate.ToString("yyyy-MM-dd")
                } else {
                    # Establish new anchor
                    $runningMinDate = $thisDate
                }
            }
        }

        # Fallback for the initial column if empty
        if ([string]::IsNullOrWhiteSpace($rowMap[$BoardColumns[0]])) {
            if ($wiDetail.fields."System.CreatedDate") {
                $rowMap[$BoardColumns[0]] = ([DateTime]$wiDetail.fields."System.CreatedDate").ToString("yyyy-MM-dd")
            }
        }
    } 
    else {
        $createdDateVal = $wiDetail.fields."System.CreatedDate"
        if ($createdDateVal) {
            $createdDate = [DateTime]$createdDateVal
            $firstCol = $BoardColumns[0]
            if (-not $rowMap[$firstCol]) { $rowMap[$firstCol] = $createdDate.ToString("yyyy-MM-dd") }
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

$areaPathMap = Get-AreaPathMap -BaseUrl $baseUrl -ApiVersion $apiVersion -Headers $headers
Write-Host "Loaded $($areaPathMap.Count) canonical Area Paths." -ForegroundColor DarkGray

$colFieldRef = if ($boardConfig.fields.columnField.referenceName) { $boardConfig.fields.columnField.referenceName } else { "System.BoardColumn" }
$doneFieldRef = if ($boardConfig.fields.doneField.referenceName) { $boardConfig.fields.doneField.referenceName } else { "System.BoardColumnDone" }
Write-Host "Resolved Board Fields: Column = $colFieldRef" -ForegroundColor DarkGray

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

# --- REORDERED: ID, Link, Title -> Workflow Steps -> Metadata
$finalHeaders = @("ID", "Link", "Title") + $boardColumns + @("Work Item Type", "Tags", "Changed Date") + $extraHeaders + @("State", "Area Path", "Blocked", "Blocked Days")
if ($ChildCount) { $finalHeaders += "ChildCount" }

# --- 3. Incremental Cache Load ---
$cache = $null
if ($IncrementalUpdate) {
    $cache = Get-ExistingCache -Path $Output -TargetHeaders $finalHeaders -Format $Format
    if ($cache) { Write-Host "Cache loaded: $($cache.Count) items found." -ForegroundColor Cyan }
}

# --- 4. Fetch Work Items (Lightweight) ---
Write-Host "Fetching work item list..."
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

$wiql = "SELECT [System.Id], [System.ChangedDate] FROM WorkItems WHERE [System.TeamProject] = '$Project' $typeWhere $areaWhere ORDER BY [System.ChangedDate] DESC"
$queryResponse = Invoke-RestMethod -Uri "$baseUrl/_apis/wit/wiql?api-version=$apiVersion" -Method Post -Headers $headers -Body (@{ query = $wiql } | ConvertTo-Json) -ContentType "application/json"
$rawWorkItems = $queryResponse.workItems | Select-Object -First $HistoryLimit

# --- 5. Determine Delta ---
$itemsToProcess = [System.Collections.Generic.List[Object]]::new()
$cachedRowsToKeep = [System.Collections.Generic.List[Object]]::new()

if ($cache) {
    Write-Host "Calculating delta..." -ForegroundColor Cyan
    $newCount = 0; $changeCount = 0; $skipCount = 0
    
    $allIds = $rawWorkItems.id
    $start = 0
    while ($allIds -and $start -lt $allIds.Count) {
        $count = [Math]::Min(200, $allIds.Count - $start)
        $batchIds = $allIds[$start..($start + $count - 1)]
        $start += $count
        
        $batchUrl = "$baseUrl/_apis/wit/workitems?ids=$($batchIds -join ',')&fields=System.Id,System.ChangedDate,System.AreaId,System.AreaPath&api-version=$apiVersion"
        $batchResponse = Invoke-AdoRest -Url $batchUrl -Headers $headers
        
        foreach ($wi in $batchResponse.value) {
            $id = [string]$wi.id
            $serverDateStr = $wi.fields."System.ChangedDate"
            
            if ($cache.ContainsKey($id)) {
                $cachedDateStr = $cache[$id].ChangedDate
                $isMatch = $false
                if ($serverDateStr -and $cachedDateStr) {
                    try {
                        $dtServer = [DateTime]$serverDateStr; $dtCache = [DateTime]$cachedDateStr
                        if ($dtServer.ToString("yyyy-MM-dd") -eq $dtCache.ToString("yyyy-MM-dd")) { $isMatch = $true }
                    } catch {
                        if ($serverDateStr -eq $cachedDateStr) { $isMatch = $true }
                    }
                }

                if ($isMatch) {
                    $cachedRow = $cache[$id].Data
                    $canonicalAreaPath = Resolve-AreaPath -Fields $wi.fields -AreaPathMap $areaPathMap -WorkItemId $wi.id
                    Set-AreaPathMetadata -RowMap $cachedRow -AreaPath $canonicalAreaPath -CalcFlags $calcFlags
                    $cachedRowsToKeep.Add($cachedRow)
                    $skipCount++
                } else {
                    $itemsToProcess.Add($wi)
                    $changeCount++
                }
            } else {
                $itemsToProcess.Add($wi)
                $newCount++
            }
        }
    }
    Write-Host "Delta: $newCount New, $changeCount Changed, $skipCount Skipped." -ForegroundColor Yellow
} else {
    foreach($item in $rawWorkItems) { $itemsToProcess.Add($item) }
}

# --- 6. Process Loop ---
$psVersion = $PSVersionTable.PSVersion.Major
$processedResults = [System.Collections.Generic.List[Object]]::new()

if ($itemsToProcess.Count -gt 0) {
    if ($psVersion -ge 7) {
        Write-Host "Processing $($itemsToProcess.Count) items in Parallel..." -ForegroundColor Yellow
        $funcInvokeRest = ${function:Invoke-AdoRest}.ToString()
        $funcGetRow = ${function:Get-FlowMetricsRow}.ToString()
        $funcResolveAreaPath = ${function:Resolve-AreaPath}.ToString()
        $funcSetRowValue = ${function:Set-RowValue}.ToString()
        $funcSetAreaPathMetadata = ${function:Set-AreaPathMetadata}.ToString()
        $itemsArray = $itemsToProcess.ToArray()

        $pResults = $itemsArray | ForEach-Object -Parallel {
            ${function:Invoke-AdoRest} = $using:funcInvokeRest
            ${function:Get-FlowMetricsRow} = $using:funcGetRow
            ${function:Resolve-AreaPath} = $using:funcResolveAreaPath
            ${function:Set-RowValue} = $using:funcSetRowValue
            ${function:Set-AreaPathMetadata} = $using:funcSetAreaPathMetadata
            
            $row = Get-FlowMetricsRow `
                -Id $_.id `
                -BaseUrl $using:baseUrl `
                -ApiVersion $using:apiVersion `
                -BoardColumns $using:boardColumns `
                -SplitMap $using:splitMap `
                -FieldRefMap $using:fieldRefMap `
                -CalcFlags $using:calcFlags `
                -FixDecreasingDates $using:FixDecreasingDates `
                -ColFieldRef $using:colFieldRef `
                -DoneFieldRef $using:doneFieldRef `
                -AreaPathMap $using:areaPathMap `
                -IncludeChildCount $using:ChildCount `
                -Headers $using:headers
            return [PSCustomObject]$row
        } -ThrottleLimit $ThrottleLimit

        foreach($r in $pResults) { $processedResults.Add($r) }
    } 
    else {
        Write-Host "Processing $($itemsToProcess.Count) items Sequentially..." -ForegroundColor Yellow
        $current = 0; $total = $itemsToProcess.Count
        foreach ($item in $itemsToProcess) {
            $current++
            $row = Get-FlowMetricsRow `
                -Id $item.id `
                -BaseUrl $baseUrl `
                -ApiVersion $apiVersion `
                -BoardColumns $boardColumns `
                -SplitMap $splitMap `
                -FieldRefMap $fieldRefMap `
                -CalcFlags $calcFlags `
                -FixDecreasingDates $FixDecreasingDates `
                -ColFieldRef $colFieldRef `
                -DoneFieldRef $doneFieldRef `
                -AreaPathMap $areaPathMap `
                -IncludeChildCount $ChildCount `
                -Headers $headers

            $processedResults.Add([PSCustomObject]$row)
            Write-Progress -Activity "Processing Work Items" -Status "ID: $($item.id) ($current/$total)" -PercentComplete (($current / $total) * 100)
        }
    }
}

# --- 7. Merge and Export ---
Write-Host "Merging & Exporting to $Format format..." -ForegroundColor Cyan

$allData = [System.Collections.Generic.List[PSCustomObject]]::new()
foreach($c in $cachedRowsToKeep) { $allData.Add($c) }
foreach($p in $processedResults) { $allData.Add($p) }

if ($Format -eq 'json') {
    $jsonRows = [System.Collections.Generic.List[String]]::new()
    function Format-JsonStr { 
        param($s) 
        if ($s -is [DateTime]) { return '"' + $s.ToString("yyyy-MM-dd") + '"' }
        return '"' + $s.ToString().Replace('\', '\\').Replace('"', '\"') + '"' 
    }

    $headerStr = "[" + (($finalHeaders | ForEach-Object { Format-JsonStr $_ }) -join ",") + "]"
    $jsonRows.Add($headerStr)

    foreach ($item in $allData) {
        $rowValues = @()
        foreach ($h in $finalHeaders) {
            $val = $item.$h
            if ($null -eq $val) { $val = "" }
            $rowValues += Format-JsonStr $val
        }
        $jsonRows.Add("[" + ($rowValues -join ",") + "]")
    }

    $finalJson = "[" + [Environment]::NewLine + ($jsonRows -join "," + [Environment]::NewLine) + [Environment]::NewLine + "]"
    $finalJson | Set-Content -Path $Output -Encoding UTF8

} elseif ($Format -eq 'csv' -or $Format -eq 'excel') {
    $excelSuccess = $false

    if ($Format -eq 'excel') {
        $absOutput = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Output) -replace '/', '\'
        $outDir = Split-Path $absOutput
        if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Force -Path $outDir | Out-Null }
        if (Test-Path $absOutput) { Remove-Item $absOutput -Force }

        $tempCsv = Join-Path $outDir ("~temp_" + [System.Guid]::NewGuid().ToString().Substring(0,8) + ".csv")
        $allData | Select-Object $finalHeaders | Export-Csv -Path $tempCsv -NoTypeInformation -Encoding UTF8 -UseCulture

        try {
            Write-Host "Invoking Excel COM Object for native .xlsx conversion..." -ForegroundColor Gray
            $excel = New-Object -ComObject Excel.Application
            $excel.DisplayAlerts = $false
            $wb = $excel.Workbooks.Open($tempCsv)
            
            $wb.Worksheets.Item(1).Rows.Item(1).Font.Bold = $true
            
            $wb.SaveAs($absOutput, 51) # 51 = xlOpenXMLWorkbook (.xlsx)
            
            if (Test-Path $absOutput) {
                $excelSuccess = $true
            } else {
                Write-Warning "Excel reported success but the file was not found at: $absOutput"
            }
            
        } catch {
            Write-Warning "Failed to generate native Excel file: $($_.Exception.Message)"
        } finally {
            if ($null -ne $wb) { try { $wb.Close($false) } catch {} }
            if ($null -ne $excel) { 
                try { $excel.Quit() } catch {}
                [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
            }
            if (Test-Path $tempCsv) { Remove-Item $tempCsv -Force -ErrorAction SilentlyContinue }
        }
    }

    if ($Format -eq 'csv' -or -not $excelSuccess) {
        if (-not $excelSuccess -and $Format -eq 'excel') {
            Write-Warning "Falling back to standard CSV format."
            $Output = $Output -replace '\.xlsx$', '.csv'
        }
        $allData | Select-Object $finalHeaders | Export-Csv -Path $Output -NoTypeInformation -Encoding UTF8
    }
}

Write-Host "Export complete! File saved to: $Output" -ForegroundColor Green
