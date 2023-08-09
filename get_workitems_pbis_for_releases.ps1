param(
    [string] $buildPipelineName
)

$ErrorActionPreference = "Stop"

# Replace 'YourOrganization', 'YourProject', 'YourBuildDefinitionId', 'YourBuildNumber', and 'YourPAT' with your actual values.
$organization = "org_name"
$project = "proj_name"
$buildPipelineName = "release_pipeline_name"
$pat = "paste your PAT t"

# Base64 encode the PAT to use in the Authorization header.
$base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(":$pat"))

# Get the list of build definitions for the specified project and pipeline name
Write-Host "Fetching Build Definition ID for pipeline '$buildPipelineName'..."

# Get the list of build definitions for the specified project and pipeline name
$buildDefinitionsUrl = "https://dev.azure.com/$organization/$project/_apis/build/definitions?name=$buildPipelineName&api-version=6.0"
$buildDefinitionsResponse = Invoke-RestMethod -Uri $buildDefinitionsUrl -Headers @{Authorization=("Basic {0}" -f $base64AuthInfo)} -Method Get
$buildDefinitions = $buildDefinitionsResponse.value | Where-Object { $_.name -eq $buildPipelineName } | Select-Object -ExpandProperty id

# Output the Build Definition ID
Write-Host "Build Definition ID: $buildDefinitions"


# get build id from build definition
$latestBuildUrl = "https://dev.azure.com/$organization/$project/_apis/build/builds?definitions=$buildDefinitions&\$top=1&\$orderby=startTime desc&api-version=6.0"
$latestBuildResponse = Invoke-RestMethod -Uri $latestBuildUrl -Headers @{Authorization=("Basic {0}" -f $base64AuthInfo)} -Method Get
$latestBuild = $latestBuildResponse.value[0]
$buildId = $latestBuild.id

# Output the Build ID
Write-Host "Build ID: $buildId"

# Get the work items associated with the build
$workItemsUrl = "https://dev.azure.com/$organization/$project/_apis/build/builds/$buildId/workitems?api-version=6.0"
$workItemsResponse = Invoke-RestMethod -Uri $workItemsUrl -Headers @{Authorization=("Basic {0}" -f $base64AuthInfo)} -Method Get

# Output the work items
$workItemsResponse.value
