Param(
    [Parameter(Mandatory=$true)]
    [string] $subscriptionId,
    [Parameter(Mandatory=$true)]
    [string] $newResourceGroupPrefix,
    [string] $deploymentId = (Get-Date -UFormat "%y%m%dt%H%M%S"),
    [Parameter(Mandatory=$true)]
    [string] $dataverseConnectionString
)

Set-AzContext -Subscription $subscriptionId

Write-Host "Starting Resource Group Deployment."
$resourceGroupName = "{0}-{1}" -f $newResourceGroupPrefix, $deploymentId
New-AzResourceGroup -Name $resourceGroupName -Location 'eastus'

Write-Host "Starting Resource Deployment."
$deploymentParams = @{
    "deploymentId" = $deploymentId
    "dataverseConnectionstring" = $dataverseConnectionString
}

$deployment = New-AzResourceGroupDeployment `
    -ResourceGroupName $resourceGroupName `
    -TemplateFile "./main.bicep" `
    -TemplateParameterObject $deploymentParams

Write-Host "Waiting 5 seconds."
Start-Sleep -Seconds 5

Write-Host "Starting Web App Deployment."
cd ../CampaignMailer
dotnet publish --output published

if (Test-Path "webapp.zip") {
    compress-archive published\* webapp.zip -Update
} else {
    compress-archive published\* webapp.zip
}

$webAppName = "web-app-{0}" -f $deploymentId
az webapp deploy --async false --clean true --subscription $subscriptionId --resource-group $resourceGroupName --name $webAppName --restart true --src-path ./webapp.zip 

cd ../Deployment

Write-Host "Waiting 5 seconds."
Start-Sleep -Seconds 5

Write-Host "Starting Event Subscription Deployment."
$eventDeploymentParams = @{
    "communicationServiceId" = $deployment.Outputs["communicationServiceId"].Value
    "webAppName" = $deployment.Outputs["webAppName"].Value
}

New-AzResourceGroupDeployment `
    -ResourceGroupName $resourceGroupName `
    -TemplateFile "./eventsubscription.bicep" `
    -TemplateParameterObject $eventDeploymentParams

Write-Host "Deployment Complete."