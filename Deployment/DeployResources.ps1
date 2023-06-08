Param(
    [string] $subscriptionId,
    [string] $newResourceGroupPrefix,
    [string] $deploymentId = (Get-Date -UFormat "%y%m%dt%H%M%S")
)

Set-AzContext -Subscription $subscriptionId

$resourceGroupName = "{0}-{1}" -f $newResourceGroupPrefix, $deploymentId
New-AzResourceGroup -Name $resourceGroupName -Location 'eastus'

$deploymentParams = @{
    "deploymentId" = $deploymentId
}

New-AzResourceGroupDeployment `
    -ResourceGroupName $resourceGroupName `
    -TemplateFile "./main.bicep" `
    -TemplateParameterObject $deploymentParams

# TODO: Waiting 10 seconds because the function apps need to be ready before we can push the code.
# Need to look into a better way to do this
Start-Sleep -Seconds 10

# Publish campaign list function app
$campaignListFunctionAppName = "campaign-list-function-app-{0}" -f $deploymentId
cd ../CampaignList
func azure functionapp publish $campaignListFunctionAppName --csharp

# Publish campaign mailer function app
$campaignMailerFunctionAppName = "campaign-mailer-function-app-{0}" -f $deploymentId
cd ../CampaignMailer
func azure functionapp publish $campaignMailerFunctionAppName --csharp

# Publish campaign telemetry function app
$campaignTelemetryFunctionAppName = "campaign-telemetry-function-app-{0}" -f $deploymentId
cd ../EmailTelemetry
func azure functionapp publish $campaignTelemetryFunctionAppName --csharp