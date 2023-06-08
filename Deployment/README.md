## Prerequisites

The following tools need to be installed:
- Bicep: https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/install#windows
- Azure PowerShell: https://learn.microsoft.com/en-us/powershell/azure/install-azps-windows?view=azps-9.7.1&tabs=powershell&pivots=windows-msi
- Azure Functions Core Tools: https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=v4%2Cwindows%2Ccsharp%2Cportal%2Cbash

Make sure you are signed into Azure Powershell and the Azure CLI before deploying the resources.

## Deploying the resources

The "resource group prefix" in the following command will be appended with a deployment ID to create the resource group name. By default, the deployment ID is a timestamp. If you want to provide your own deployment ID, you can pass in the optional `-deploymentId` parameter.

```bash
./DeployResources.ps1 -subscriptionId "<your-subscription-id>" -resourceGroupPrefix "<your-resource-group-prefix>"
```