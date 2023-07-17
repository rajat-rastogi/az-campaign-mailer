## Prerequisites

The following tools need to be installed:
- Bicep: https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/install#windows
- Azure PowerShell: https://learn.microsoft.com/en-us/powershell/azure/install-azps-windows?view=azps-9.7.1&tabs=powershell&pivots=windows-msi
- Azure CLI: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli

Make sure you are signed into Azure Powershell and the Azure CLI with the subscription your are hoping to deploy to before deploying the resources.

## Deploying the resources

The "resource group prefix" in the following command will be appended with a deployment ID to create the resource group name. 

By default, the deployment ID is a timestamp. If you want to provide your own deployment ID, you can pass in the optional `-deploymentId` parameter.

You will also need to provide the dataverse connection string that will be used to read the email content and contacts.

```bash
./DeployResources.ps1 -subscriptionId "<your-subscription-id>" -resourceGroupPrefix "<your-resource-group-prefix>" -deploymentId "<some-unique-id>" -dataverseConnectionString "<your-dataverse-connection-string>
```