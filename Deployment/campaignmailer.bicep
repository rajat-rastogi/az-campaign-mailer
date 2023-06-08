param deploymentId string
param location string
param campaignCosmosDbAccountConnectionString string
param campaignStorageAccountName string
param campaignStorageAccountAccessKey string
param communicationServiceConnectionString string

resource campaignMailerAppServicePlan 'Microsoft.Web/serverfarms@2022-09-01' = {
  name: 'campaign-mailer-app-service-plan-${deploymentId}'
  location: location
  kind: 'functionapp'
  sku: {
    name: 'EP1'
    tier: 'ElasticPremium'
    size: 'EP1'
    family: 'EP'
  }
}

resource campaignMailerFunctionApp 'Microsoft.Web/sites@2022-09-01' = {
  name: 'campaign-mailer-function-app-${deploymentId}'
  location: location
  kind: 'functionapp'
  properties: {
    siteConfig: {
      appSettings: [
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'FUNCTIONS_WORKER_RUNTIME'
          value: 'dotnet'
        }
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${campaignStorageAccountName};AccountKey=${campaignStorageAccountAccessKey};EndpointSuffix=core.windows.net'
        }
        {
          name: 'COSMOSDB_CONNECTION_STRING'
          value: campaignCosmosDbAccountConnectionString
        }
        {
          name: 'COMMUNICATION_SERVICES_CONNECTION_STRING'
          value: communicationServiceConnectionString
        }
      ]
    }
    serverFarmId: campaignMailerAppServicePlan.id
  }
}
