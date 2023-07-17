param deploymentId string
param dataverseConnectionString string
param location string = resourceGroup().location

// Deploy Service Bus
resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' = {
  name: 'service-bus-namespace-${deploymentId}'
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
  }
}

resource serviceBusContactListQueue 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: serviceBusNamespace
  name: 'contactlist'
  properties: {
    status: 'Active'
    maxDeliveryCount: 10
    maxSizeInMegabytes: 5120
    maxMessageSizeInKilobytes: 256
    requiresDuplicateDetection: true
    requiresSession: false
    defaultMessageTimeToLive: 'P14D'
    deadLetteringOnMessageExpiration: false
    enableBatchedOperations: true
    autoDeleteOnIdle: 'P10675199DT2H48M5.4775807S'
    duplicateDetectionHistoryTimeWindow: 'P1DT10M'
    lockDuration: 'PT5M'
    enablePartitioning: false
    enableExpress: false
  }
}

// Deploy Email Resources
resource communicationService 'Microsoft.Communication/communicationServices@2023-03-31' = {
  name: 'comm-service-${deploymentId}'
  location: 'global'
  properties: {
    dataLocation: 'UnitedStates'
    linkedDomains: [
      azureManagedDomain.id
    ]
  }
}

resource emailService 'Microsoft.Communication/emailServices@2023-03-31' = {
  name: 'email-service-${deploymentId}'
  location: 'global'
  properties: {
    dataLocation: 'UnitedStates'
  }
}

resource azureManagedDomain 'Microsoft.Communication/emailServices/domains@2023-03-31' = {
  parent: emailService
  name: 'AzureManagedDomain'
  location: 'global'
  properties: {
    domainManagement: 'AzureManaged'
  }
}

// Deploy Storage Account
resource storageAccount 'Microsoft.Storage/storageAccounts@2022-09-01' = {
  name: 'sa${deploymentId}'
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
}

// Deploy Web App
resource appServicePlan 'Microsoft.Web/serverfarms@2022-09-01' = {
  name: 'app-service-plan-${deploymentId}'
  location: location
  kind: 'app'
  sku: {
    name: 'P1v3'
    tier: 'PremiumV3'
    size: 'P1v3'
    family: 'Pv3'
    capacity: 1
  }
}

var serviceBusListKeysEndpoint = '${serviceBusNamespace.id}/AuthorizationRules/RootManageSharedAccessKey'
var storageAccountName = storageAccount.name
var storageAccountAccessKey = storageAccount.listKeys().keys[0].value

resource webApp 'Microsoft.Web/sites@2022-09-01' = {
  name: 'web-app-${deploymentId}'
  location: location
  kind: 'app'
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      appSettings: [
        {
          name: 'WEBSITE_RUN_FROM_PACKAGE'
          value: '1'
        }
      ]
      connectionStrings: [
        {
          name: 'COMMUNICATIONSERVICES_CONNECTION_STRING'
          connectionString: communicationService.listKeys().primaryConnectionString
        }
        {
          name: 'DATAVERSE_CONNECTION_STRING'
          connectionString: dataverseConnectionString
        }
        {
          name: 'SERVICEBUS_CONNECTION_STRING'
          connectionString: listKeys(serviceBusListKeysEndpoint, serviceBusNamespace.apiVersion).primaryConnectionString
        }
        {
          name: 'STORAGE_CONNECTION_STRING'
          connectionString: 'DefaultEndpointsProtocol=https;AccountName=${storageAccountName};AccountKey=${storageAccountAccessKey};EndpointSuffix=core.windows.net'
        }
      ]
    }
  }
}

// Outputs
output communicationServiceId string = communicationService.id
output webAppName string = webApp.name
