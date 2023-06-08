param deploymentId string
param location string = resourceGroup().location

// Deploy CosmosDB
resource campaignCosmosDbAccount 'Microsoft.DocumentDB/databaseAccounts@2023-04-15' = {
  name: 'campaign-list-cosmos-account-${deploymentId}'
  location: location
  properties: {
    databaseAccountOfferType: 'Standard'
    locations: [
      {
        locationName: 'West US'
        failoverPriority: 0
      }
    ]
  }
}

resource campaignCosmosDbDatabase 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2023-04-15' = {
  parent: campaignCosmosDbAccount
  name: 'Campaign'
  properties: {
    resource: {
      id: 'Campaign'
    }
  }
}

resource campaignCosmosDbContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2023-04-15' = {
  parent: campaignCosmosDbDatabase
  name: 'EmailList'
  properties: {
    resource: {
      id: 'EmailList'
      partitionKey: {
        paths: [
          '/campaignId'
        ]
        kind: 'Hash'
        version: 2
      }
      
    }
    options: {
      autoscaleSettings: {
        maxThroughput: 80000
      }
    }
  }
}


// Deploy Storage Account
resource campaignStorageAccount 'Microsoft.Storage/storageAccounts@2021-04-01' = {
  name: 'campaignsa${deploymentId}'
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    accessTier: 'Hot'
  }
}

resource campaignBlobStorageService 'Microsoft.Storage/storageAccounts/blobServices@2022-09-01' = {
  parent: campaignStorageAccount
  name: 'default'
}

resource campainBlobStorageContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2022-09-01' = {
  parent: campaignBlobStorageService
  name: 'campaign'
}


// Deploy Campaign List Function App
module campaignList './campaignlist.bicep' = {
  name: 'campaignList'
  params: {
    campaignCosmosDbAccountConnectionString: campaignCosmosDbAccount.listConnectionStrings().connectionStrings[0].connectionString
    campaignStorageAccountAccessKey: campaignStorageAccount.listKeys().keys[0].value
    campaignStorageAccountName: campaignStorageAccount.name
    dataverseConnectionString: '<insert-dataverse-connection-string-here>'
    deploymentId: deploymentId
    location: location
  }
}

// Deploy Campaign Mailer Function App
module campaignMailer 'campaignmailer.bicep' = {
  name: 'campaignMailer'
  params: {
    campaignCosmosDbAccountConnectionString: campaignCosmosDbAccount.listConnectionStrings().connectionStrings[0].connectionString
    campaignStorageAccountAccessKey: campaignStorageAccount.listKeys().keys[0].value
    campaignStorageAccountName: campaignStorageAccount.name
    communicationServiceConnectionString: campaignCommunicationService.listKeys().primaryConnectionString
    deploymentId: deploymentId
    location: location
  }
}

// Deploy Communication Service
resource campaignCommunicationService 'Microsoft.Communication/communicationServices@2023-03-31' = {
  name: 'campaign-comm-service-${deploymentId}'
  location: 'global'
  properties: {
    dataLocation: 'UnitedStates'
    linkedDomains: [
      azureManagedDomain.id
    ]
  }
}

resource campaignEmailService 'Microsoft.Communication/emailServices@2023-03-31' = {
  name: 'campaign-email-service-${deploymentId}'
  location: 'global'
  properties: {
    dataLocation: 'UnitedStates'
  }
}

resource azureManagedDomain 'Microsoft.Communication/emailServices/domains@2023-03-31' = {
  parent: campaignEmailService
  name: 'AzureManagedDomain'
  location: 'global'
  properties: {
    domainManagement: 'AzureManaged'
  }
}

// Deploy Event Grid
resource campaignTelemetryEventHubNamespace 'Microsoft.EventHub/namespaces@2022-10-01-preview' = {
  name: 'campaignTelemetryEventHubNamespace${deploymentId}'
  location: location
}

resource campaignTelemetryEventHub 'Microsoft.EventHub/namespaces/eventhubs@2022-10-01-preview' = {
  parent: campaignTelemetryEventHubNamespace
  name: 'campaign-telemetry-eventhub'
}

resource campaignTelemetrySystemTopic 'Microsoft.EventGrid/systemTopics@2022-06-15' = {
  name: 'campaign-telemetry-system-topic-${deploymentId}'
  location: 'global'
  properties: {
    source: campaignCommunicationService.id
    topicType: 'Microsoft.Communication.CommunicationServices'
  }
}

resource campaignTelemetryEventSubscriptions 'Microsoft.EventGrid/systemTopics/eventSubscriptions@2022-06-15' = {
  parent: campaignTelemetrySystemTopic
  name: '${campaignTelemetrySystemTopic.name}-subscription'
  properties: {
    destination: {
      properties: {
        resourceId: campaignTelemetryEventHub.id
      }
      endpointType: 'EventHub'
    }
    filter: {
      includedEventTypes: [
        'Microsoft.Communication.EmailEngagementTrackingReportReceived'
        'Microsoft.Communication.EmailDeliveryReportReceived'
      ]
    }
  }
}

// Deploy Campaign Telemetry Function App
module campaignTelemetry 'campaigntelemetry.bicep' = {
  name: 'campaignTelemetry'
  params: {
    campaignCosmosDbAccountConnectionString: campaignCosmosDbAccount.listConnectionStrings().connectionStrings[0].connectionString
    campaignStorageAccountAccessKey: campaignStorageAccount.listKeys().keys[0].value
    campaignStorageAccountName: campaignStorageAccount.name
    campaignTelemetryEventHubName: campaignTelemetryEventHub.name
    deploymentId: deploymentId
    location: location
  }
}
