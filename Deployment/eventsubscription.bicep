param communicationServiceId string
param webAppName string

// Deploy Event Grid
resource eventSystemTopic 'Microsoft.EventGrid/systemTopics@2022-06-15' = {
  name: 'sbaeventtopic'
  location: 'global'
  properties: {
    source: communicationServiceId
    topicType: 'Microsoft.Communication.CommunicationServices'
  }
}

resource eventSubscription 'Microsoft.EventGrid/systemTopics/eventSubscriptions@2022-06-15' = {
  parent: eventSystemTopic
  name: 'sbaemaildelivery'
  properties: {
    destination:{
      properties: {
        maxEventsPerBatch: 100
        preferredBatchSizeInKilobytes: 64
        endpointUrl: 'https://${webAppName}.azurewebsites.net/campaign/deliveryEvents'
      }
      endpointType: 'WebHook'
    }
    filter: {
      includedEventTypes: [
        'Microsoft.Communication.EmailDeliveryReportReceived'
      ]
      enableAdvancedFilteringOnArrays: true
    }
    labels: []
    eventDeliverySchema: 'EventGridSchema'
    retryPolicy: {
      maxDeliveryAttempts: 30
      eventTimeToLiveInMinutes: 1440
    }
  }
}
