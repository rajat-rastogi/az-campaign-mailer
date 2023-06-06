using Azure;
using Azure.Data.Tables;
using Azure.Messaging.EventGrid;
using Azure.Messaging.EventGrid.SystemEvents;
using Azure.Messaging.ServiceBus;
using CampaignMailer.Models;
using CampaignMailer.Utilities;
using Microsoft.AspNetCore.Mvc;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using EmailAddress = CampaignMailer.Models.EmailAddress;
using EmailRecipients = CampaignMailer.Models.EmailRecipients;

namespace CampaignMailer.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class CampaignController : ControllerBase
    {
        private const int concurrentReceiveMessageThreads = 10;
        private const int messagesProcessedPerThread = 100;
        private const string serviceBusQueueName = "contactlist";
        private const string storageTableName = "OperationStatus";
        private readonly ILogger<CampaignController> log;
        private readonly ServiceClient dataverseClient;
        private readonly HashSet<string> blockList;
        private readonly ServiceBusClient serviceBusClient;
        private readonly ServiceBusSender serviceBusSender;
        private readonly ServiceBusReceiver serviceBusReceiver;
        private readonly TableClient tableClient;
        private readonly Mailer mailer;
        private readonly ConcurrentDictionary<string, Campaign> campaigns;

        public CampaignController(IConfiguration configuration, ILogger<CampaignController> logger)
        {
            var dataverseConnectionString = GetDataverseConnectionString(configuration);
            var serviceBusConnectionString = configuration.GetConnectionString("SERVICEBUS_CONNECTION_STRING");
            var communicationServicesConnectionString = configuration.GetConnectionString("COMMUNICATIONSERVICES_CONNECTION_STRING");
            var storageConnectionString = configuration.GetConnectionString("STORAGE_CONNECTION_STRING");

            log = logger;
            blockList = GetBlockList();
            dataverseClient = new(dataverseConnectionString);

            var serviceBusClientOptions = new ServiceBusClientOptions()
            {
                TransportType = ServiceBusTransportType.AmqpWebSockets
            };
            var serviceBusReceiverOptions = new ServiceBusReceiverOptions()
            {
                PrefetchCount = 5000
            };
            serviceBusClient = new ServiceBusClient(serviceBusConnectionString, serviceBusClientOptions);
            serviceBusSender = serviceBusClient.CreateSender(serviceBusQueueName);
            serviceBusReceiver = serviceBusClient.CreateReceiver(serviceBusQueueName, serviceBusReceiverOptions);

            var tableServiceClient = new TableServiceClient(storageConnectionString);
            tableClient = tableServiceClient.GetTableClient(storageTableName);
            tableClient.CreateIfNotExists();

            mailer = new Mailer(communicationServicesConnectionString);
            campaigns = new ConcurrentDictionary<string, Campaign>();
        }

        [HttpPost("start", Name = "StartCampaign")]
        public IActionResult StartCampaign(
            [FromBody][Required] StartCampaignDto campaignDto)
        {
            try
            {
                campaigns.TryAdd(campaignDto.CampaignId, new Campaign(campaignDto));
            }
            catch (ArgumentNullException anex)
            {
                return BadRequest(anex.Message);
            }

            // Process the campaign list
            if (!string.IsNullOrWhiteSpace(campaignDto.ListName))
            {
                if (dataverseClient.IsReady)
                {
                    _ = Task.Run(async () =>
                    {
                        if (campaignDto.SkipFetchContacts)
                        {
                            log.LogInformation($"************ Skipping download and queueing of contacts *************");
                        }
                        else
                        {
                            await FetchAndQueueContacts(campaignDto);
                            log.LogInformation($"************ Finished downloading contacts and queueing contacts *************");
                            log.LogInformation($"Waiting 10 secs...");
                            await Task.Delay(TimeSpan.FromSeconds(10));
                        }

                        do
                        {
                            log.LogInformation($"************ Starting to process messages *************");
                            await StartProcessingMessages(campaignDto.CampaignId);
                        } while (await ExecuteWithRetriesAsync(IsActiveMessageOnQueueAsync, new int[] { 60, 180, 500 }));

                        log.LogInformation($"************ EVERYTHING FINISHED *************");
                    });
                    return Accepted(new ErrorDetail { Message = $"Campaign with Id={campaignDto.CampaignId} started..." });
                }
                else
                {
                    var errorMessage = $"A web service connection was not established. Campaign list {campaignDto.ListName} was not processed";
                    log.LogError(errorMessage);
                    return StatusCode(StatusCodes.Status500InternalServerError, errorMessage);
                }
            }
            else
            {
                const string listEmptyErrorMessage = "Campaign list name was empty. Please provide the name of a campaign list to process.";
                log.LogError(listEmptyErrorMessage);
                return BadRequest(listEmptyErrorMessage);
            }
        }

        [HttpDelete("deliveryResults", Name = "DeleteDeliveryResults")]
        public IActionResult DeleteDeliveryResults(
            [FromBody][Required] DeleteDeliveryResultsDto deleteDeliveryResultsDto)
        {
            _ = Task.Run(async () => await DeleteRowsByCampiagnIdAsync(deleteDeliveryResultsDto.CampaignId));
            return Accepted(new ErrorDetail { Message = $"Delivery results for campaign with Id={deleteDeliveryResultsDto.CampaignId} are being deleted..." });
        }

        [HttpDelete("deliveryAllResults", Name = "DeleteAllDeliveryResults")]
        public async Task<IActionResult> DeleteAllDeliveryResults()
        {
            var response = await tableClient.DeleteAsync();
            if (response.IsError)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, Encoding.UTF8.GetString(response.Content));
            }
            else
            {
                if (await ExecuteWithRetriesAsync(TryCreateTableIfNotExists, new int[] { 15, 30, 60 }))
                {
                    return Ok();
                }
                else
                {
                    return StatusCode(StatusCodes.Status500InternalServerError, $"Table deleted but could not be created back again. Please create the {storageTableName} storage table manually.");
                }
            }
        }

        [HttpGet("deliveryStatistics", Name = "GetDeliveryStatistics")]
        public async Task<IActionResult> GetDeliveryStatisticsAsync(
            [FromQuery][Required] string campaignId)
        {
            var deliveryStatistics = await GetDeliveryStatisticsByCampaignIdAsync(campaignId);
            return Ok(deliveryStatistics);
        }

        [HttpPost("deliveryEvents", Name = "ProcessDeliveryEvents")]
        public async Task<IActionResult> ProcessDeliveryEventsAsync(
            [FromBody] object request,
            CancellationToken cancellationToken)
        {
            // Deserializing the request
            EventGridEvent[] eventGridEvents = System.Text.Json.JsonSerializer.Deserialize<EventGridEvent[]>(request.ToString());

            log.LogInformation($"[EventProcessor] Received {eventGridEvents.Length} events.");

            var tasks = new List<Task>();
            try
            {
                foreach (var eventGridEvent in eventGridEvents)
                {
                    // Handle system events
                    if (eventGridEvent.TryGetSystemEventData(out object systemEventData))
                    {
                        if (systemEventData is SubscriptionValidationEventData subscriptionValidationEventData)
                        {
                            var responseData = new SubscriptionValidationResponse()
                            {
                                ValidationResponse = subscriptionValidationEventData.ValidationCode,
                            };
                            return new OkObjectResult(responseData);
                        }
                        else
                        {
                            tasks.Add(ProcessEventAsync(eventGridEvent, cancellationToken));
                        }
                    }
                    else
                    {
                        // Handle custom events
                        tasks.Add(ProcessEventAsync(eventGridEvent, cancellationToken));
                    }
                }

                await Task.WhenAll(tasks);

                log.LogInformation($"[EventProcessor] Successfully processed {eventGridEvents.Length} events.");
                return Ok();
            }
            catch (Exception ex)
            {
                log.LogError($"[EventProcessor] {ex}");
                return StatusCode(StatusCodes.Status500InternalServerError, ex);
            }
        }

        private async Task ProcessEventAsync(EventGridEvent eventGridEvent, CancellationToken cancellationToken = default)
        {
            if (eventGridEvent != null && eventGridEvent.Data != null)
            {
                var emailEventData = JsonConvert.DeserializeObject<EmailEventData>(Encoding.UTF8.GetString(eventGridEvent.Data));

                var entity = new OperationStatusEntity
                {
                    PartitionKey = emailEventData.Recipient,
                    RowKey = emailEventData.MessageId,
                    Status = emailEventData.Status
                };

                await tableClient.UpsertEntityAsync(entity, cancellationToken: cancellationToken);
            }
        }

        private async Task FetchAndQueueContacts(StartCampaignDto campaignConfig)
        {
            // Define the query attributes for pagination of the results.
            // Set the number of records per page to retrieve.
            int pageSize = campaignConfig.PageSize;

            // Initialize the page number.
            int pageNumber = 1;

            // Specify the current paging cookie. For retrieving the first page, 
            // pagingCookie should be null.
            string pagingCookie = null;


            // Determine if the list is dynamic or static
            bool isDynamic = IsDynamic(campaignConfig.ListName);

            // Get the query XML specific to dynamic and static lists
            string queryXml;
            if (isDynamic)
            {
                // Get the XML query
                queryXml = GetDynamicQuery(campaignConfig.ListName);
            }
            else
            {
                // Retrieve the ID of the static campaign list
                var listId = GetCampaignListID(campaignConfig.ListName);
                queryXml = GetStaticQuery(listId);
            }

            int totalContactCount = 0;

            // Process each page of the list query results until every page has been processed
            bool morePages = true;
            var queueTasks = new List<Task>();
            try
            {
                while (morePages)
                {
                    // Add the pagination attributes to the XML query.
                    string currQueryXml = AddPaginationAttributes(queryXml, pagingCookie, pageNumber, pageSize);

                    // Execute the fetch query and get the results in XML format.
                    RetrieveMultipleRequest fetchRequest = new()
                    {
                        Query = new FetchExpression(currQueryXml)
                    };

                    log.LogInformation($"************** Fetching page {pageNumber} ********************");

                    EntityCollection pageCollection = ((RetrieveMultipleResponse)dataverseClient.Execute(fetchRequest)).EntityCollection;

                    // Convert EntityCollection to JSON serializable object collection.
                    if (pageCollection.Entities.Count > 0)
                    {
                        queueTasks.Add(QueueContactsAsync(pageCollection.Entities, campaignConfig, isDynamic));
                        totalContactCount += pageCollection.Entities.Count;
                    }

                    // Check for more records.
                    if (pageCollection.MoreRecords)
                    {
                        // Increment the page number to retrieve the next page.
                        pageNumber++;

                        // Set the paging cookie to the paging cookie returned from current results.                            
                        pagingCookie = pageCollection.PagingCookie;
                    }
                    else
                    {
                        morePages = false;
                    }
                }

                await Task.WhenAll(queueTasks);
            }
            catch (Exception ex)
            {
                log.LogError($"{ex}");
            }

            log.LogInformation($"Total number of contacts in the list: {totalContactCount}");
            log.LogInformation($"Successfully completed processing {campaignConfig.ListName}");
        }

        private async Task StartProcessingMessages(string campaignId)
        {
            campaigns[campaignId].ReadyToSendMail -= Campaign_ReadyToSendMail;
            campaigns[campaignId].ReadyToSendMail += Campaign_ReadyToSendMail;

            var tasks = new List<Task>();
            for (int i = 0; i < concurrentReceiveMessageThreads; i++)
            {
                int threadIndex = i;
                tasks.Add(Task.Run(async () => await FetchMessages(threadIndex)));
            }
            await Task.WhenAll(tasks);
        }

        private async Task FetchMessages(int threadIndex)
        {
            var timer = new System.Timers.Timer(TimeSpan.FromMinutes(1).TotalMilliseconds);
            timer.Elapsed += (sender, e) => { timer.Stop(); };
            timer.AutoReset = false;
            timer.Start();

            while (true)
            {
                var receivedMessages = await serviceBusReceiver.ReceiveMessagesAsync(messagesProcessedPerThread);
                if (receivedMessages == null || receivedMessages.Count == 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    if (!timer.Enabled)
                    {
                        break;
                    }
                }
                else
                {
                    timer.Stop();
                    log.LogInformation($"Thread{threadIndex} - Received {receivedMessages.Count} messages");
                    await ProcessMessages(receivedMessages);
                    timer.Start();
                }
            }

            log.LogWarning($"Thread{threadIndex} - No messages received for past 1 minute. Stopping thread.");
        }

        private async Task ProcessMessages(IReadOnlyList<ServiceBusReceivedMessage> messages)
        {
            List<Task> tasks = new();
            foreach (var message in messages)
            {
                tasks.Add(ProcessMessage(message));
            }
            await Task.WhenAll(tasks);
        }

        private async Task ProcessMessage(ServiceBusReceivedMessage message)
        {
            if (string.Equals(message.Subject, MessageType.Request.ToString()))
            {
                EmailRequestServiceBusMessageDto messageDto = null;
                try
                {
                    messageDto = JsonConvert.DeserializeObject<EmailRequestServiceBusMessageDto>(Encoding.UTF8.GetString(message.Body), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All });
                }
                catch (Exception ex)
                {
                    log.LogError($"{ex}");
                }
                await SendEmailAsync(
                    campaigns[messageDto.CampaignId],
                    messageDto.EmailRecipients,
                    messageDto.OperationId,
                    new List<ServiceBusReceivedMessage> { message },
                    MessageType.Request);

            }
            else if (string.Equals(message.Subject, MessageType.Address.ToString()))
            {
                var messageDto = JsonConvert.DeserializeObject<EmailAddressServiceBusMessageDto>(Encoding.UTF8.GetString(message.Body));
                campaigns[messageDto.CampaignId].AddMessage(message);
            }
        }

        private async Task Campaign_ReadyToSendMail(ReadyToSendMailEventArgs args)
        {
            await SendEmailAsync(args.Campaign, GetEmailRecipients(args.Messages), Guid.NewGuid().ToString(), args.Messages, MessageType.Address);
        }

        private static HashSet<string> GetBlockList()
        {
            HashSet<string> addresses = new();
            string path = "BlockList.txt";
            using (StreamReader sr = new(path))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    addresses.Add(line);
                }
            }
            return addresses;
        }

        /// <summary>
        /// Determines if the selected campaign list is dynamic or static.
        /// </summary>
        /// <param name="listName"></param>
        /// <returns>true if dynamic or false if static.</returns>
        private bool IsDynamic(string listName)
        {
            // Query a campaign name
            var query = new QueryByAttribute("list")
            {
                ColumnSet = new ColumnSet("type")
            };
            query.AddAttributeValue("listname", listName);

            var results = dataverseClient.RetrieveMultiple(query);
            return bool.Parse(results.Entities.First().Attributes["type"].ToString());
        }

        /// <summary>
        /// Create XML query that allows for paginated retrieval of campaign contacts.
        /// </summary>
        /// <param name="queryXml"></param>
        /// <param name="pageCookie"></param>
        /// <param name="pageNum"></param>
        /// <param name="recCount"></param>
        /// <returns></returns>
        private string GetDynamicQuery(string listName)
        {
            // Query a campaign name and use it to derive the Dataverse query XML
            var query = new QueryByAttribute("list")
            {
                ColumnSet = new ColumnSet("query")
            };
            query.AddAttributeValue("listname", listName);

            var results = dataverseClient.RetrieveMultiple(query);
            // Return value containing the query XML
            string queryXml = results.Entities.First().Attributes["query"].ToString();

            // Update the query XML to ensure it has the email address attribute 
            queryXml = AddEmailAttribute(queryXml);

            return queryXml;
        }

        /// <summary>
        /// Add the email attribute to the campaign list query XML if it is not in the query already. 
        /// add the attribute.
        /// </summary>
        /// <param name="queryXml">The </param>
        /// <returns></returns> 
        private static string AddEmailAttribute(string queryXml)
        {
            var xDocument = XDocument.Parse(queryXml);

            // Find the contact entity node
            var entity = xDocument.Descendants("entity").Where(e => e?.Attribute("name").Value == "contact").First();

            // Does an email address attribute exist? If it doesn't, add it
            var emailAttributeExists = entity.Elements("attribute").Where(e => e?.Attribute("name").Value == "emailaddress1").Any();
            if (!emailAttributeExists)
            {
                entity.Add(new XElement("attribute", new XAttribute("name", "emailaddress1")));
            }

            // Return the udpated query XML
            return xDocument.ToString();
        }

        /// <summary>
        /// Retrieve the ID of a campaign list
        /// </summary>
        /// <param name="listName">Name of the list to retrieve the ID</param>
        /// <returns></returns>
        private string GetCampaignListID(string listName)
        {
            // Query a campaign name
            var query = new QueryByAttribute("list")
            {
                ColumnSet = new ColumnSet("listid")
            };
            query.AddAttributeValue("listname", listName);

            var results = dataverseClient.RetrieveMultiple(query);
            return results.Entities.First().Attributes["listid"].ToString();
        }

        /// <summary>
        /// Create XML query that allows for paginated retrieval of campaign contacts.
        /// </summary>
        /// <param name="queryXml"></param>
        /// <param name="pageCookie"></param>
        /// <param name="pageNum"></param>
        /// <param name="recCount"></param>
        /// <returns></returns>
        private static string GetStaticQuery(string listId)
        {
            // Return value containing the query XML
            string queryXml =
                $@" <fetch>
                    <entity name=""listmember"">
                        <attribute name=""entitytype"" />
                        <attribute name=""listmemberid"" />
                        <attribute name=""entityid"" />
                        <filter type=""and"">
                            <condition attribute=""listid"" operator=""eq"" value=""{listId}"" />
                        </filter>
                        <link-entity name=""contact"" from=""contactid"" to=""entityid"" alias=""Contact"">
                            <attribute name=""emailaddress1"" />
                            <attribute name=""fullname"" />
                        </link-entity>
                    </entity>
                </fetch>";

            return queryXml;
        }

        /// <summary>
        /// Add pagination attributes to the Dataverse query XML
        /// </summary>
        /// <param name="queryXml">Query XML for the list</param>
        /// <param name="pageCookie">Cookie used to mark the record that ended the previous page</param>
        /// <param name="pageNum">The page number of the previous page</param>
        /// <param name="pageSize">The number of records in each page</param>
        /// <returns></returns>
        private static string AddPaginationAttributes(string queryXml, string pageCookie, int pageNum, int pageSize)
        {
            StringReader stringReader = new(queryXml);
            XmlReader reader = XmlReader.Create(stringReader);

            // Load document
            XmlDocument doc = new();
            doc.Load(reader);

            XmlAttributeCollection attrs = doc.DocumentElement.Attributes;

            if (pageCookie != null)
            {
                XmlAttribute pagingAttr = doc.CreateAttribute("paging-cookie");
                pagingAttr.Value = pageCookie;
                attrs.Append(pagingAttr);
            }

            XmlAttribute pageAttr = doc.CreateAttribute("page");
            pageAttr.Value = Convert.ToString(pageNum);
            attrs.Append(pageAttr);

            XmlAttribute countAttr = doc.CreateAttribute("count");
            countAttr.Value = Convert.ToString(pageSize);
            attrs.Append(countAttr);

            StringBuilder sb = new(1024);
            StringWriter stringWriter = new(sb);

            XmlTextWriter writer = new(stringWriter);
            doc.WriteTo(writer);
            writer.Close();

            return sb.ToString();
        }

        /// <summary>
        /// Queues campaign email list contacts in the Azure Storage queue.
        /// </summary>
        /// <param name="pageCollection">EntityCollection containing the list of CampaignContact objects</param>
        /// <param name="log">Logger object used to log messages to Log Analytics workspace</param>
        /// <returns></returns>
        private async Task QueueContactsAsync(
            DataCollection<Entity> contactList,
            StartCampaignDto campaignConfig,
            bool isDynamic)
        {
            ServiceBusMessageBatch messageBatch = await serviceBusSender.CreateMessageBatchAsync();

            // Iterate through EntityCollection and queue each campaign contact
            foreach (var contact in contactList)
            {
                var recipientInfo = GetRecipientInfo(isDynamic, contact);

                if (ShouldBlock(recipientInfo.Address))
                {
                    continue;
                }

                ServiceBusMessage message = null;

                if (campaignConfig.ShouldUseBcc)
                {
                    var emailAddressServiceBusMessageDto = new EmailAddressServiceBusMessageDto
                    {
                        CampaignId = campaignConfig.CampaignId,
                        RecipientAddress = recipientInfo
                    };

                    message = new ServiceBusMessage(JsonConvert.SerializeObject(emailAddressServiceBusMessageDto))
                    {
                        Subject = MessageType.Address.ToString()
                    };
                }
                else
                {
                    var emailRequestServiceBusMessageDto = new EmailRequestServiceBusMessageDto
                    {
                        CampaignId = campaignConfig.CampaignId,
                        EmailRecipients = new EmailRecipients
                        {
                            To = new List<EmailAddress> { recipientInfo }
                        },
                        OperationId = Guid.NewGuid().ToString()
                    };

                    message = new ServiceBusMessage(JsonConvert.SerializeObject(emailRequestServiceBusMessageDto))
                    {
                        Subject = MessageType.Request.ToString()
                    };
                }

                message.MessageId = $"{recipientInfo.Address}_{campaignConfig.CampaignId}";

                if (!messageBatch.TryAddMessage(message))
                {
                    await SendMessagesAsync(messageBatch, log);
                    messageBatch.Dispose();
                    messageBatch = await serviceBusSender.CreateMessageBatchAsync();
                    messageBatch.TryAddMessage(message);
                }
            }

            if (messageBatch.Count > 0)
            {
                await SendMessagesAsync(messageBatch, log);
                messageBatch.Dispose();
            }
        }

        private static EmailAddress GetRecipientInfo(bool isDynamic, Microsoft.Xrm.Sdk.Entity contact)
        {
            EmailAddress recipientInfo;

            if (isDynamic)
            {
                recipientInfo = new EmailAddress
                {
                    Address = contact.Attributes["emailaddress1"].ToString(),
                    DisplayName = contact.Attributes["fullname"].ToString()
                };
            }
            else
            {
                recipientInfo = new EmailAddress
                {
                    Address = ((AliasedValue)contact.Attributes["Contact.emailaddress1"]).Value.ToString(),
                    DisplayName = ((AliasedValue)contact.Attributes["Contact.fullname"]).Value.ToString()
                };
            }

            return recipientInfo;
        }

        private bool ShouldBlock(string emailAddress)
        {
            return blockList.Contains(emailAddress);
        }

        private async Task SendMessagesAsync(ServiceBusMessageBatch messageBatch, ILogger log)
        {
            bool isTransientFailure = false;

            do
            {
                try
                {
                    await serviceBusSender.SendMessagesAsync(messageBatch);
                    log.LogInformation($"Sent a batch of {messageBatch.Count} messages to the Service Bus queue");
                    isTransientFailure = false;
                }
                catch (ServiceBusException sbex)
                {
                    isTransientFailure = sbex.IsTransient;

                    if (sbex.Reason == ServiceBusFailureReason.QuotaExceeded)
                    {
                        // Service Bus Queue Quota exceeded. Wait 5 minutes and try again.
                        log.LogInformation("Service Bus Queue Quota exceeded. Waiting 5 minutes and trying again.");
                        await Task.Delay(300000);
                    }
                    else if (sbex.Reason == ServiceBusFailureReason.ServiceBusy)
                    {
                        // Temporary error. Wait 10 seconds and try again.
                        log.LogInformation("Service Bus is busy. Waiting 10 seconds and trying again.");
                        await Task.Delay(10000);
                    }
                }
                catch (Exception ex)
                {
                    log.LogError($"{ex}");
                }
            } while (isTransientFailure);
        }

        private async Task SendEmailAsync(
            Campaign campaign,
            EmailRecipients recipients,
            string operationId,
            List<ServiceBusReceivedMessage> messages,
            MessageType messageType)
        {
            bool retriableError = false;
            SendMailResponse response = null;
            try
            {
                var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));

                // Send the email request
                response = await mailer.SendAsync(campaign, recipients, operationId, cts.Token);
            }
            catch (OperationCanceledException ocex)
            {
                log.LogWarning($"Timeout Exception while sending email - {ocex}");
                retriableError = true;
            }
            catch (Exception ex)
            {
                log.LogWarning($"Exception while sending email - {ex}");
                retriableError = true;
            }

            if (response != null)
            {
                if (response.IsSuccessCode)
                {
                    log.LogInformation($"Sent email request with OperationId: {operationId}.");
                    // Add the operation status to the OperationStatus table
                    await Task.WhenAll(
                        new List<Task>
                        {
                            CompleteMessagesAsync(messages),
                            AddOperationStatusAsync(campaign, recipients, operationId)
                        });
                }
                else
                {
                    if (response.StatusCode == HttpStatusCode.BadRequest)
                    {
                        log.LogError($"SendMail response - {response}. OperationId: {operationId}. Non-retriable error detected. Cleaning up.");
                        await CompleteMessagesAsync(messages);
                    }
                    else
                    {
                        log.LogWarning($"SendMail response - {response}. OperationId: {operationId}.");
                        retriableError = true;
                    }
                }
            }

            if (retriableError)
            {
                log.LogWarning("Waiting for 1 Minute before releasing the lock to retry");
                await Task.Delay(TimeSpan.FromMinutes(1));

                if (messageType == MessageType.Request)
                {
                    await AbandondMessagesAsync(messages);
                }
                else if (messageType == MessageType.Address)
                {
                    var tasks = new List<Task> { CompleteMessagesAsync(messages) };

                    var emailRequestServiceBusMessageDto = new EmailRequestServiceBusMessageDto
                    {
                        CampaignId = campaign.Id,
                        EmailRecipients = recipients,
                        OperationId = operationId
                    };
                    var message = new ServiceBusMessage(JsonConvert.SerializeObject(emailRequestServiceBusMessageDto))
                    {
                        Subject = MessageType.Request.ToString(),
                        MessageId = $"{operationId}_{campaign.Id}"
                    };

                    tasks.Add(serviceBusSender.SendMessageAsync(message));
                    await Task.WhenAll(tasks);
                }
            }
        }

        private async Task AbandondMessagesAsync(List<ServiceBusReceivedMessage> messages)
        {
            foreach (var message in messages)
            {
                await serviceBusReceiver.AbandonMessageAsync(message);
            }
        }

        private async Task CompleteMessagesAsync(List<ServiceBusReceivedMessage> messages)
        {
            // Explicitly call CompleteAsync on the serviceBusReceiver to remove the messages from the queue
            foreach (var message in messages)
            {
                await serviceBusReceiver.CompleteMessageAsync(message);
            }
        }

        private async Task AddOperationStatusAsync(Campaign campaign, EmailRecipients recipients, string operationId)
        {
            IEnumerable<OperationStatusEntity> operationStatusEntities;

            if (campaign.ShouldUseBcc)
            {
                operationStatusEntities = recipients.BCC.Select(recipient => new OperationStatusEntity
                {
                    CampaignId = campaign.Id,
                    PartitionKey = recipient.Address,
                    RowKey = operationId,
                });
            }
            else
            {
                operationStatusEntities = recipients.To.Select(recipient => new OperationStatusEntity
                {
                    CampaignId = campaign.Id,
                    PartitionKey = recipient.Address,
                    RowKey = operationId,
                });
            }

            var tableTasks = new List<Task<Response>>();
            foreach (var operationStatusEntity in operationStatusEntities)
            {
                tableTasks.Add(tableClient.UpsertEntityAsync(operationStatusEntity));
            }

            await Task.WhenAll(tableTasks);
        }

        private static EmailRecipients GetEmailRecipients(List<ServiceBusReceivedMessage> messages)
        {
            List<EmailAddress> recipients = new();

            foreach (var message in messages)
            {
                var dto = JsonConvert.DeserializeObject<EmailAddressServiceBusMessageDto>(Encoding.UTF8.GetString(message.Body));
                recipients.Add(dto.RecipientAddress);
            }

            return new EmailRecipients { BCC = recipients };
        }

        private async Task<bool> IsActiveMessageOnQueueAsync()
        {
            log.LogInformation("Checking if there are any messages on the queue.");
            var message = await serviceBusReceiver.PeekMessageAsync();
            if (message == null)
            {
                log.LogInformation("No messages on the queue.");
            }
            else
            {
                log.LogInformation($"Message found on the queue.");
            }
            return message != null;
        }

        private async Task<bool> ExecuteWithRetriesAsync(Func<Task<bool>> action, [Required, MinLength(3), MaxLength(3)] int[] retryInSecs)
        {
            const int maxRetries = 3;
            TimeSpan[] timeouts = retryInSecs.Select(s => TimeSpan.FromSeconds(s)).ToArray();
            int retries = 0;
            TimeSpan timeout;

            while (true)
            {
                if (await action())
                {
                    return true;
                }

                if (retries >= maxRetries)
                {
                    return false;
                }

                timeout = timeouts[retries];
                retries++;

                log.LogInformation($"Retrying after {timeout}.");
                await Task.Delay(timeout);
            }
        }

        private async Task DeleteRowsByCampiagnIdAsync(string campaignId)
        {
            var entities = tableClient.QueryAsync<OperationStatusEntity>(x => x.CampaignId == campaignId);

            await foreach (var entity in entities)
            {
                await tableClient.DeleteEntityAsync(entity.PartitionKey, entity.RowKey);
            }
        }

        private async Task<CampaignStatisticsResponseDto> GetDeliveryStatisticsByCampaignIdAsync(string campaignId)
        {
            var responseDto = new CampaignStatisticsResponseDto(campaignId);

            var entities = tableClient.QueryAsync<OperationStatusEntity>(x => x.CampaignId == campaignId);

            await foreach (var entity in entities)
            {
                responseDto.TotalOperations++;

                string status = entity.Status;

                if (string.IsNullOrEmpty(status))
                {
                    status = "InProgress";
                }

                if (responseDto.TotalByStatus.TryGetValue(status, out int value))
                {
                    responseDto.TotalByStatus[status] = value + 1;
                }
                else
                {
                    responseDto.TotalByStatus.Add(status, 1);
                }
            }

            return responseDto;
        }

        private async Task<bool> TryCreateTableIfNotExists()
        {
            try
            {
                await tableClient.CreateIfNotExistsAsync();
                return true;
            }
            catch (RequestFailedException rfex) when (rfex.Status == 409)
            {
                return false;
            }
        }

        private static string GetDataverseConnectionString(IConfiguration configuration)
        {
            //string url = configuration.GetValue<string>("DATAVERSE_URL");
            //string appId = configuration.GetValue<string>("DATAVERSE_APPID");
            //string secret = configuration.GetValue<string>("DATAVERSE_SECRET");

            //return $@"AuthType=ClientSecret;
            //    SkipDiscovery=true;url={url};
            //    Secret={secret};
            //    ClientId={appId};
            //    RequireNewInstance=true";

            return configuration.GetConnectionString("DATAVERSE_CONNECTION_STRING");
        }
    }
}