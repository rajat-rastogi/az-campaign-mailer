using Azure.Communication.Email;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;
using CampaignMailer.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.PowerPlatform.Dataverse.Client;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace CampaignMailer.Functions
{
    public static class CampaignList
    {
        private static ServiceClient dataverseClient = null;
        private static HashSet<string> blockList = new();
        private static ServiceBusClient serviceBusClient = null;
        private static ServiceBusSender serviceBusSender = null;


        /// <summary>
        /// HTTP trigger for the CampaignList Durable Function.
        /// </summary>
        /// <param name="req">The HTTPRequestMessage containing the request content.</param>
        /// <param name="client">The Durable Function orchestration client.</param>
        /// <param name="log">The logger instance used to log messages and status.</param>
        /// <returns>The URLs to check the status of the function.</returns>
        [FunctionName("CampaignListHttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
           [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = "startCampaign")] HttpRequestMessage req,
           [DurableClient] IDurableOrchestrationClient client,
           ILogger log)
        {
            // Get the campaign information from the HTTP request body
            CampaignConfiguration campaignConfig = await req.Content.ReadAsAsync<CampaignConfiguration>();

            // Function input comes from the request content.
            string instanceId = await client.StartNewAsync("CampaignListOrchestrator", campaignConfig);

            log.LogInformation($"Started orchestration with ID = '{instanceId}", instanceId);

            // Create the URL to allow the client to check status of a request (excluding the function key in the code querystring)
            string checkStatusUrl = string.Format("{0}://{1}:{2}/campaign/CampaignListHttpStart_Status?id={3}", req.RequestUri.Scheme, req.RequestUri.Host, req.RequestUri.Port, instanceId);

            // Create the response and add headers
            var response = new HttpResponseMessage()
            {
                StatusCode = HttpStatusCode.Accepted,
                Content = new StringContent(checkStatusUrl),
            };
            response.Headers.Add("Location", checkStatusUrl);
            response.Headers.Add("Retry-After", "10");

            return response;
        }


        /// <summary>
        /// Orchestrates all activity functions for the campaign list queuing process.
        /// </summary>
        /// <param name="context">The context object of the orchestration.</param>
        /// <param name="log">The logger instance used to log messages and status.</param>
        /// <returns></returns>
        [FunctionName("CampaignListOrchestrator")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            ILogger log,
            ExecutionContext executionContext)
        {
            log.LogInformation($"************** RunOrchestrator method executing ********************");

            // Retrive the campaign configuration object from the HTTP context
            CampaignConfiguration campaignConfig = context.GetInput<CampaignConfiguration>();

            try
            {
                await UploadCampaign(campaignConfig, log);
            }
            catch (Exception ex)
            {
                log.LogError($"Failed to upload campaign {campaignConfig.CampaignId} to blob storage {ex}");
                return "Failed";
            }

            blockList = GetBlockList(executionContext);

            // Create the Dataverse ServiceClient by connecting to the Dataverse database
            if (dataverseClient == null)
            {
                InitializeDataverseClient(log);
            }

            // Create the ServiceBusSender to send messages to the Service Bus queue
            if (serviceBusSender == null)
            {
                InitializeServiceBusSender(log);
            }

            // Process the campaign list
            if (!string.IsNullOrWhiteSpace(campaignConfig.ListName))
            {
                if (dataverseClient.IsReady)
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
                        while (morePages && totalContactCount < 10000)
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
                                queueTasks.Add(QueueContactsAsync(pageCollection.Entities, campaignConfig, isDynamic, log));
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
                    finally
                    {
                        await CleanupServiceBusClientAsync();
                    }

                    log.LogInformation($"Total number of contacts in the list: {totalContactCount}");
                    log.LogInformation($"Successfully completed processing {campaignConfig.ListName}");
                }
                else
                {
                    log.LogError($"A web service connection was not established. Campaign list {campaignConfig.ListName} was not processed");
                }
            }
            else
            {
                log.LogError("Campaign list name was empty. Please provide the name of a campaign list to process.");
            }

            // Wait until all the activity functions have done their work
            log.LogInformation($"************** 'Waiting' for parallel results ********************");
            log.LogInformation($"************** All activity functions complete ********************");

            return "Finished";
        }

        /// <summary>
        /// Queues campaign email list contacts in the Azure Storage queue.
        /// </summary>
        /// <param name="pageCollection">EntityCollection containing the list of CampaignContact objects</param>
        /// <param name="log">Logger object used to log messages to Log Analytics workspace</param>
        /// <returns></returns>
        public static async Task QueueContactsAsync(
            DataCollection<Microsoft.Xrm.Sdk.Entity> contactList,
            CampaignConfiguration campaignConfig,
            bool isDynamic,
            ILogger log)
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
                        EmailRecipients = new EmailRecipients(new List<EmailAddress> { recipientInfo }),
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
                recipientInfo = new(contact.Attributes["emailaddress1"].ToString(), contact.Attributes["fullname"].ToString());
            }
            else
            {
                recipientInfo = new(((AliasedValue)contact.Attributes["Contact.emailaddress1"]).Value.ToString(), ((AliasedValue)contact.Attributes["Contact.fullname"]).Value.ToString());
            }

            return recipientInfo;
        }

        public static async Task SendMessagesAsync(ServiceBusMessageBatch messageBatch, ILogger log)
        {
            try
            {
                await serviceBusSender.SendMessagesAsync(messageBatch);
                log.LogInformation($"Sent a batch of {messageBatch.Count} messages to the Service Bus queue");
            }
            catch (ServiceBusException sbex)
            {
                log.LogError($"ServiceBusException: {sbex.Message}");

                try
                {
                    if (sbex.Reason == ServiceBusFailureReason.QuotaExceeded)
                    {
                        // Service Bus Queue Quota exceeded. Wait 5 minutes and try again.
                        await Task.Delay(300000);
                        await serviceBusSender.SendMessagesAsync(messageBatch);
                        log.LogInformation($"Sent a batch of {messageBatch.Count} messages to the Service Bus queue");
                    }
                    else if (sbex.Reason == ServiceBusFailureReason.ServiceBusy)
                    {
                        // Temporary error. Wait 10 seconds and try again.
                        await Task.Delay(10000);
                        await serviceBusSender.SendMessagesAsync(messageBatch);
                        log.LogInformation($"Sent a batch of {messageBatch.Count} messages to the Service Bus queue");
                    }
                }
                catch (Exception ex)
                {
                    log.LogError($"{ex}");
                }
            }
            catch (Exception ex)
            {
                log.LogError($"{ex}");
            }
        }


        /// <summary>
        /// Create XML query that allows for paginated retrieval of campaign contacts.
        /// </summary>
        /// <param name="queryXml"></param>
        /// <param name="pageCookie"></param>
        /// <param name="pageNum"></param>
        /// <param name="recCount"></param>
        /// <returns></returns>
        public static string GetDynamicQuery(string listName)
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
        /// Create XML query that allows for paginated retrieval of campaign contacts.
        /// </summary>
        /// <param name="queryXml"></param>
        /// <param name="pageCookie"></param>
        /// <param name="pageNum"></param>
        /// <param name="recCount"></param>
        /// <returns></returns>
        public static string GetStaticQuery(string listId)
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
        public static string AddPaginationAttributes(string queryXml, string pageCookie, int pageNum, int pageSize)
        {
            StringReader stringReader = new(queryXml);
            XmlReader reader = XmlReader.Create(stringReader);          //new XmlTextReader(stringReader);

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
        /// Determines if the selected campaign list is dynamic or static.
        /// </summary>
        /// <param name="listName"></param>
        /// <returns>true if dynamic or false if static.</returns>
        public static bool IsDynamic(string listName)
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
        /// Retrieve the ID of a campaign list
        /// </summary>
        /// <param name="listName">Name of the list to retrieve the ID</param>
        /// <returns></returns>
        public static string GetCampaignListID(string listName)
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
        /// Add the email attribute to the campaign list query XML if it is not in the query already. 
        /// add the attribute.
        /// </summary>
        /// <param name="queryXml">The </param>
        /// <returns></returns> 
        public static string AddEmailAttribute(string queryXml)
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
        /// Connects to the campaign database using the Dataverse API.
        /// </summary>
        /// <returns>Returns a ServiceClient used to access and query the database.</returns>
        private static void InitializeDataverseClient(ILogger log)
        {
            // Dataverse environment URL and auth credentials.
            // Dataverse environment URL and login info.
            //string url = Environment.GetEnvironmentVariable("DATAVERSE_URL");
            //string appId = Environment.GetEnvironmentVariable("DATAVERSE_APPID");
            //string secret = Environment.GetEnvironmentVariable("DATAVERSE_SECRET");



            //// Create the Dataverse connection string
            //string connectionString =
            //    $@"AuthType=ClientSecret;
            //    SkipDiscovery=true;url={url};
            //    Secret={secret};
            //    ClientId={appId};
            //    RequireNewInstance=true";

            string connectionString = Environment.GetEnvironmentVariable("DATAVERSE_CONNECTION_STRING");

            try
            {
                // Create the Dataverse ServiceClient instance
                dataverseClient = new(connectionString);
                log.LogInformation("Successfully created the Dataverse service client");
            }
            catch (Exception ex)
            {
                log.LogError($"Exception thrown creating the Dataverse ServiceClient: {ex}");
                throw;
            }
        }

        private static HashSet<string> GetBlockList(ExecutionContext context)
        {
            HashSet<string> addresses = new();
            string path = Path.Combine(context.FunctionAppDirectory, "BlockList.txt");
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

        private static bool ShouldBlock(string emailAddress)
        {
            return blockList.Contains(emailAddress);
        }

        private static string GetSerializedBlobDto(CampaignConfiguration campaignConfiguration)
        {
            BlobDto blobDto = new()
            {
                MessageBodyHtml = campaignConfiguration.MessageBodyHtml,
                MessageBodyPlainText = campaignConfiguration.MessageBodyPlainText,
                MessageSubject = campaignConfiguration.MessageSubject,
                SenderEmailAddress = campaignConfiguration.SenderEmailAddress,
                ReplyToEmailAddress = campaignConfiguration.ReplyToEmailAddress,
                ReplyToDisplayName = campaignConfiguration.ReplyToDisplayName,
                MaxRecipientsPerSendMailRequest = campaignConfiguration.MaxRecipientsPerSendMailRequest,
            };

            return JsonConvert.SerializeObject(blobDto);
        }

        private static async Task UploadCampaign(CampaignConfiguration campaignConfiguration, ILogger log)
        {
            string blobFileName = campaignConfiguration.CampaignId + ".json";

            // Retrieve the Azure Storage account connection string and blob container name from app settings
            string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string containerName = "campaigns";

            // Create the container if it doesn't exist
            var container = new BlobContainerClient(storageConnectionString, containerName);
            await container.CreateIfNotExistsAsync();

            // Retrieve a reference to the blob
            var blob = container.GetBlobClient(blobFileName);

            // Upload the JSON data to the blob
            using (Stream stream = new MemoryStream(Encoding.UTF8.GetBytes(GetSerializedBlobDto(campaignConfiguration))))
            {
                await blob.UploadAsync(stream);
            }

            log.LogInformation($"Uploaded JSON file '{blobFileName}' to blob container '{containerName}'");
        }

        private static void InitializeServiceBusSender(ILogger log)
        {
            string connectionString = Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTION_STRING");
            string queueName = "contactlist";

            try
            {
                var clientOptions = new ServiceBusClientOptions()
                {
                    TransportType = ServiceBusTransportType.AmqpWebSockets
                };
                serviceBusClient = new ServiceBusClient(connectionString, clientOptions);
                serviceBusSender = serviceBusClient.CreateSender(queueName);
                log.LogInformation("Successfully created the Service Bus client");
            }
            catch (Exception ex)
            {
                log.LogError($"Exception thrown creating the Service Bus client: {ex}");
                throw;
            }
        }

        private static async Task CleanupServiceBusClientAsync()
        {
            await serviceBusSender.DisposeAsync();
            await serviceBusClient.DisposeAsync();
            serviceBusSender = null;
            serviceBusClient = null;
        }
    }
}