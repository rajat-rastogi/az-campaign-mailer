using Azure;
using Azure.Communication.Email;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using CampaignMailer.Models;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CampaignMailer.Functions
{
    public class CampaignMailer
    {
        private readonly EmailClient emailClient;
        private readonly BlobContainerClient blobContainer;
        private readonly TableClient tableClient;
        private readonly int numRecipientsPerRequest;
        private readonly bool useBcc;
        private readonly bool useSkipDeliveryHeader;

        public CampaignMailer(IConfiguration configuration)
        {
            emailClient = new EmailClient(configuration.GetConnectionStringOrSetting("COMMUNICATIONSERVICES_CONNECTION_STRING"));
            blobContainer = GetBlobContainer();
            numRecipientsPerRequest = configuration.GetValue<int>("RecipientsPerSendMailRequest");
            useBcc = numRecipientsPerRequest > 1;
            useSkipDeliveryHeader = configuration.GetValue<bool>("UseSkipDeliveryHeader");
            tableClient = InitializeTableClient();
        }

        /// <summary>
        /// HTTP trigger for the CampaignList Durable Function.
        /// </summary>
        /// <param name="req">The HTTPRequestMessage containing the request content.</param>
        /// <param name="client">The Durable Function orchestration client.</param>
        /// <param name="log">The logger instance used to log messages and status.</param>
        /// <returns>The URLs to check the status of the function.</returns>
        [FunctionName("CampaignMailerSBTrigger")]
        public async Task Run([ServiceBusTrigger("contactlist", Connection = "SERVICEBUS_CONNECTION_STRING")] Message[] messageList, ILogger log)
        {
            log.LogInformation($"Received {messageList.Length} messages from Service bus");

            var sendMailTasks = new List<Task>();
            Dictionary<string, Campaign> campaignDictionary = new();

            foreach (var message in messageList)
            {
                var messageBody = Encoding.UTF8.GetString(message.Body);
                var customer = JsonConvert.DeserializeObject<ServiceBusMessageDto>(messageBody);

                // Add campaign to dictionary if it doesn't exists
                if (!campaignDictionary.ContainsKey(customer.CampaignId))
                {
                    var emailBlobContent = await ReadEmailContentFromBlobStream(customer.CampaignId);

                    var newCampaign = new Campaign(customer.CampaignId)
                    {
                        EmailContent = new EmailContent(emailBlobContent.MessageSubject)
                        {
                            Html = emailBlobContent.MessageBodyHtml,
                            PlainText = emailBlobContent.MessageBodyPlainText
                        },
                        ReplyTo = new EmailAddress(emailBlobContent.ReplyToEmailAddress, emailBlobContent.ReplyToDisplayName),
                        SenderEmailAddress = emailBlobContent.SenderEmailAddress
                    };

                    campaignDictionary.Add(customer.CampaignId, newCampaign);
                }

                // Get the campaign
                var campaign = campaignDictionary[customer.CampaignId];

                // Add email to the campaign to send
                campaign.RecipientsList.Add(new EmailAddress(customer.RecipientEmailAddress));

                // Send if the email is full
                if (campaign.RecipientsList.Count == numRecipientsPerRequest)
                {
                    EmailRecipients recipients = GetEmailRecipients(campaign.RecipientsList);
                    sendMailTasks.Add(SendEmailAsync(campaign, recipients, log));
                    campaign.RecipientsList.Clear();
                }
            }

            // send any remaining messages
            foreach (var campaign in campaignDictionary.Values)
            {
                if (campaign.RecipientsList.Count > 0)
                {
                    EmailRecipients recipients = GetEmailRecipients(campaign.RecipientsList);
                    sendMailTasks.Add(SendEmailAsync(campaign, recipients, log));
                }
            }

            await Task.WhenAll(sendMailTasks);
        }

        private async Task SendEmailAsync(Campaign campaign, EmailRecipients recipients, ILogger log)
        {
            try
            {
                EmailMessage message = new(campaign.SenderEmailAddress, recipients, campaign.EmailContent);

                // ***************** REMOVE *********************
                if (useSkipDeliveryHeader)
                {
                    message.Headers.Add("x-ms-acsemail-loadtest-skip-email-delivery", "ACS");
                }

                var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));

                EmailSendOperation emailSendOperation = await emailClient.SendAsync(WaitUntil.Started, message, cts.Token);

                await AddOperationStatusAsync(campaign.Id, recipients, emailSendOperation.Id);
            }
            catch (RequestFailedException ex)
            {
                /// OperationID is contained in the exception message and can be used for troubleshooting purposes
                log.LogError($"Email send operation failed with error code: {ex.ErrorCode}, message: {ex.Message}");
            }
            catch (OperationCanceledException ocex)
            {
                log.LogError($"Timeout Exception while sending email - {ocex}");
            }
            catch (Exception ex)
            {
                log.LogError($"Exception while sending email - {ex}");
            }
        }

        private async Task AddOperationStatusAsync(string campaignId, EmailRecipients recipients, string operationId)
        {
            IEnumerable<OperationStatusEntity> operationStatusEntities;

            if (useBcc)
            {
                operationStatusEntities = recipients.BCC.Select(recipient => new OperationStatusEntity
                {
                    CampaignId = campaignId,
                    PartitionKey = recipient.Address,
                    RowKey = operationId,
                });
            }
            else
            {
                operationStatusEntities = recipients.To.Select(recipient => new OperationStatusEntity
                {
                    CampaignId = campaignId,
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

        private static BlobContainerClient GetBlobContainer()
        {
            // Retrieve the Azure Storage account connection string and blob container name from app settings
            string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string containerName = "campaigns";

            // Create the container if it doesn't exist
            var container = new BlobContainerClient(storageConnectionString, containerName);
            container.CreateIfNotExists();

            return container;
        }

        private async Task<BlobDto> ReadEmailContentFromBlobStream(string campaignId)
        {
            var blobName = campaignId + ".json";
            var blobClient = blobContainer.GetBlobClient(blobName);
            using var stream = new MemoryStream();
            await blobClient.DownloadToAsync(stream);

            var blobContentSerializedString = Encoding.UTF8.GetString(stream.ToArray());

            try
            {
                var blobContentDeSerialized = JsonConvert.DeserializeObject<BlobDto>(blobContentSerializedString);
                return blobContentDeSerialized;
            }
            catch (Exception)
            {
                throw;
            }
        }

        private static TableClient InitializeTableClient()
        {
            TableClient tableClient = null;

            try
            {
                var serviceClient = new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
                tableClient = serviceClient.GetTableClient("OperationStatus");
                tableClient.CreateIfNotExists();
            }
            catch (Exception ex)
            {
                //log.LogError($"{ex}");
            }

            return tableClient;
        }

        private EmailRecipients GetEmailRecipients(HashSet<EmailAddress> recipents)
        {
            return useBcc ? new EmailRecipients(bcc: recipents) : new EmailRecipients(recipents);
        }
    }
}