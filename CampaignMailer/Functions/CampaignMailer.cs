using Azure;
using Azure.Communication.Email;
using Azure.Data.Tables;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;
using CampaignMailer.Exceptions;
using CampaignMailer.Models;
using CampaignMailer.Utilities;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CampaignMailer.Functions
{
    public class CampaignMailer
    {
        private readonly BlobContainerClient blobContainer;
        private readonly TableClient tableClient;
        private readonly Mailer mailer;
        private readonly ConcurrentDictionary<string, BlobDto> campaignBlobs;


        public CampaignMailer(IConfiguration configuration)
        {
            mailer = new Mailer(configuration.GetConnectionStringOrSetting("COMMUNICATIONSERVICES_CONNECTION_STRING"), configuration.GetValue<int>("MaxRequestsPerMinute"));
            blobContainer = GetBlobContainer();
            tableClient = InitializeTableClient();
            campaignBlobs = new ConcurrentDictionary<string, BlobDto>();
        }

        /// <summary>
        /// HTTP trigger for the CampaignList Durable Function.
        /// </summary>
        /// <param name="req">The HTTPRequestMessage containing the request content.</param>
        /// <param name="client">The Durable Function orchestration client.</param>
        /// <param name="log">The logger instance used to log messages and status.</param>
        /// <returns>The URLs to check the status of the function.</returns>
        [FunctionName("CampaignMailerSBTrigger")]
        public async Task Run(
            [ServiceBusTrigger("contactlist", Connection = "SERVICEBUS_CONNECTION_STRING")] ServiceBusReceivedMessage[] messageList,
            ServiceBusSender serviceBusSender,
            ServiceBusReceiver serviceBusReceiver,
            ILogger log)
        {
            log.LogInformation($"Received {messageList.Length} messages from Service bus");

            var sendMailTasks = new List<Task>();
            Dictionary<string, Campaign> campaignDictionary = new();

            foreach (var message in messageList)
            {
                var messageBody = Encoding.UTF8.GetString(message.Body);

                if (string.Equals(message.Subject, MessageType.Request.ToString()))
                {
                    var requestDetails = JsonConvert.DeserializeObject<EmailRequestServiceBusMessageDto>(messageBody);

                    // Add campaign to dictionary if it doesn't exists
                    if (!campaignDictionary.ContainsKey(requestDetails.CampaignId))
                    {
                        campaignDictionary.Add(requestDetails.CampaignId, GetCampaign(requestDetails.CampaignId, await GetBlob(requestDetails.CampaignId)));
                    }

                    // Send the email
                    sendMailTasks.Add(
                        SendEmailAsync(
                            campaignDictionary[requestDetails.CampaignId],
                            requestDetails.EmailRecipients,
                            requestDetails.OperationId,
                            new List<ServiceBusReceivedMessage> { message },
                            serviceBusSender,
                            serviceBusReceiver,
                            MessageType.Request,
                            log));
                }
                else if (string.Equals(message.Subject, MessageType.Address.ToString()))
                {
                    var recipientDetails = JsonConvert.DeserializeObject<EmailAddressServiceBusMessageDto>(messageBody);

                    // Add campaign to dictionary if it doesn't exists
                    if (!campaignDictionary.ContainsKey(recipientDetails.CampaignId))
                    {
                        campaignDictionary.Add(recipientDetails.CampaignId, GetCampaign(recipientDetails.CampaignId, await GetBlob(recipientDetails.CampaignId)));
                    }

                    // Get the campaign
                    var campaign = campaignDictionary[recipientDetails.CampaignId];

                    // Add email to the campaign to send
                    campaign.Recipients.Add(recipientDetails.RecipientAddress, message);

                    // Send if the email is full
                    if (campaign.Recipients.Count == campaign.MaxRecipientsPerSendMailRequest)
                    {
                        EmailRecipients recipients = GetEmailRecipients(campaign);
                        sendMailTasks.Add(
                            SendEmailAsync(
                                campaign,
                                recipients,
                                Guid.NewGuid().ToString(),
                                new List<ServiceBusReceivedMessage>(campaign.Recipients.Values),
                                serviceBusSender,
                                serviceBusReceiver,
                                MessageType.Address,
                                log));
                        campaign.Recipients.Clear();
                    }
                }
            }

            // send any remaining messages
            foreach (var campaign in campaignDictionary.Values)
            {
                if (campaign.Recipients.Count > 0)
                {
                    EmailRecipients recipients = GetEmailRecipients(campaign);
                    sendMailTasks.Add(
                        SendEmailAsync(
                            campaign,
                            recipients,
                            Guid.NewGuid().ToString(),
                            new List<ServiceBusReceivedMessage>(campaign.Recipients.Values),
                            serviceBusSender,
                            serviceBusReceiver,
                            MessageType.Address,
                            log));
                }
            }

            await Task.WhenAll(sendMailTasks);
        }

        private static Campaign GetCampaign(string campaignId, BlobDto blob)
        {
            return new Campaign(campaignId)
            {
                EmailContent = new EmailContent(blob.MessageSubject)
                {
                    Html = blob.MessageBodyHtml,
                    PlainText = blob.MessageBodyPlainText
                },
                ReplyTo = new EmailAddress(blob.ReplyToEmailAddress, blob.ReplyToDisplayName),
                SenderEmailAddress = blob.SenderEmailAddress,
                MaxRecipientsPerSendMailRequest = blob.MaxRecipientsPerSendMailRequest
            };
        }

        private async Task SendEmailAsync(
            Campaign campaign,
            EmailRecipients recipients,
            string operationId,
            List<ServiceBusReceivedMessage> messages,
            ServiceBusSender serviceBusSender,
            ServiceBusReceiver serviceBusReceiver,
            MessageType messageType,
            ILogger log)
        {
            bool retriableError = false;
            SendMailResponse response = null;
            try
            {
                var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));

                // Send the email request
                response = await mailer.SendAsync(campaign, recipients, operationId, cts.Token);
            }
            catch (RateLimitExceededException)
            {
                log.LogWarning($"Rate limit exceeded detected on the client. Not sending the request.");
                retriableError = true;
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
                    // Add the operation status to the OperationStatus table
                    await Task.WhenAll(
                        new List<Task>
                        {
                            CompleteMessagesAsync(messages, serviceBusReceiver),
                            AddOperationStatusAsync(campaign, recipients, operationId)
                        });
                }
                else
                {
                    if (response.StatusCode == HttpStatusCode.BadRequest)
                    {
                        log.LogError($"SendMail response - {response}. OperationId: {operationId}. Non-retriable error detected. Cleaning up.");
                        await CompleteMessagesAsync(messages, serviceBusReceiver);
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
                log.LogWarning("Retriable error detected. Waiting for 1 Minute before releasing the lock to retry");
                await Task.Delay(TimeSpan.FromMinutes(1));

                if (messageType == MessageType.Request)
                {
                    await AbandondMessagesAsync(messages, serviceBusReceiver);
                }
                else if (messageType == MessageType.Address)
                {
                    var tasks = new List<Task> { CompleteMessagesAsync(messages, serviceBusReceiver) };

                    var emailRequestServiceBusMessageDto = new EmailRequestServiceBusMessageDto
                    {
                        CampaignId = campaign.Id,
                        EmailRecipients = recipients,
                        OperationId = operationId
                    };
                    var message = new ServiceBusMessage(JsonConvert.SerializeObject(emailRequestServiceBusMessageDto))
                    {
                        Subject = MessageType.Request.ToString()
                    };

                    tasks.Add(serviceBusSender.SendMessageAsync(message));
                    await Task.WhenAll(tasks);
                }
            }
        }

        private static async Task AbandondMessagesAsync(List<ServiceBusReceivedMessage> messages, ServiceBusReceiver serviceBusReceiver)
        {
            foreach (var message in messages)
            {
                await serviceBusReceiver.AbandonMessageAsync(message);
            }
        }

        private static async Task CompleteMessagesAsync(List<ServiceBusReceivedMessage> messages, ServiceBusReceiver serviceBusReceiver)
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
            return JsonConvert.DeserializeObject<BlobDto>(Encoding.UTF8.GetString(stream.ToArray()));
        }

        private async Task<BlobDto> GetBlob(string campaignId)
        {
            if (!campaignBlobs.ContainsKey(campaignId))
            {
                campaignBlobs.TryAdd(campaignId, await ReadEmailContentFromBlobStream(campaignId));
            }

            return campaignBlobs[campaignId];
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

        private static EmailRecipients GetEmailRecipients(Campaign campaign)
        {
            var recipients = campaign.Recipients.Keys;
            return campaign.ShouldUseBcc ? new EmailRecipients(bcc: recipients) : new EmailRecipients(recipients);
        }
    }
}