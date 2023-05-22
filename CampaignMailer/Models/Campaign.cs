using Azure.Communication.Email;
using Azure.Messaging.ServiceBus;
using CampaignMailer.Utilities;
using System.Collections.Generic;

namespace CampaignMailer.Models
{
    internal class Campaign
    {
        public Campaign(string id)
        {
            Id = id;
            Recipients = new Dictionary<EmailAddress, ServiceBusReceivedMessage>(new EmailAddressEqualityComparer());
        }

        public string Id { get; }

        public EmailContent EmailContent { get; set; }

        public EmailAddress ReplyTo { get; set; }

        public string SenderEmailAddress { get; set; }

        public int MaxRecipientsPerSendMailRequest { get; set; }

        public bool ShouldUseBcc => MaxRecipientsPerSendMailRequest > 1;

        public Dictionary<EmailAddress, ServiceBusReceivedMessage> Recipients { get; }
    }
}
