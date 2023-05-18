using Azure.Communication.Email;
using CampaignMailer.Utilities;
using System.Collections.Generic;

namespace CampaignMailer.Models
{
    internal class Campaign
    {
        public Campaign(string id)
        {
            Id = id;
        }

        public string Id { get; }

        public EmailContent EmailContent { get; set; }

        public EmailAddress ReplyTo { get; set; }

        public string SenderEmailAddress { get; set; }

        public HashSet<EmailAddress> RecipientsList { get; } = new HashSet<EmailAddress>(new EmailAddressEqualityComparer());
    }
}
