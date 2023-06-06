using Newtonsoft.Json;

namespace CampaignMailer.Models
{
    public class EmailAddressServiceBusMessageDto
    {
        [JsonProperty("campaignId")]
        public string CampaignId { get; set; }

        [JsonProperty("recipientAddress")]
        public EmailAddress RecipientAddress { get; set; }
    }
}
