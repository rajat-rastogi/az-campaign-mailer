using Newtonsoft.Json;

namespace CampaignMailer.Models
{
    public class ServiceBusMessageDto
    {
        [JsonProperty("campaignId")]
        public string CampaignId { get; set; }

        [JsonProperty("recipientEmailAddress")]
        public string RecipientEmailAddress { get; set; }

        [JsonProperty("recipientFullName")]
        public string RecipientFullName { get; set; }
    }
}
