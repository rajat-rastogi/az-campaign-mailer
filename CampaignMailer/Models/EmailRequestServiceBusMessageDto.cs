using Newtonsoft.Json;

namespace CampaignMailer.Models
{
    public class EmailRequestServiceBusMessageDto
    {
        [JsonProperty("campaignId")]
        public string CampaignId { get; set; }

        [JsonProperty("emailRecipients")]
        public EmailRecipients EmailRecipients { get; set; }

        [JsonProperty("operationId")]
        public string OperationId { get; set; }
    }
}
