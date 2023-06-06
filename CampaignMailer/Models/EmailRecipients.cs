using Newtonsoft.Json;

namespace CampaignMailer.Models
{
    public class EmailRecipients
    {
        [JsonProperty("to")]
        public List<EmailAddress> To { get; set; }

        [JsonProperty("cc")]
        public List<EmailAddress> CC { get; set; }

        [JsonProperty("bcc")]
        public List<EmailAddress> BCC { get; set; }
    }
}
