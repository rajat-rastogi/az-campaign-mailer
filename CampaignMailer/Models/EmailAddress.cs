using Newtonsoft.Json;

namespace CampaignMailer.Models
{
    public class EmailAddress
    {
        [JsonProperty("address")]
        public string Address { get; set; }

        [JsonProperty("displayName")]
        public string DisplayName { get; set; }
    }
}
