namespace CampaignMailer.Models
{
    internal class CampaignStatisticsResponseDto
    {
        public string CampaignId { get; }
        public int TotalOperations { get; set; }
        public Dictionary<string, int> TotalByStatus { get; }

        public CampaignStatisticsResponseDto(string campaignId)
        {
            CampaignId = campaignId;
            TotalOperations = 0;
            TotalByStatus = new Dictionary<string, int>();
        }
    }
}