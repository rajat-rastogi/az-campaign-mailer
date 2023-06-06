using System.Net;

namespace CampaignMailer.Models
{
    internal class SendMailResponse
    {
        public bool IsSuccessCode { get; set; }
        public string Message { get; set; }
        public string Code { get; set; }
        public HttpStatusCode StatusCode { get; set; }
        public int RetryAfter { get; set; }

        public override string ToString()
        {
            return $"StatusCode:{StatusCode} ErrorCode:{Code} Message:{Message}";
        }
    }
}
