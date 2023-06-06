namespace CampaignMailer.Models
{
    /// <summary>
    /// Campaign configuration data used to encapsulate the HTTP message body data.
    /// </summary>
    public class StartCampaignDto
    {
        public string CampaignId { get; set; }

        public string ListName { get; set; }

        public int PageSize { get; set; }

        public string MessageSubject { get; set; }

        public string MessageBodyHtml { get; set; }

        public string MessageBodyPlainText { get; set; }

        public string SenderEmailAddress { get; set; }

        public string ReplyToEmailAddress { get; set; }

        public string ReplyToDisplayName { get; set; }

        private int maxRecipientsPerSendMailRequest;
        public int MaxRecipientsPerSendMailRequest
        {
            get { return maxRecipientsPerSendMailRequest; }
            set { maxRecipientsPerSendMailRequest = Math.Min(Math.Max(value, 1), 50); }
        }

        public bool SkipFetchContacts { get; set; }

        internal bool ShouldUseBcc => maxRecipientsPerSendMailRequest > 1;
    }
}