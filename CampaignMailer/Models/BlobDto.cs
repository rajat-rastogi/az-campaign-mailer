namespace CampaignMailer.Models
{
    public class BlobDto
    {
        public string MessageSubject { get; set; }

        public string MessageBodyHtml { get; set; }

        public string MessageBodyPlainText { get; set; }

        public string SenderEmailAddress { get; set; }

        public string ReplyToEmailAddress { get; set; }

        public string ReplyToDisplayName { get; set; }

        public int MaxRecipientsPerSendMailRequest { get; set; }
    }
}
