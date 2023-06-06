using Azure.Communication.Email;
using Azure.Messaging.ServiceBus;
using CampaignMailer.Utilities;

namespace CampaignMailer.Models
{
    public class Campaign
    {
        private readonly MessageCollection messages;
        public Campaign(StartCampaignDto startCampaignDto)
        {
            Id = ValidateAndGetString(startCampaignDto.CampaignId, nameof(startCampaignDto.CampaignId));
            EmailContent = new EmailContent(startCampaignDto.MessageSubject)
            {
                Html = startCampaignDto.MessageBodyHtml,
                PlainText = startCampaignDto.MessageBodyPlainText
            };
            ReplyTo = new EmailAddress { Address = startCampaignDto.ReplyToEmailAddress, DisplayName = startCampaignDto.ReplyToDisplayName };
            SenderEmailAddress = ValidateAndGetString(startCampaignDto.SenderEmailAddress, nameof(startCampaignDto.SenderEmailAddress));
            MaxRecipientsPerSendMailRequest = startCampaignDto.MaxRecipientsPerSendMailRequest;
            messages = new MessageCollection(startCampaignDto.MaxRecipientsPerSendMailRequest);
            messages.CountThresholdReached += Messages_CountThresholdReached;
            messages.FinalPayloadReached += Messages_FinalPayloadReached;
        }

        public string Id { get; }

        public EmailContent EmailContent { get; }

        public EmailAddress ReplyTo { get; }

        public string SenderEmailAddress { get; }

        public int MaxRecipientsPerSendMailRequest { get; }

        public bool ShouldUseBcc => MaxRecipientsPerSendMailRequest > 1;

        public event Func<ReadyToSendMailEventArgs, Task> ReadyToSendMail;

        public void AddMessage(ServiceBusReceivedMessage message)
        {
            messages.Add(message);
        }

        protected virtual void OnReadyToSendMail(ReadyToSendMailEventArgs e)
        {
            ReadyToSendMail?.Invoke(e);
        }

        private void Messages_CountThresholdReached(object sender, CountThresholdReachedEventArgs e)
        {
            OnReadyToSendMail(new ReadyToSendMailEventArgs(this, e));
        }

        private void Messages_FinalPayloadReached(object sender, FinalPayloadEventArgs e)
        {
            OnReadyToSendMail(new ReadyToSendMailEventArgs(this, e));
        }

        private static string ValidateAndGetString(string value, string name)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentNullException(name);
            }

            return value;
        }
    }
}
