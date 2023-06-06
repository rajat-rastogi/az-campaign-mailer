using Azure.Messaging.ServiceBus;

namespace CampaignMailer.Models
{
    public class CountThresholdReachedEventArgs : MessageListEventArgs
    {
        public CountThresholdReachedEventArgs(List<ServiceBusReceivedMessage> messages) : base(messages)
        {
        }

        public CountThresholdReachedEventArgs(CountThresholdReachedEventArgs args) : this(args.Messages)
        {
        }
    }
}
