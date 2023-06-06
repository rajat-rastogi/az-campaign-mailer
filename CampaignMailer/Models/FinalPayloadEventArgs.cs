using Azure.Messaging.ServiceBus;

namespace CampaignMailer.Models
{
    public class FinalPayloadEventArgs : MessageListEventArgs
    {
        public FinalPayloadEventArgs(List<ServiceBusReceivedMessage> messages) : base(messages)
        {
        }

        public FinalPayloadEventArgs(FinalPayloadEventArgs args) : this(args.Messages)
        {
        }
    }
}
