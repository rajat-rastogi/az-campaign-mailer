using Azure.Messaging.ServiceBus;

namespace CampaignMailer.Models
{
    public class MessageListEventArgs : EventArgs
    {
        public List<ServiceBusReceivedMessage> Messages { get; }

        public MessageListEventArgs(List<ServiceBusReceivedMessage> messages)
        {
            Messages = new(messages);
        }

        public MessageListEventArgs(MessageListEventArgs args) : this(args.Messages)
        {
        }
    }
}
