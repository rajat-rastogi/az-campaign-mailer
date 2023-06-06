namespace CampaignMailer.Models
{
    public class ReadyToSendMailEventArgs : MessageListEventArgs
    {
        public Campaign Campaign { get; }

        public ReadyToSendMailEventArgs(Campaign campaign, MessageListEventArgs args) : base(args)
        {
            Campaign = campaign;
        }
    }
}
