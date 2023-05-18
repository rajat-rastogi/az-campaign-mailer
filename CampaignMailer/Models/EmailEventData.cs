using System;

namespace CampaignMailer.Models
{
    internal class EmailEventData
    {
        public string Sender { get; set; }
        public string Recipient { get; set; }
        public string MessageId { get; set; }
        public string Status { get; set; }
        public DateTimeOffset DeliveryAttemptTimestamp { get; set; }

        public override string ToString()
        {
            return $"Sender={Sender}, Recipient={Recipient}, MessageId={MessageId}, Status={Status}, DeliveryAttemptTimestamp={DeliveryAttemptTimestamp}";
        }
    }
}
