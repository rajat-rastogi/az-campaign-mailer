using Azure.Messaging.ServiceBus;
using CampaignMailer.Models;
using System.Collections.Concurrent;

namespace CampaignMailer.Utilities
{
    public class MessageCollection
    {
        private readonly ConcurrentQueue<ServiceBusReceivedMessage> messages;
        private readonly int capacity;
        private readonly object lockObject;
        private readonly Timer timer;

        public event EventHandler<CountThresholdReachedEventArgs> CountThresholdReached;
        public event EventHandler<FinalPayloadEventArgs> FinalPayloadReached;

        public MessageCollection(int capacity)
        {
            this.capacity = capacity;
            messages = new();
            lockObject = new();
            timer = new Timer(OnTimerElapsed, null, Timeout.Infinite, Timeout.Infinite);
        }

        public void Add(ServiceBusReceivedMessage message)
        {
            messages.Enqueue(message);
            timer.Change(TimeSpan.FromMinutes(1), Timeout.InfiniteTimeSpan);

            lock (lockObject)
            {
                if (messages.Count >= capacity)
                {
                    OnCountThresholdReached(new CountThresholdReachedEventArgs(Pop()));
                } 
            }
        }

        protected virtual void OnCountThresholdReached(CountThresholdReachedEventArgs e)
        {
            CountThresholdReached?.Invoke(this, e);
        }

        protected virtual void OnFinalPayloadReached(FinalPayloadEventArgs e)
        {
            FinalPayloadReached?.Invoke(this, e);
        }

        private void OnTimerElapsed(object state)
        {
            lock (lockObject)
            {
                while (messages.Count > 0)
                {
                    OnFinalPayloadReached(new FinalPayloadEventArgs(Pop()));
                }
            }
        }

        private List<ServiceBusReceivedMessage> Pop()
        {
            var messagesToReturn = new List<ServiceBusReceivedMessage>();

            while (messagesToReturn.Count < capacity && messages.TryDequeue(out var message))
            {
                messagesToReturn.Add(message);
            }

            return messagesToReturn;
        }
    }
}
