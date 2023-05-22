using System;
using System.Runtime.Serialization;

namespace CampaignMailer.Exceptions
{
    [Serializable]
    internal class RateLimitExceededException : Exception
    {
        public RateLimitExceededException()
        {
        }

        public RateLimitExceededException(string message) : base(message)
        {
        }

        public RateLimitExceededException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected RateLimitExceededException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}