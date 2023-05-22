using CampaignMailer.Exceptions;
using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace CampaignMailer.Utilities
{
    public class ThrottlingDelegatingHandler : DelegatingHandler
    {
        private readonly SemaphoreSlim semaphore;
        private readonly int maxRequests;
        private readonly TimeSpan timeSpan;

        public ThrottlingDelegatingHandler(int maxRequestsPerMinute)
        {
            this.maxRequests = maxRequestsPerMinute;
            this.timeSpan = TimeSpan.FromMinutes(1);
            this.semaphore = new SemaphoreSlim(maxRequestsPerMinute, maxRequestsPerMinute);
            this.resettimer = new Timer(o => this.semaphore.Release(maxRequestsPerMinute), null, this.timeSpan, this.timeSpan);
        }

        private readonly Timer resettimer;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (!await this.semaphore.WaitAsync(TimeSpan.Zero, cancellationToken))
            {
                throw new RateLimitExceededException("Rate limit exceeded");
            }

            try
            {
                return await base.SendAsync(request, cancellationToken);
            }
            finally
            {
                this.semaphore.Release();
            }
        }
    }
}
