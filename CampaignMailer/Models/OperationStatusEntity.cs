using Azure;
using Azure.Data.Tables;
using System;

namespace CampaignMailer.Models
{
    internal class OperationStatusEntity : ITableEntity
    {
        public string CampaignId { get; set; }
        public string Status { get; set; }
        public string PartitionKey { get; set; } // PartitionKey is the RecipientEmailAddress
        public string RowKey { get; set; } // RowKey is the OperationId
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
    }
}
