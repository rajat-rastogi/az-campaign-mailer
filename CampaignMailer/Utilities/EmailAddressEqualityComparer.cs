using Azure.Communication.Email;
using System.Collections.Generic;

namespace CampaignMailer.Utilities
{
    internal class EmailAddressEqualityComparer : IEqualityComparer<EmailAddress>
    {
        public bool Equals(EmailAddress x, EmailAddress y)
        {
            return x.Address == y.Address && x.DisplayName == y.DisplayName;
        }

        public int GetHashCode(EmailAddress obj)
        {
            return (obj.Address + obj.DisplayName).GetHashCode();
        }
    }
}
