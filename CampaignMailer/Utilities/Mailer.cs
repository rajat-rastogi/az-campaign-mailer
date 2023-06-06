using CampaignMailer.Models;
using Newtonsoft.Json;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace CampaignMailer.Utilities
{
    internal class Mailer
    {
        private const string ApiVersion = "2023-03-31";
        private const string endpointGroupName = "endpoint";
        private const string accessKeyGroupName = "accesskey";
        private const string ConnectionStringPattern = @$"endpoint=(?<{endpointGroupName}>\S+);accesskey=(?<{accessKeyGroupName}>\S+)";
        private readonly HttpClient httpClient;
        private readonly Uri requestUri;
        private readonly string accessKey;

        public Mailer(string connectionString)
        {
            var (resourceEndpoint, accessKey) = ParseConnectionString(connectionString);
            requestUri = new Uri($"{resourceEndpoint}emails:send?api-version={ApiVersion}");
            this.accessKey = accessKey;
            httpClient = GetHttpClientFactory().CreateClient(nameof(Mailer));
        }

        public async Task<SendMailResponse> SendAsync(Campaign campaign, EmailRecipients recipients, string operationId, CancellationToken cancellationToken = default)
        {
            var body = new
            {
                content = campaign.EmailContent,
                senderAddress = campaign.SenderEmailAddress,
                replyTo = new List<Models.EmailAddress> { campaign.ReplyTo },
                recipients
            };

            var serializedBody = JsonConvert.SerializeObject(body);

            var requestMessage = new HttpRequestMessage(HttpMethod.Post, requestUri)
            {
                Content = new StringContent(serializedBody, Encoding.UTF8, "application/json")
            };

            // Specify the 'x-ms-date' header as the current UTC timestamp according to the RFC1123 standard
            var date = DateTimeOffset.UtcNow.ToString("r", CultureInfo.InvariantCulture);
            // Get the host name corresponding with the 'host' header.
            var host = requestUri.Authority;
            // Compute a content hash for the 'x-ms-content-sha256' header.
            var contentHash = ComputeContentHash(serializedBody);

            // Prepare a string to sign.
            var stringToSign = $"POST\n{requestUri.PathAndQuery}\n{date};{host};{contentHash}";
            // Compute the signature.
            var signature = ComputeSignature(stringToSign, accessKey);
            // Concatenate the string, which will be used in the authorization header.
            var authorizationHeader = $"HMAC-SHA256 SignedHeaders=x-ms-date;host;x-ms-content-sha256&Signature={signature}";

            // Add a date header.
            requestMessage.Headers.Add("x-ms-date", date);

            // Add a content hash header.
            requestMessage.Headers.Add("x-ms-content-sha256", contentHash);

            // Add an authorization header.
            requestMessage.Headers.Add("Authorization", authorizationHeader);

            // Add an operation id header.
            requestMessage.Headers.Add("Operation-Id", operationId);

            var response = await httpClient.SendAsync(requestMessage, cancellationToken);

            if (response.IsSuccessStatusCode)
            {
                return new SendMailResponse { IsSuccessCode = true };
            }
            else
            {
                var responseString = await response.Content.ReadAsStringAsync(cancellationToken);
                var error = JsonConvert.DeserializeObject<ErrorResponse>(responseString);
                return new SendMailResponse
                {
                    IsSuccessCode = false,
                    StatusCode = response.StatusCode,
                    Code = error.Error.Code,
                    Message = error.Error.Message
                };
            }
        }

        private static IHttpClientFactory GetHttpClientFactory()
        {
            var services = new ServiceCollection();
            services.AddHttpClient<Mailer>();
            var serviceProvider = services.BuildServiceProvider();
            return serviceProvider.GetService<IHttpClientFactory>();
        }

        private static string ComputeContentHash(string content)
        {
            byte[] hashedBytes = SHA256.HashData(Encoding.UTF8.GetBytes(content));
            return Convert.ToBase64String(hashedBytes);
        }

        private static string ComputeSignature(string stringToSign, string secret)
        {
            using var hmacsha256 = new HMACSHA256(Convert.FromBase64String(secret));
            var bytes = Encoding.UTF8.GetBytes(stringToSign);
            var hashedBytes = hmacsha256.ComputeHash(bytes);
            return Convert.ToBase64String(hashedBytes);
        }

        private static (string resourceEndpoint, string accessKey) ParseConnectionString(string connectionString)
        {
            Match match = Regex.Match(connectionString, ConnectionStringPattern);
            string resourceEndpoint = match.Groups[endpointGroupName].Value;
            string accessKey = match.Groups[accessKeyGroupName].Value;
            return (resourceEndpoint, accessKey);
        }
    }
}
