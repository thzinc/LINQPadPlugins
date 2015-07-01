<Query Kind="Program">
  <NuGetReference>CouchbaseNetClient</NuGetReference>
  <NuGetReference>Newtonsoft.Json</NuGetReference>
  <NuGetReference>RestSharp</NuGetReference>
  <Namespace>Couchbase</Namespace>
  <Namespace>Newtonsoft.Json</Namespace>
  <Namespace>RestSharp</Namespace>
  <Namespace>System.Net</Namespace>
  <Namespace>System.Xml.Serialization</Namespace>
</Query>

public static class MyExtensions
{
    // Write custom extension methods here. They will be available to all queries.
    public static T Median<T, U>(this IEnumerable<T> items, Func<T, U> selector, Func<T, T, T> combinator = null)
    {
        if (combinator == null)
        combinator = (a, b) => a;
        
        var orderedItems = items.OrderBy(selector).ToList();
        var skip = orderedItems.Count / 2;
        
        if (orderedItems.Count % 2 == 1)
        {
            return orderedItems.Skip(skip).First();
        }
        
        var middle = orderedItems.Skip(skip - 1).Take(2).ToList();
        return combinator(middle.First(), middle.Last());
    }
   
    public static bool AtLeast<T>(this IEnumerable<T> items, int count)
    {
        return items.Some(count, i => i == count);
    }
   
    public static bool FewerThan<T>(this IEnumerable<T> items, int count)
    {
        return items.Some(count, i => i < count);
    }
    
    public static bool Some<T>(this IEnumerable<T> items, int count, Func<int,bool> comparator)
    {
        var enumerator = items.GetEnumerator();
        int i;
        for (i = 0; i < count && enumerator.MoveNext(); i++)
        { }
        
        return comparator(i);
    }
}

public class Settings
{
    public static PrivateSettings Private
    {
        get { return _private.Value; }
    }
    
    private static Lazy<PrivateSettings> _private = new Lazy<PrivateSettings>(() => {
        var filename = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Personal), "LINQPad Plugins", "settings-private.json");
        
        if (!File.Exists(filename))
        {
            string.Format("Private settings file is missing: {0}", filename).Dump("Warning");
            return null;
        }
        
        var json = File.ReadAllText(filename);
        return JsonConvert.DeserializeObject<PrivateSettings>(json);
    });
    
    public class PrivateSettings
    {
        public CouchbaseSettings Couchbase { get; set; }
        public SpreedlySettings Spreedly { get; set; }
                
        public class CouchbaseSettings
        {
            public Dictionary<string, Environment> Environments { get; set; }
        
            public CouchbaseSettings()
            {
                Environments = new Dictionary<string, Environment>();
            }
        
            public class Environment
            {
                public string Host { get; set; }
                public string RestApiHost { get; set; }
                public string Username { get; set; }
                public string Password { get; set; }
            }
        }
        
        public class SpreedlySettings
        {
            public string Secret { get; set; }
            public IDictionary<string, string> Environments { get; set;}
            
            public SpreedlySettings()
            {
                Environments = new Dictionary<string, string>();
            }
        }
    }
}

public class Spreedly
{
    public Spreedly()
    {
        DumpRawResponse = false;
    }
    
    public Spreedly(string environmentName)
        : this()
    {
        string environmentKey;
        if (!Settings.Private.Spreedly.Environments.TryGetValue(environmentName, out environmentKey))
            throw new ArgumentOutOfRangeException("environmentName", string.Format("Could not find the environment key for {0}", environmentName));
        
        EnvironmentKey = environmentKey;
    }

    public string EnvironmentKey { get; set; }
    public string Secret { get; set; }
    public bool DumpRawResponse { get; set; }
    
    private string GetSecret()
    {
        if (Secret != null)
            return Secret;
        
        return Settings.Private != null ? Settings.Private.Spreedly.Secret : null;
    }
    
    private T Download<T>(string pattern, params object[] parameters)
    {
        using (var client = new WebClient())
        {
            client.Credentials = new NetworkCredential() {
                UserName = EnvironmentKey,
                Password = GetSecret()
            };
        
            var xml = client.DownloadString(string.Concat("https://core.spreedly.com/v1/", string.Format(pattern, parameters)));
            
            if (DumpRawResponse) xml.Dump("Raw Response");
            
            XmlSerializer serializer = new XmlSerializer(typeof(T));
            using (StringReader reader = new StringReader(xml))
            {
                return (T)serializer.Deserialize(reader);
            }
        }
    }
    
    private T Post<T>(string pattern, object body)
        where T : new()
    {
        var client = new RestSharp.RestClient("https://core.spreedly.com/v1/");
        client.Authenticator = new RestSharp.HttpBasicAuthenticator(EnvironmentKey, GetSecret());
        
        var request = new RestRequest(pattern, RestSharp.Method.POST);
        request.XmlSerializer = new RestSharp.Serializers.DotNetXmlSerializer();
        request.RequestFormat = DataFormat.Xml;
        request.AddBody(body);
        
        var response = client.Execute<T>(request);
        
        if (DumpRawResponse) response.Content.Dump("Raw Response");
        
        return response.Data;
    }
    
    public IEnumerable<TransactionResponse> PaymentMethodTransactions(string paymentMethodToken)
    {
        return Download<TransactionsResponse>("payment_methods/{0}/transactions.xml", paymentMethodToken).Transactions;
    }
    
    public TransactionResponse Transaction(string token)
    {
        return Download<TransactionResponse>("transactions/{0}.xml", token);
    }
    
    public IEnumerable<TransactionResponse> GetReferencedTransactions(TransactionResponse transaction)
    {
        return transaction.ApiUrls
            .Where (au => !string.IsNullOrEmpty(au.ReferencingTransactionUrl))
            .Select(au => Download<TransactionResponse>(au.ReferencingTransactionUrl.Replace("https://core.spreedly.com/v1/", "")));
    }
    
    public PaymentMethodResponse PaymentMethod(string paymentMethodToken)
    {
        return Download<PaymentMethodResponse>("payment_methods/{0}.xml", paymentMethodToken);
    }
    
    public IEnumerable<PaymentMethodResponse> PaymentMethods()
    {
        return Download<PaymentMethodsResponse>("payment_methods.xml?order=desc").PaymentMethods;
    }
    
    public TransactionResponse CreatePaymentMethod(
        string number,
        byte expirationMonth,
        short expirationYear,
        string firstName,
        string lastName,
        short? verificationValue = null,
        string email = null,
        string address1 = null,
        string address2 = null,
        string city = null,
        string state = null,
        string zip = null,
        string country = null
    )
    {
        var request = new XElement("payment_method",
            new XElement("credit_card",
                new XElement("first_name", firstName),
                new XElement("last_name", lastName),
                new XElement("number", number),
                new XElement("verification_value", verificationValue),
                new XElement("month", expirationMonth.ToString("00")),
                new XElement("year", expirationYear.ToString("0000")),
                new XElement("email", email),
                new XElement("address1", address1),
                new XElement("address2", address2),
                new XElement("city", city),
                new XElement("state", state),
                new XElement("zip", zip),
                new XElement("country", country)
            )
        );
        
        return Post<TransactionResponse>("payment_methods.xml", request);
    }
    
    public IEnumerable<GatewayResponse> Gateways()
    {
        return Download<GatewaysResponse>("gateways.xml").Gateways;
    }
    
    public Receiver CreateReceiver(CreateReceiverRequest request)
    {
        return Post<Receiver>("receivers.xml", request);
    }
    
    public IEnumerable<Receiver> Receivers()
    {
        return Download<ReceiversResponse>("receivers.xml").Receivers;
    }
    
    public DeliverTransactionResponse Deliver(string receiverToken, string paymentMethodToken, string url, string headers, string body)
    {
        var request = new XElement("delivery",
            new XElement("payment_method_token", paymentMethodToken),
            new XElement("url", url),
            new XElement("headers", new XCData(headers)),
            new XElement("body", new XCData(body))
        );
        
        return Post<DeliverTransactionResponse>(string.Format("receivers/{0}/deliver.xml", receiverToken), request);
    }
    
    public string MigratePaymentMethodToken(string receiverToken, string paymentMethodToken, string targetEnvironmentKey)
    {
        var url = string.Format("https://core.spreedly.com/v1/payment_methods.json?environment_key={0}", targetEnvironmentKey);
        var headers = "Content-Type: application/json";
        var body = @"{
            ""payment_method"": {
                ""credit_card"": {
                    ""first_name"": ""{{credit_card_first_name}}"",
                    ""last_name"": ""{{credit_card_last_name}}"",
                    ""number"": ""{{credit_card_number}}"",
                    ""verification_value"": ""{{credit_card_verification_value}}"",
                    ""month"": ""{{credit_card_month}}"",
                    ""year"": ""{{credit_card_year}}"",
                    ""email"": ""{{credit_card_email}}"",
                    ""address1"": ""{{credit_card_address1}}"",
                    ""address2"": ""{{credit_card_address2}}"",
                    ""city"": ""{{credit_card_city}}"",
                    ""state"": ""{{credit_card_state}}"",
                    ""zip"": ""{{credit_card_zip}}"",
                    ""country"": ""{{credit_card_country}}""
                }
            }
        }";
        
        var response = Deliver(receiverToken, paymentMethodToken, url, headers, body);
        
        if ((int)response.Response.Status / 100 == 2)
        {
            var transaction = JsonConvert.DeserializeObject<JsonTransactionResponse>(response.Response.Body);
            
            return transaction.Transaction.PaymentMethod.Token;
        }
    
        throw new Exception(string.Format("Could not successfully migrate payment method token {0} to environment {1}. Details:\n{2}", paymentMethodToken, targetEnvironmentKey, response.Response.Body));
    }
    
    [XmlRoot("receiver")]
    public class CreateReceiverRequest
    {
        [XmlElement("receiver_type")]
        public string ReceiverType { get; set; }

        [XmlElement("credentials")]
        public List<Credential> Credentials { get; set; }
        
        public CreateReceiverRequest()
        {
            Credentials = new List<Credential>();
        }
        
        public class Credential
        {
            [XmlElement("name")]
            public string Name { get; set; }
            
            [XmlElement("value")]
            public string Value { get; set; }
            
            [XmlElement("safe")]
            public bool Safe { get; set; }
        }
    }
    
    [XmlRoot("receivers")]
    public class ReceiversResponse
    {
        [XmlElement("receiver")]
        public List<Receiver> Receivers { get; set; }
        
        public ReceiversResponse()
        {
            Receivers = new List<Receiver>();
        }
    }
    
    [XmlRoot("receiver")]
    public class Receiver
    {
        [XmlElement("receiver_type")]
        public string ReceiverType { get; set; }
        
        [XmlElement("token")]
        public string Token { get; set; }
        
        [XmlElement("hostnames")]
        public string Hostnames { get; set; }
        
        [XmlElement("state")]
        public string State { get; set; }
        
        [XmlElement("created_at")]
        public DateTime CreatedAt { get; set; }

        [XmlElement("updated_at")]
        public DateTime UpdatedAt { get; set; }
        
        [XmlElement("credentials")]
        public List<Credential> Credentials { get; set; }
        
        public Receiver()
        {
            Credentials = new List<Credential>();
        }

        public class Credential
        {
            [XmlElement("name")]
            public string Name { get; set; }
            
            [XmlElement("value")]
            public string Value { get; set; }
        }
    }
    
    [XmlRoot("transaction")]
    public class DeliverTransactionResponse
    {
        [XmlElement("created_at")]
        public DateTime CreatedAt { get; set; }

        [XmlElement("updated_at")]
        public DateTime UpdatedAt { get; set; }

        [XmlElement("succeeded")]
        public bool Succeeded { get; set; }

        [XmlElement("transaction_type")]
        public string TransactionType { get; set; }

        [XmlElement("token")]
        public string Token { get; set; }

        [XmlElement("state")]
        public string State { get; set; }

        [XmlElement("message")]
        public string Message { get; set; }

        [XmlElement("receiver")]
        public Receiver Receiver { get; set; }
        
        [XmlElement("payment_method")]
        public PaymentMethodResponse PaymentMethod { get; set; }

        [XmlElement("response")]
        public DeliverResponse Response { get; set; }

        public class DeliverResponse
        {
            [XmlElement("status")]
            public System.Net.HttpStatusCode Status { get; set; }
            
            [XmlElement("headers")]
            public string Headers { get; set; }
            
            public ILookup<string, string> HeadersLookup
            {
                get
                {
                    return (Headers ?? "")
                        .Split(new[] { '\n' }, StringSplitOptions.RemoveEmptyEntries)
                        .Select(s => s.Split(new[] { ':' }, 2))
                        .ToLookup(a => a[0], a => a[1].TrimStart());
                }
            }
            
            [XmlElement("body")]
            public string Body { get; set; }
        }
    }
    
    [XmlRoot("payment_methods")]
    public class PaymentMethodsResponse
    {
        [XmlElement("payment_method")]
        public List<PaymentMethodResponse> PaymentMethods { get; set; }
        
        public PaymentMethodsResponse()
        {
            PaymentMethods = new List<PaymentMethodResponse>();
        }
    }
    
    [XmlRoot("transactions")]
    public class TransactionsResponse
    {
        [XmlElement("transaction")]
        public List<TransactionResponse> Transactions{ get; set; }
    }
    
    public class JsonTransactionResponse
    {
        [JsonProperty("transaction")]
        public TransactionResponse Transaction { get; set; }
    }
    
    [XmlRoot("transaction")]
    public class TransactionResponse
    {
        [XmlElement("created_at")]
        [JsonProperty("created_at")]
        public DateTime CreatedAt { get; set; }

        [XmlElement("updated_at")]
        [JsonProperty("updated_at")]
        public DateTime UpdatedAt { get; set; }

        [XmlElement("succeeded")]
        [JsonProperty("succeeded")]
        public bool Succeeded { get; set; }

        [XmlElement("transaction_type")]
        [JsonProperty("transaction_type")]
        public string TransactionType { get; set; }

        [XmlElement("token")]
        [JsonProperty("token")]
        public string Token { get; set; }

        [XmlElement("gateway_token")]
        [JsonProperty("gateway_token")]
        public string GatewayToken { get; set; }

        [XmlElement("order_id")]
        [JsonProperty("order_id")]
        public string OrderId { get; set; }

        [XmlElement("amount")]
        [JsonProperty("amount")]
        public int Amount { get; set; }

        [XmlElement("response")]
        [JsonProperty("response")]
        public ExternalGatewayResponse GatewayResponse { get; set; }

        [XmlElement("payment_method")]
        [JsonProperty("payment_method")]
        public PaymentMethodResponse PaymentMethod { get; set; }

        [XmlElement("state")]
        [JsonProperty("state")]
        public string State { get; set; }

        [XmlElement("message")]
        [JsonProperty("message")]
        public string Message { get; set; }

        [XmlElement("gateway_transaction_id")]
        [JsonProperty("gateway_transaction_id")]
        public string GatewayTransactionId { get; set; }

        [XmlElement("gateway_specific_response_fields")]
        [JsonProperty("gateway_specific_response_fields")]
        public GatewaySpecificResponseFieldsResponse GatewaySpecificResponseFields { get; set; }
        
        [XmlElement("api_urls")]
        [JsonProperty("api_urls")]
        public List<ApiUrlsResponse> ApiUrls { get; set; }
        
        public class ApiUrlsResponse
        {
            [XmlElement("referencing_transaction")]
            [JsonProperty("referencing_transaction")]
            public string ReferencingTransactionUrl { get; set; }
        }

        public class ExternalGatewayResponse
        {
            [XmlElement("success")]
            [JsonProperty("success")]
            public bool Success { get; set; }

            [XmlElement("message")]
            [JsonProperty("message")]
            public string Message { get; set; }

            [XmlElement("error_code")]
            [JsonProperty("error_code")]
            public string ErrorCode { get; set; }
        }

        public class GatewaySpecificResponseFieldsResponse
        {
            [XmlElement("authorize_net")]
            [JsonProperty("authorize_net")]
            public AuthorizeNetResponse AuthorizeNet { get; set; }

            public class AuthorizeNetResponse 
            {
                [XmlElement("response_reason_code")]
                [JsonProperty("response_reason_code")]
                public string ResponseReasonCode { get; set; }
            }
        }
        
        public TransactionResponse()
        {
            ApiUrls = new List<ApiUrlsResponse>();
        }
    }
    
    [XmlRoot("payment_method")]
    public class PaymentMethodResponse
    {
        [XmlElement("token")]
        [JsonProperty("token")]
        public string Token { get; set; }

        [XmlElement("number")]
        [JsonProperty("number")]
        public string Number { get; set; }

        [XmlElement("last_four_digits")]
        [JsonProperty("last_four_digits")]
        public string LastFourDigits { get; set; }
        
        [XmlElement("first_six_digits")]
        [JsonProperty("first_six_digits")]
        public string FirstSixDigits { get; set; }

        [XmlElement("month")]
        [JsonProperty("month")]
        public int Month { get; set; }

        [XmlElement("year")]
        [JsonProperty("year")]
        public int Year { get; set; }

        [XmlElement("first_name")]
        [JsonProperty("first_name")]
        public string FirstName { get; set; }

        [XmlElement("last_name")]
        [JsonProperty("last_name")]
        public string LastName { get; set; }

        [XmlElement("storage_state")]
        [JsonProperty("storage_state")]
        public string StorageState { get; set; }
        
        [XmlElement("address1")]
        [JsonProperty("address1")]
        public string Address1 { get; set; }

        [XmlElement("address2")]
        [JsonProperty("address2")]
        public string Address2 { get; set; }

        [XmlElement("city")]
        [JsonProperty("city")]
        public string City { get; set; }

        [XmlElement("state")]
        [JsonProperty("state")]
        public string State { get; set; }

        [XmlElement("zip")]
        [JsonProperty("zip")]
        public string Zip { get; set; }

        [XmlElement("country")]
        [JsonProperty("country")]
        public string Country{ get; set; }

        public bool Retained { get { return String.Equals("retained", StorageState); } }
    }
    
    public abstract class BaseGatewayResponse
    {
        [XmlElement("name")]
        public string Name { get; set; }

        [XmlElement("gateway_type")]
        public string GatewayType { get; set; }
   
        [XmlElement("payment_methods")]
        public GatewayAbstractResponse.PaymentMethodsResponse[] PaymentMethods { get; set; }

        [XmlElement("characteristics")]
        public GatewayAbstractResponse.CharacteristicsResponse Characteristics { get; set; }
    }

    /// <summary>
    /// Represents a specific gateway implementation.
    /// </summary>
    [Serializable]
    [XmlRoot("gateway")]
    public class GatewayResponse : BaseGatewayResponse
    {
        [XmlElement("token")]
        public string Token { get; set; }

        [XmlElement("credentials")]
        public CredentialsResponse Credentials { get; set; }

        [XmlElement("redacted")]
        public bool Redacted { get; set; }

        [XmlElement("mode")]
        public string Mode { get; set; }

        [XmlElement("state")]
        public string State { get; set; }

        [XmlElement("created_at")]
        public DateTime CreatedAt { get; set; }

        [XmlElement("updated_at")]
        public DateTime UpdatedAt { get; set; }

        [Serializable]
        [XmlRoot("credentials")]
        public class CredentialsResponse
        {
            [XmlElement("credential")]
            public CredentialResponse[] Credentials { get; set; }

            [XmlRoot("credential")]
            public class CredentialResponse
            {
                [XmlElement("name")]
                public string Name { get; set; }

                [XmlElement("value")]
                public string Value { get; set; }
            }
        }
    }

    /// <summary>
    /// Represents a description of an available gateway.
    /// </summary>
    [Serializable]
    [XmlRoot("gateway")]
    public class GatewayAbstractResponse : BaseGatewayResponse
    {
        [XmlElement("token")]
        public string Token { get; set; }

        [XmlElement("auth_modes")]
        public AuthModesResponse AuthModes { get; set; }

        [XmlElement("supported_countries")]
        public string SupportedCountries { get; set; }

        [XmlElement("regions")]
        public string Regions { get; set; }

        [XmlElement("homepage")]
        public string Homepage { get; set; }

        [XmlElement("company_name")]
        public string CompanyName { get; set; }

        #region Sub Classes

        [Serializable]
        [XmlRoot("payment_methods")]
        public class PaymentMethodsResponse
        {
            [XmlElement("payment_method")]
            public string PaymentMethod { get; set; }
        }

        [Serializable]
        [XmlRoot("characteristics")]
        public class CharacteristicsResponse
        {
            [XmlElement("supports_purchase")]
            public bool SupportsPurchase { get; set; }

            [XmlElement("supports_authorize")]
            public bool SupportsAuthorize { get; set; }

            [XmlElement("supports_capture")]
            public bool SupportsCapture { get; set; }

            [XmlElement("supports_credit")]
            public bool SupportsCredit { get; set; }

            [XmlElement("supports_general_credit")]
            public bool SupportsGeneralCredit { get; set; }

            [XmlElement("supports_void")]
            public bool SupportsVoid { get; set; }

            [XmlElement("supports_verify")]
            public bool SupportsVerify { get; set; }

            [XmlElement("supports_reference_purchase")]
            public bool SupportsReferencePurchase { get; set; }

            [XmlElement("supports_purchase_via_preauthorization")]
            public bool SupportPurchaseViaPreAuthorization { get; set; }

            [XmlElement("supports_offsite_purchase")]
            public bool SupportOffsitePurchase { get; set; }

            [XmlElement("supports_offsite_authorize")]
            public bool SupportOffsiteAuthorize { get; set; }

            [XmlElement("supports_3dsecure_purchase")]
            public bool Supports3dSecurePurchase { get; set; }

            [XmlElement("supports_3dsecure_authorize")]
            public bool Supports3dSecureAuthorize { get; set; }

            [XmlElement("supports_store")]
            public bool SupportsStore { get; set; }

            [XmlElement("supports_remove")]
            public bool SupportsRemove { get; set; }
        }

        [Serializable]
        [XmlRoot("auth_modes")]
        public class AuthModesResponse
        {
            [XmlElement("auth_mode")]
            public AuthModeResponse[] AuthModes { get; set; }

            [XmlRoot("auth_mode")]
            public class AuthModeResponse
            {
                [XmlElement("auth_mode_type")]
                public string AuthModeType { get; set; }

                [XmlElement("name")]
                public string Name { get; set; }

                [XmlElement("credentials")]
                public CredentialsAbstractResponse Credentials { get; set; }
            }
        }

        #endregion

        [Serializable]
        [XmlRoot("credentials")]
        public class CredentialsAbstractResponse
        {
            [XmlElement("credential")]
            public CredentialAbstractResponse[] Credentials { get; set; }

            [XmlRoot("credential")]
            public class CredentialAbstractResponse
            {
                [XmlElement("name")]
                public string Name { get; set; }

                [XmlElement("label")]
                public string Label { get; set; }

                [XmlElement("safe")]
                public bool Safe { get; set; }

                [XmlElement("large")]
                public bool Large { get; set; }
            }
        }
    }

    [Serializable]
    [XmlRoot("gateways")]
    public class GatewaysResponse
    {
        [XmlElement("gateway")]
        public GatewayResponse[] Gateways { get; set; }
    }

    [Serializable]
    [XmlRoot("gateways")]
    public class GatewayAbstractsResponse
    {
        [XmlElement("gateway")]
        public GatewayAbstractResponse[] Gateways { get; set; }
    }
}

public class CouchbaseHelper
{
    public string Host { get; set; }
    public string RestApiHost { get; set; }
    public string Username { get; set; }
    public string Password { get; set; }
    
    public CouchbaseHelper()
    { }
    
    public CouchbaseHelper(string environmentKey)
        : this()
    {
        var env = Settings.Private.Couchbase.Environments[environmentKey];
        Host = env.Host;
        RestApiHost = env.RestApiHost;
        Username = env.Username;
        Password = env.Password;
    }
    
    private string GetEndKey(string startKey)
    {
        return startKey.Length == 0 ? "" : startKey.Substring(0, startKey.Length - 1) + ((char)(startKey[startKey.Length - 1] + 1)).ToString();
    }
    
    private RestClient GetClient()
    {
        var client = new RestClient(RestApiHost);
        client.Authenticator = new HttpBasicAuthenticator(Username, Password);
        
        return client;
    }
    
    public IEnumerable<string> DeleteDocuments(string bucketName, Func<string, bool> predicate)
    {
        var cluster = new Cluster();
        cluster.Configuration.Servers = new List<Uri>() { new Uri(new Uri(Host), new Uri("/pools", UriKind.Relative)) };
        
        using (var bucket = cluster.OpenBucket(bucketName, ""))
        {
            var client = GetClient();
            
            var designDocumentName = "dev_temp_" + Guid.NewGuid().ToString();
            var createTempDesignDoc = new RestRequest("{bucket}/_design/{designDocumentName}", Method.PUT);
            createTempDesignDoc.AddUrlSegment("bucket", bucketName);
            createTempDesignDoc.AddUrlSegment("designDocumentName", designDocumentName);
            createTempDesignDoc.AddJsonBody(new
            {
                views = new
                {
                    allDocuments = new
                    {
                        map = @"function (doc, meta) { emit(meta.id, null); }"
                    }
                }
            });
            client.Execute(createTempDesignDoc);
            
            var query = bucket.CreateQuery(designDocumentName, "allDocuments");
            query.Limit(int.MaxValue);
            
            var keys = bucket.Query<dynamic>(query).Rows
                .Select(r => r.Id)
                .Where(predicate)
                .ToList();
                
            var deleteTempDesignDoc = new RestRequest("{bucket}/_design/{designDocumentName}", Method.DELETE);
            deleteTempDesignDoc.AddUrlSegment("bucket", bucketName);
            deleteTempDesignDoc.AddUrlSegment("designDocumentName", designDocumentName);
            client.Execute(deleteTempDesignDoc);
                
            bucket.Remove(keys);
            
            return keys;
        }
    }
    
    public IEnumerable<string> DeleteDocuments(string bucketName, IList<string> keys)
    {
        var cluster = new Cluster();
        cluster.Configuration.Servers = new List<Uri>() { new Uri(new Uri(Host), new Uri("/pools", UriKind.Relative)) };
        
        using (var bucket = cluster.OpenBucket(bucketName, ""))
        {
            var client = GetClient();
            
            bucket.Remove(keys);
            
            return keys;
        }
    }
    
    public class Results
    {
        public class Docs
        {
            public List<DocumentRef> Rows { get; set; }
            
            public Docs()
            {
                Rows = new List<DocumentRef>();
            }
        }
        
        public class DocumentRef
        {
            public string Id { get; set; }
            public string Key { get; set; }
        }
    }
}

public static class SpWho2Extensions
{
    public class ProcessListItem
    {
        public int ProcessId { get; set; }
        public string Status { get; set; }
        public string Login { get; set; }
        public string HostName { get; set; }
        public IEnumerable<ProcessListItem> BlockedByProcesses { get; private set; }
        public string DatabaseName { get; set; }
        public string Command { get; set; }
        public int? CpuTime { get; set; }
        public int? DiskIO { get; set; }
        public string ProgramName { get; set; }
        
        public ProcessListItem(DataRow r, ReturnDataSet dataSet)
        {
            ProcessId = r.ParseField<int>("SPID", int.TryParse).GetValueOrDefault();
            Status = r.TrimField("Status");
            Login = r.TrimField("Login");
            HostName = r.TrimField("HostName").NullIf(s => StringComparer.Ordinal.Equals(s, "."));
            int dummy;
            if (int.TryParse(r.TrimField("BlkBy").NullIf(s => StringComparer.Ordinal.Equals(s, ".")), out dummy))
                BlockedByProcesses = dataSet
                    .AsProcessList()
                    .Where(x => x.ProcessId == dummy);
            else
                BlockedByProcesses = new List<ProcessListItem>();
            
            DatabaseName = r.TrimField("DBName");
            Command = r.TrimField("Command");
            CpuTime = r.ParseField<int>("CPUTime", int.TryParse);
            DiskIO = r.ParseField<int>("DiskIO", int.TryParse);
            ProgramName = r.TrimField("ProgramName");
        }
    }
    
    public static IEnumerable<ProcessListItem> AsProcessList(this ReturnDataSet dataSet)
    {
        return dataSet.Tables[0]
            .AsEnumerable()
            .Select(x => new ProcessListItem(x, dataSet));
    }

	private static string TrimField(this DataRow row, string columnName)
	{
		var value = row.Field<string>(columnName);
		if (value != null) return value.Trim();
		return null;
	}
	
	private static T NullIf<T>(this T obj, Func<T, bool> predicate)
	{
		return predicate(obj) ? default(T) : obj;
	}
	
	private delegate bool TryParse<T>(string input, out T value);
	
	private static T? ParseField<T>(this DataRow row, string columnName, TryParse<T> tryParse)
		where T: struct
	{
		var input = row.TrimField(columnName);
		T value;
		return tryParse(input, out value) ? (T?)value : null;
	}
}