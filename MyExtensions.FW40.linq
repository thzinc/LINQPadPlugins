<Query Kind="Program">
  <NuGetReference>Newtonsoft.Json</NuGetReference>
  <NuGetReference>RestSharp</NuGetReference>
  <Namespace>Newtonsoft.Json</Namespace>
  <Namespace>RestSharp</Namespace>
  <Namespace>System.Net</Namespace>
  <Namespace>System.Xml.Serialization</Namespace>
</Query>

void Main()
{
    // Write code to test your extensions here. Press F5 to compile and run.
}

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

// You can also define non-static classes, enums, etc.

// Define other methods and classes here
public class Spreedly
{
    public string EnvironmentKey { get; set; }
    public string Secret { get; set; }
    
    private T Download<T>(string pattern, params object[] parameters)
    {
        using (var client = new WebClient())
        {
            client.Credentials = new NetworkCredential() {
                UserName = EnvironmentKey,
                Password = Secret
            };
        
            var xml = client.DownloadString(string.Concat("https://core.spreedly.com/v1/", string.Format(pattern, parameters)));
            
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
        client.Authenticator = new RestSharp.HttpBasicAuthenticator(EnvironmentKey, Secret);
        
        var request = new RestRequest(pattern, RestSharp.Method.POST);
        request.XmlSerializer = new RestSharp.Serializers.DotNetXmlSerializer();
        request.RequestFormat = DataFormat.Xml;
        request.AddBody(body);
        
        var response = client.Execute<T>(request);
        
        return response.Data;
    }
    
    public IEnumerable<TransactionResponse> PaymentMethodTransactions(string paymentMethodToken)
    {
        return Download<TransactionsResponse>("payment_methods/{0}/transactions.xml", paymentMethodToken).Transactions;
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