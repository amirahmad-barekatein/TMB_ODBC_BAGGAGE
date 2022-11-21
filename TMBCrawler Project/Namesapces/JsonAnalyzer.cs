namespace JsonAnalyzer 
{
   // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse);
    public class Airline
    {
        public string title { get; set; }
        public string code { get; set; }
    }

    public class Airport
    {
        public string code { get; set; }
        public string title { get; set; }
    }

    public class Baggage
    {
        public Total total { get; set; }
        public List<Item> items { get; set; }
    }

    public class Flight
    {
        public string id { get; set; }
        public string code { get; set; }
    }

    public class FlightBaggageModel
    {
        public Flight flight { get; set; }
        public Airline airline { get; set; }
        public string city { get; set; }
        public Airport airport { get; set; }
        public Baggage baggage { get; set; }
    }

    

    public class Item
    {
        public string barcode { get; set; }
        public string createdAt { get; set; }
        public string group { get; set; }
    }

    // public class Root
    // {
    //      public Dictionary<string, object> { get; set; }
    // }

    public class Total
    {
        public object weight { get; set; }
        public object quantity { get; set; }
    }
}