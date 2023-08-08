using System.Net;
using System.Text;
using System.Text.Json;
using JsonAnalyzer;
using ObjectStructures;
using Microsoft.EntityFrameworkCore;
using System.Data.Odbc;
using System.Data;


namespace TMBProject
{
    public class BaggageContext : DbContext
    {
        public DbSet<BaggageDataModel> Baggages { get; set; }
        public string DbPath { get; }

        public BaggageContext()
        {
            var folder = Environment.SpecialFolder.LocalApplicationData;
            var path = Environment.GetFolderPath(folder);
            DbPath = System.IO.Path.Join(path, "baggage.db");
        }

        // The following configures EF to create a Sqlite database file in the
        // special "local" folder for your platform.
        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseSqlite($"Data Source={DbPath}");
    }

    [System.AttributeUsage(System.AttributeTargets.Property ,AllowMultiple = true)]
    public class UseInReport : System.Attribute
    {
        public bool Use;
        public UseInReport()
        {
            Use = false;
        }
    }
    public class BaggageDataModel 
    {
        public int BaggageDataModelId {get; set;}
        [UseInReport(Use = true)]
        public string? FlightName { get; set;}
        [UseInReport(Use = true)]
        public string? FilghtId { get; set; }
        [UseInReport(Use = true)]
        public string? FlightCode { get; set; }
        [UseInReport(Use = true)]
        public string? AirlineTitle { get; set; }
        [UseInReport(Use = true)]
        public string? AirlineCode { get; set; }
        [UseInReport(Use = true)]
        public string? CityName {get; set;}
        [UseInReport(Use = true)]
        public string? AirportTitle { get; set; }
        [UseInReport(Use = true)]
        public string? AirportCode { get; set; }
        [UseInReport(Use = true)]
        public string? BaggageGroup { get; set; }
        [UseInReport(Use = true)]
        public string? BaggageBarcode { get; set; }
        [UseInReport(Use = true)]
        public DateTime? BaggageCreatedAt { get; set; }

        public static string DBTableName = "BaggageContext";
        public static string DBDateColName = "BaggageCreatedAt";
        public string ToCsv()
        {
            return GetType().GetProperties()
            .Where( p => p.IsDefined(typeof(UseInReport), true))
            .Select(info => (info.Name, Value: info.GetValue(this, null) ?? "null"))
            .Aggregate(
                new StringBuilder(),
                (sb, pair) => sb.Append("\"" + pair.Value.ToString() + "\"" + ","),
                sb => sb.ToString());
        }
        
        public bool InsertToDB(OdbcConnection connection, OdbcTransaction transaction, OdbcCommand command, string TableName = "BaggageContext")
        {
            try{
                command.CommandText = getInsertQuery(); 
                Console.WriteLine(command.CommandText);
                command.Connection = connection;    
                command.Transaction = transaction;
                command.ExecuteNonQuery();    
                transaction.Commit();
                transaction.Dispose();
                Console.WriteLine("Record Submitted","Congrats");    
               }
               catch (OdbcException exp)
               {
                    Console.WriteLine("Database Error:" + exp.Message.ToString());
                    transaction.Commit();
                    transaction.Dispose();
                    return false;
               }
               return true;
        }
        
        public string getInsertValues()
        {
            var insertValuesQuery ="(";

            var values =  GetType().GetProperties()
                        .Where( p => p.IsDefined(typeof(UseInReport), true))
                        .Select(info => (info.Name, Value: info.GetValue(this, null) ?? ""))
                        .Aggregate(
                            new StringBuilder(),
                            (sb, pair) => sb.Append( "\"" + pair.Value.ToString() + "\"" + ","),
                            sb => sb.ToString());
            values = values.Remove(values.Length - 1, 1); 
            insertValuesQuery = insertValuesQuery 
                        + values
                        +")";
            Console.WriteLine(insertValuesQuery);
            return insertValuesQuery;
        }
        public string getInsertProperties()
        {
            var insertPropertiesQuery = "INSERT INTO " 
                            + DBTableName
                            + " (";

            string properties = GetType().GetProperties()
                            .Where( p => p.IsDefined(typeof(UseInReport), true))
                            .Select(info => (info.Name, Value: info.GetValue(this, null) ?? ""))
                            .Aggregate(
                                new StringBuilder(),
                                (sb, pair) => sb.Append(pair.Name.ToString() + ","),
                                sb => sb.ToString());
            properties = properties.Remove(properties.Length - 1, 1); 

            insertPropertiesQuery = insertPropertiesQuery 
                        + properties
                        +")";
            return insertPropertiesQuery;
        }

       
        public string getInsertQuery()
        {
            var insertQuery = "INSERT INTO " 
                            + DBTableName
                            + " (";

            string properties = GetType().GetProperties()
                            .Where( p => p.IsDefined(typeof(UseInReport), true))
                            .Select(info => (info.Name, Value: info.GetValue(this, null) ?? ""))
                            .Aggregate(
                                new StringBuilder(),
                                (sb, pair) => sb.Append(pair.Name.ToString() + ","),
                                sb => sb.ToString());
            properties = properties.Remove(properties.Length - 1, 1); 

            insertQuery = insertQuery 
                        + properties
                        +")";
            insertQuery = insertQuery 
                        + " VALUES("; 
            var values =  GetType().GetProperties()
                        .Where( p => p.IsDefined(typeof(UseInReport), true))
                        .Select(info => (info.Name, Value: info.GetValue(this, null) ?? ""))
                        .Aggregate(
                            new StringBuilder(),
                            (sb, pair) => sb.Append( "\'" + pair.Value.ToString() + "\'" + ","),
                            sb => sb.ToString());
            values = values.Remove(values.Length - 1, 1); 
            insertQuery = insertQuery 
                        + values
                        +")";
            Console.WriteLine(insertQuery);
            return insertQuery;
        }
        public string getReportColumnName()
        {
            return GetType().GetProperties()
            .Where( p => p.IsDefined(typeof(UseInReport), true))
            .Select(info => (info.Name, Value: info.GetValue(this, null) ?? "null"))
            .Aggregate(
                new StringBuilder(),
                (sb, pair) => sb.Append(pair.Name + ","),
                sb => sb.ToString());
        }
    }

    public class FlightDataModel 
    {
        public int FlightDataModelId {get; set;}
        [UseInReport(Use = true)]
        public string? FlightName { get; set;}
        [UseInReport(Use = true)]
        public string? FlightId { get; set;}
        [UseInReport(Use = true)]
        public string? FlightCode { get; set;}
        [UseInReport(Use = true)]
        public string? AirlineTitle { get; set;}
        [UseInReport(Use = true)]
        public string? AirlineCode { get; set;}
        [UseInReport(Use = true)]
        public string? City { get; set;}
        [UseInReport(Use = true)]
        public string? AirportCode { get; set;}
        [UseInReport(Use = true)]
        public string? AirportTitle { get; set;}
        [UseInReport(Use = true)]
        public string? BaggageTotalWight { get; set;}
        [UseInReport(Use = true)]
        public string? BaggageQuantity { get; set;}
        public static string DBTableName = "FlightContext";
        public bool InsertToDB(OdbcConnection connection, OdbcTransaction transaction, OdbcCommand command, string TableName = "BaggageContext")
        {
            try{
                command.CommandText = getInsertQuery(); 
                Console.WriteLine(command.CommandText);
                command.Connection = connection;    
                command.Transaction = transaction;
                command.ExecuteNonQuery();    
                transaction.Commit();
                transaction.Dispose();
                Console.WriteLine("Record Submitted","Congrats");    
               }
               catch (OdbcException exp)
               {
                    Console.WriteLine("Database Error:" + exp.Message.ToString());
                    transaction.Commit();
                    transaction.Dispose();
                    return false;
               }
               return true;
        }
        public string getInsertValues()
        {
            var insertValuesQuery ="(";

            var values =  GetType().GetProperties()
                        .Where( p => p.IsDefined(typeof(UseInReport), true))
                        .Select(info => (info.Name, Value: info.GetValue(this, null) ?? ""))
                        .Aggregate(
                            new StringBuilder(),
                            (sb, pair) => sb.Append( "\"" + pair.Value.ToString() + "\"" + ","),
                            sb => sb.ToString());
            values = values.Remove(values.Length - 1, 1); 
            insertValuesQuery = insertValuesQuery 
                        + values
                        +")";
            Console.WriteLine(insertValuesQuery);
            return insertValuesQuery;
        }
        public string getInsertProperties()
        {
            var insertPropertiesQuery = "INSERT INTO " 
                            + DBTableName
                            + " (";

            string properties = GetType().GetProperties()
                            .Where( p => p.IsDefined(typeof(UseInReport), true))
                            .Select(info => (info.Name, Value: info.GetValue(this, null) ?? ""))
                            .Aggregate(
                                new StringBuilder(),
                                (sb, pair) => sb.Append(pair.Name.ToString() + ","),
                                sb => sb.ToString());
            properties = properties.Remove(properties.Length - 1, 1); 

            insertPropertiesQuery = insertPropertiesQuery 
                        + properties
                        +")";
            return insertPropertiesQuery;
        }
        public string getInsertQuery()
        {
            var insertQuery = "INSERT INTO " 
                            + DBTableName
                            + " (";

            string properties = GetType().GetProperties()
                            .Where( p => p.IsDefined(typeof(UseInReport), true))
                            .Select(info => (info.Name, Value: info.GetValue(this, null) ?? ""))
                            .Aggregate(
                                new StringBuilder(),
                                (sb, pair) => sb.Append(pair.Name.ToString() + ","),
                                sb => sb.ToString());
            properties = properties.Remove(properties.Length - 1, 1); 

            insertQuery = insertQuery 
                        + properties
                        +")";
            insertQuery = insertQuery 
                        + " VALUES("; 
            var values =  GetType().GetProperties()
                        .Where( p => p.IsDefined(typeof(UseInReport), true))
                        .Select(info => (info.Name, Value: info.GetValue(this, null) ?? ""))
                        .Aggregate(
                            new StringBuilder(),
                            (sb, pair) => sb.Append( "\'" + pair.Value.ToString() + "\'" + ","),
                            sb => sb.ToString());
            values = values.Remove(values.Length - 1, 1); 
            insertQuery = insertQuery 
                        + values
                        +")";
            Console.WriteLine(insertQuery);
            return insertQuery;
        }
        public string getReportColumnName()
        {
            return GetType().GetProperties()
            .Where( p => p.IsDefined(typeof(UseInReport), true))
            .Select(info => (info.Name, Value: info.GetValue(this, null) ?? "null"))
            .Aggregate(
                new StringBuilder(),
                (sb, pair) => sb.Append(pair.Name + ","),
                sb => sb.ToString());
        }
    }
    class Program
    {
        public static string getDeleteOldRecords(string DBTableName, string DBDateColName)
        {
            DateTime lastWeek = DateTime.Now.AddDays(-7);
            string delelteQuery = "DELETE FROM "
                                + DBTableName 
                                + " WHERE "
                                + DBDateColName
                                + " < "
                                + "\'" + lastWeek.ToString("M/d/yyyy HH:mm:ss tt", System.Globalization.CultureInfo.CurrentCulture) + "\'";

            return delelteQuery;
        }
         public static bool DeleteOldRecordsFromDb(OdbcConnection connection, OdbcTransaction transaction, OdbcCommand command, string TableName = "BaggageContext")
        {
            try{
                command.CommandText = getDeleteOldRecords(BaggageDataModel.DBTableName, BaggageDataModel.DBDateColName); 
                Console.WriteLine(command.CommandText);
                command.Connection = connection;    
                command.Transaction = transaction;
                int res = command.ExecuteNonQuery();
                Console.WriteLine("Execution -> res : {0}", res);    
                transaction.Commit();
                transaction.Dispose();
                Console.WriteLine("Record Deleted","Congrats");    
               }
               catch (OdbcException exp)
               {
                    Console.WriteLine("Database Error:" + exp.Message.ToString());
                    transaction.Commit();
                    transaction.Dispose();
                    return false;
               }
               return true;
        }
        public static void InsertRow(string ConnectionString, string insertQuery, string TableName = "BaggageContext")
        {
            string query = string.Format("select * from [{0}]", (object) TableName);
            using (OdbcConnection connection = new OdbcConnection(ConnectionString))
            {
               try{
                    OdbcCommand cmd = connection.CreateCommand();    
                    connection.Open();
                    cmd.CommandText = insertQuery; 
                    Console.WriteLine(cmd.CommandText);
                    cmd.Connection = connection;    
                    cmd.ExecuteNonQuery();    
                    Console.WriteLine("Record Submitted","Congrats");    
                    connection.Close();   
               }
               catch (OdbcException exp)
               {
                        Console.WriteLine("Database Error:" + exp.Message.ToString());
               }

            }
        }
        static async Task<HashSet<BaggageDataModel>> GetBaggageData(UrlGenerator urls )
        {
            Console.WriteLine("Making API call to airport at: " + DateTime.Now);
            using (var client = new HttpClient(new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate }))
            {
                Console.WriteLine(urls.Name);
                Console.WriteLine(urls.UrlDetail.BaseUrl);
                Console.WriteLine(urls.UrlDetail.Token);
                client.BaseAddress = new Uri(urls.UrlDetail.BaseUrl);
                HttpResponseMessage response = client.GetAsync(urls.GetUrl()).Result;
                if(response.IsSuccessStatusCode == false)
                {
                    Console.WriteLine("Error has Occured");
                    return null;
                }
                response.EnsureSuccessStatusCode();
                string resultJson = response.Content.ReadAsStringAsync().Result;
                using (StreamWriter writer = new StreamWriter("test_json.json"))
                {
                    writer.Write(resultJson);
                }
                var jsonObj = JsonSerializer.Deserialize<Dictionary<string,FlightBaggageModel>>(resultJson);
                HashSet<BaggageDataModel> baggagesData = new HashSet<BaggageDataModel>();
                
                foreach(var jo in jsonObj){
                    foreach(var baggageItem in jo.Value.baggage.items){
                        BaggageDataModel bgmd = new BaggageDataModel();
                        //Flight
                        bgmd.FlightName = jo.Key.ElementAt(0).ToString();
                        bgmd.FlightCode = jo.Value.flight.code;
                        bgmd.FilghtId = jo.Value.flight.id;
                        //Airline
                        bgmd.AirlineCode = jo.Value.airline.code;
                        bgmd.AirlineTitle = jo.Value.airline.title;
                        //Airport
                        bgmd.AirportCode = jo.Value.airport.code;
                        bgmd.AirportTitle = jo.Value.airport.title;
                        //City
                        bgmd.CityName = jo.Value.city;
                        //Baggage
                        bgmd.BaggageBarcode = baggageItem.barcode;
                        bgmd.BaggageCreatedAt = DateTime.ParseExact(baggageItem.createdAt, "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.CurrentCulture);
                        bgmd.BaggageGroup = baggageItem.group;
                        //Add Baggage Data
                        baggagesData.Add(bgmd);
                    }
                    Console.WriteLine(jo);
                }    
                return baggagesData;
            }
            
        }
        static async Task<HashSet<FlightDataModel>> GetFlightsData(UrlGenerator urls )
        {
            Console.WriteLine("Making API call to airport at: " + DateTime.Now);
            using (var client = new HttpClient(new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate }))
            {
                Console.WriteLine(urls.Name);
                Console.WriteLine(urls.UrlDetail.BaseUrl);
                Console.WriteLine(urls.UrlDetail.Token);
                client.BaseAddress = new Uri(urls.UrlDetail.BaseUrl);
                HttpResponseMessage response = client.GetAsync(urls.GetUrl()).Result;
                if(response.IsSuccessStatusCode == false)
                {
                    Console.WriteLine("Error has Occured");
                    return null;
                }
                response.EnsureSuccessStatusCode();
                string resultJson = response.Content.ReadAsStringAsync().Result;
                using (StreamWriter writer = new StreamWriter("test_json.json"))
                {
                    writer.Write(resultJson);
                }
                var jsonObj = JsonSerializer.Deserialize<Dictionary<string,FlightBaggageModel>>(resultJson);
                HashSet<FlightDataModel> flightsData = new HashSet<FlightDataModel>();
                
                foreach(var jo in jsonObj)
                {
                    FlightDataModel fmd = new FlightDataModel();
                        //Flight
                        fmd.FlightName = jo.Key.ElementAt(0).ToString();
                        fmd.FlightId = jo.Value.flight.id;
                        fmd.FlightCode = jo.Value.flight.code;
                        //Airline
                        fmd.AirlineCode = jo.Value.airline.code;
                        fmd.AirlineTitle = jo.Value.airline.title;
                        //City
                        fmd.City = jo.Value.city;
                        //Airport
                        fmd.AirportCode = jo.Value.airport.code;
                        fmd.AirportTitle = jo.Value.airport.title;
                        //Baggage
                        fmd.BaggageQuantity =  (string)jo.Value.baggage.total.quantity;
                        fmd.BaggageTotalWight =  (string)jo.Value.baggage.total.weight;
                        //Add Baggage Data
                        flightsData.Add(fmd);
                }
                return flightsData;
            }
            
        }

        public static bool isTimeBetween(DateTime datetime, TimeSpan start, TimeSpan end)
        {
            // convert datetime to a TimeSpan
            TimeSpan now = datetime.TimeOfDay;
            // see if start comes before end
            if (start < end)
                return start <= now && now <= end;
            // start is after end, so do the inverse comparison
            return !(end < now && now < start);
        }
        static async Task Main(string[] args)
        {
            string sCurrentDirectory = AppDomain.CurrentDomain.BaseDirectory;
            Console.WriteLine(sCurrentDirectory);
            string sFile = System.IO.Path.Combine(sCurrentDirectory, @"..\..\..\TMBCrawler Project\Config\Configurations.json");
            string sFilePath = Path.GetFullPath(sFile);
            Console.WriteLine(sFilePath);
            string jsonConf;
            using(StreamReader f = new StreamReader(sFilePath)){
                jsonConf = f.ReadToEnd().ToString();
            }
            Console.WriteLine(jsonConf);
            Configuration conf = JsonSerializer.Deserialize<Configuration>(jsonConf);

            Console.WriteLine("Configuration: " + conf.DBConnection);
            var toDeleteAction = false;
            
            // Delete Test
            // string ConStr = conf.DBConnection;
            // using (OdbcConnection connectionForCheck = new OdbcConnection(ConStr))
            // {
            //     using (OdbcConnection connection = new OdbcConnection(ConStr))
            //     {
            //         connection.Open();
            //         OdbcTransaction transaction = connection.BeginTransaction();
            //         OdbcCommand command = connection.CreateCommand();
            //         DeleteOldRecordsFromDb(connection, transaction, command);
            //         transaction = null;
            //         transaction = connection.BeginTransaction();
            //         connection.Close();   
            //         }
            //         connectionForCheck.Close();
            // }
            // return ;
            while(true)
            {
                var baggegesData = await GetBaggageData(conf.UrlDetails);
                var flightsData = await GetFlightsData(conf.UrlDetails);
                string ConStr = conf.DBConnection;
                Console.WriteLine(conf.DBConnection);
                //Add Baggages Data
                using (OdbcConnection connectionForCheck = new OdbcConnection(ConStr)){
                    using (OdbcConnection connection = new OdbcConnection(ConStr))
                    {
                        connection.Open();
                        OdbcTransaction transaction = connection.BeginTransaction();
                        OdbcCommand command = connection.CreateCommand();
                        foreach(var bd in baggegesData)
                        {
                            bd.InsertToDB(connection, transaction, command);
                            transaction = null;
                            transaction = connection.BeginTransaction();
                        }
                        if(isTimeBetween(DateTime.Now, new TimeSpan(0,0,0), new TimeSpan(1,0,0)))
                        {
                            if(toDeleteAction == false)
                            {
                                DeleteOldRecordsFromDb(connection, transaction, command);
                                // transaction = null;
                                // transaction = connection.BeginTransaction();

                            }
                        }
                        Console.WriteLine("Batch Records Submitted","Congrats");    
                        connection.Close();   
                    }
                    connectionForCheck.Close();
                }
                //Add Flights Data
                using (OdbcConnection connectionForCheck = new OdbcConnection(ConStr)){
                    using (OdbcConnection connection = new OdbcConnection(ConStr))
                    {
                        connection.Open();
                        OdbcTransaction transaction = connection.BeginTransaction();
                        OdbcCommand command = connection.CreateCommand();
                        foreach(var fd in flightsData)
                        {
                            fd.InsertToDB(connection, transaction, command);
                            transaction = null;
                            transaction = connection.BeginTransaction();
                        }
                        Console.WriteLine("Batch Records Submitted","Congrats");    
                        connection.Close();   
                    }
                    connectionForCheck.Close();
                }
                Thread.Sleep(conf.Delay);
           }

        }

    }
}
    


