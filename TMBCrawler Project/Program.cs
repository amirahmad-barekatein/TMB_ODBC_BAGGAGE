using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using JsonAnalyzer;
using ObjectStructures;
using System.Threading;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Data.OleDb;
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
        public string? BaggageCreatedAt { get; set; }

        public static string DBTableName = "BaggageContext";
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
        
        public bool InsertToDB(OleDbConnection connection, OleDbTransaction transaction, OleDbCommand command, string TableName = "BaggageContext")
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
               catch (OleDbException exp)
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
                            (sb, pair) => sb.Append( "\"" + pair.Value.ToString() + "\"" + ","),
                            sb => sb.ToString());
            values = values.Remove(values.Length - 1, 1); 
            insertQuery = insertQuery 
                        + values
                        +")";
            //var existanceQuery = "IF NOT EXISTS (SELECT BaggageBarcode FROM " + DBTableName + " WHERE BaggageBarcode = " + BaggageBarcode + ") ";
            //var insertNoDuplicateQuery = "BEGIN " + existanceQuery + insertQuery + " END;";
            
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
        public static void InsertRow(string ConnectionString, string insertQuery, string TableName = "BaggageContext")
        {
            string query = string.Format("select * from [{0}]", (object) TableName);
            using (OleDbConnection connection = new OleDbConnection(ConnectionString))
            {
               try{
                    OleDbCommand cmd = connection.CreateCommand();    
                    connection.Open();
                    cmd.CommandText = insertQuery; 
                    Console.WriteLine(cmd.CommandText);
                    cmd.Connection = connection;    
                    cmd.ExecuteNonQuery();    
                    Console.WriteLine("Record Submitted","Congrats");    
                    connection.Close();   
               }
               catch (OleDbException exp)
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
                        bgmd.BaggageCreatedAt = baggageItem.createdAt;
                        bgmd.BaggageGroup = baggageItem.group;
                        //Add Baggage Data
                        baggagesData.Add(bgmd);
                    }
                    Console.WriteLine(jo);
                }    
                return baggagesData;
            }
            
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
            while(true){
                var baggegesData = await GetBaggageData(conf.UrlDetails);
                string ConStr = conf.DBConnection;
                Console.WriteLine(conf.DBConnection);
                using (OleDbConnection connectionForCheck = new OleDbConnection(ConStr)){

                
                    using (OleDbConnection connection = new OleDbConnection(ConStr))
                    {
                        connection.Open();
                        OleDbTransaction transaction = connection.BeginTransaction();
                        OleDbCommand command = connection.CreateCommand();
                        foreach(var bd in baggegesData)
                        {
                            bd.InsertToDB(connection, transaction, command);
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
    


