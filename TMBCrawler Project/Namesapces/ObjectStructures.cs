using System.Text;

namespace ObjectStructures
{
    public class UrlGenerator
    {
        public string Name{set; get;}
        public UrlDetail UrlDetail {get; set;}

        public string GetUrl(){
            var url = UrlDetail.BaseUrl 
                    + "?"
                    + UrlDetail.Token;
            return url;
        }
    }
    public class UrlDetail 
    {
        public string BaseUrl {set; get;}
        public string Token {set; get;}
    }

    public class Configuration
    {
        public string DBConnection {get; set;}
        public UrlGenerator UrlDetails {get; set;}
        public int Delay {get; set;}
    }
}