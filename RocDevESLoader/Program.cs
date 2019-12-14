using System;
using System.IO;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace RocDevESLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            foreach (String _file in Directory.GetFiles(@"D:\Citibike", "*citibike-tripdata.csv"))
            {
                ParseAFileToES(_file);
            }

            Console.ReadLine();
        }


        static void ParseAFileToES(String FileName)
        {
            using (StreamReader sr = new System.IO.StreamReader(FileName))
            {
                int _RowCounter = 0;
                List<Task> ThreadList = new List<Task>();
                StringBuilder sb = new StringBuilder();
                String _line = String.Empty;
                while ((_line = sr.ReadLine()) != null)
                {
                    //Header lines just contain column names so let's skip those.
                    if (_RowCounter == 0)
                    {
                        _RowCounter++;
                        continue;
                    }

                    String[] _Fields = _line.Split(",");
                    sb.Append("{\"index\" : { \"_index\" : " + JsonConvert.ToString("biketrips") + " }}");
                    sb.Append(Environment.NewLine);

                    sb.Append("{\"TripDuration\":" + JsonConvert.ToString((_Fields[0].Trim('"'))));

                    //TODO: Strip the quotes around the fields after separating by comma.
                    if (DateTime.TryParse(_Fields[1].Trim('"'), out DateTime _StartTime)) { sb.Append(", \"Start.Time\":" + JsonConvert.ToString(_StartTime.ToUniversalTime().ToString("s"))); }
                    if (Int32.TryParse(_Fields[3].Trim('"'), out Int32 _StartStation)) { sb.Append(", \"Start.StationID\":" + _StartStation.ToString()); }
                    if (!String.IsNullOrEmpty(_Fields[4])) { sb.Append(", \"Start.Name\":" + JsonConvert.ToString(_Fields[4].Trim('"'))); }
                    if (!String.IsNullOrEmpty(_Fields[5])) { sb.Append(", \"Start.Location\": { \"lat\": " + _Fields[5].Trim('"') + ", \"lon\":" + _Fields[6].Trim('"') + "} "); }
                    if (DateTime.TryParse(_Fields[2].Trim('"'), out DateTime _EndTime)) { sb.Append(", \"Finish.Time\":" + JsonConvert.ToString(_EndTime.ToUniversalTime().ToString("s"))); }
                    if (Int32.TryParse(_Fields[7].Trim('"'), out Int32 _FinishStation)) { sb.Append(", \"Finish.StationID\":" + _FinishStation.ToString()); }
                    if (!String.IsNullOrEmpty(_Fields[8])) { sb.Append(", \"Finish.Name\":" + JsonConvert.ToString(_Fields[8].Trim('"'))); }
                    if (!String.IsNullOrEmpty(_Fields[9])) { sb.Append(", \"Finish.Location\": { \"lat\": " + _Fields[9].Trim('"') + ", \"lon\":" + _Fields[10].Trim('"') + "} "); }
                    if (Int32.TryParse(_Fields[11].Trim('"'), out Int32 _BikeID)) { sb.Append(", \"BikeID\":" + _BikeID.ToString()); }
                    if (!String.IsNullOrEmpty(_Fields[12])) { sb.Append(", \"UserType\":" + JsonConvert.ToString(_Fields[12].Trim('"'))); }
                    if (Int32.TryParse(_Fields[13].Trim('"'), out Int32 _BirthYear)) { sb.Append(", \"BirthYear\":" + _BirthYear.ToString()); }
                    if (Int32.TryParse(_Fields[14].Trim('"'), out Int32 _GenderInt))
                    {
                        if (_GenderInt == 0) sb.Append(", \"Gender\":\"Unspecified\"");
                        if (_GenderInt == 1) sb.Append(", \"Gender\":\"Male\"");
                        if (_GenderInt == 2) sb.Append(", \"Gender\":\"Female\"");
                    }
                    sb.Append("}");
                    sb.Append(Environment.NewLine);
                    _RowCounter++;

                    if (_RowCounter % 5000 == 0)
                    {
                        System.Diagnostics.Process _Process = System.Diagnostics.Process.GetCurrentProcess();
                        if (_Process.WorkingSet64 > (256 * 1048576)) //Cap this at 256MB
                        {
                            try { Task.WaitAll(ThreadList.ToArray()); }
                            catch (AggregateException exGroup)
                            {
                                foreach (Exception ex in exGroup.InnerExceptions) { Console.WriteLine(ex.Message); }
                            }
                            System.Threading.Thread.Sleep(5000);
                        }
                        Console.WriteLine(_RowCounter);
                        String _TempData = sb.ToString();
                        Task _Uploader = new Task(() => Uploader(_TempData));
                        ThreadList.Add(_Uploader);
                        _Uploader.Start();
                        sb = new StringBuilder();
                    }
                }
                try { Task.WaitAll(ThreadList.ToArray()); }
                catch (AggregateException exGroup)
                {
                    foreach (Exception ex in exGroup.InnerExceptions) { Console.WriteLine(ex.Message); }
                }
                Uploader(sb.ToString());
                Console.WriteLine($"Processed {FileName} with {_RowCounter} records.");                
            }
        }

        static void Uploader(String Data)
        {
            //Console.WriteLine(Data);

            HttpResponseMessage Response;
            HttpClientHandler RequestHandler = new HttpClientHandler();
            RequestHandler.ServerCertificateCustomValidationCallback += (sender, cert, chain, sslPolicyErrors) => true;

            using (HttpClient Client = new HttpClient(RequestHandler))
            {
                Client.Timeout = new TimeSpan(0, 30, 0);
                Client.DefaultRequestHeaders.Accept.Clear();
                Client.DefaultRequestHeaders.Add("User-Agent", "GenericESLoader");
                Uri _Address = new Uri(@"http://elasticpresentation:9200/" + @"_bulk");
                StringContent _Content = new StringContent(Data);
                _Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                Response = Client.PostAsync(_Address, _Content).Result;
                Response.EnsureSuccessStatusCode();
            }
        }
    }
}
