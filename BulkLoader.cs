using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Configuration;

namespace DesappstreStudio.CouchDB.Bulk
{
    /// <summary>
    /// CouchDB Bulk Loader from file.
    /// Data contained in file must be in JSON valid format
    /// </summary>
    public class BulkLoader : object
    {
        //
        private long bulk_operations;
        //
        private int bulk_errors;
        //
        private int block_limit;
        // 
        private DateTime init_time;
        //
        private static object Sync = new object();

        /// <summary>
        /// URL to _bulk_docs operations
        /// </summary>
        private string CouchDBURL
        {
            get;
            set;
        }

        /// <summary>
        /// Load data from a file to CouchDB 
        /// </summary>
        /// <param name="file">File that contains json valid data</param>
        /// <param name="rows">Inserts per operation</param>
        public BulkLoader(string file, int limit) : base()
        {
            this.FormatCouchDBURL();
            this.init_time = DateTime.Now;
            this.block_limit = limit;

            List<string> lines = new List<string>();
            List<Task> tasks = new List<Task>();

            FileStream fs = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            BufferedStream bs = new BufferedStream(fs);

            using(StreamReader sr = new StreamReader(bs))
            {
                int count = 0;
                string line = string.Empty;
                

                while((line = sr.ReadLine()) != null)
                {
                    lines.Add(line);
                    count++;

                    if(count == limit)
                    {
                        List<string> copy = new List<string>(lines);

                        Task bulk_task = this.CreateBulkTask(copy);
                        tasks.Add(bulk_task);

                        count = 0;
                        lines.Clear();
                    }
                }

                if(lines.Count != 0)
                {
                    Task bulk_task = this.CreateBulkTask(lines);
                    tasks.Add(bulk_task);
                }
            }

            Task.WaitAll(tasks.ToArray());
        }

        /// <summary>
        /// Build the URL based on the App.config data
        /// </summary>
        private void FormatCouchDBURL()
        {
            string base_url = ConfigurationManager.AppSettings["couchdb_server_baseurl"];
            string database = ConfigurationManager.AppSettings["couchdb_server_db"];

            this.CouchDBURL = string.Format("{0}/{1}/_bulk_docs", base_url, database);
        }

        /// <summary>
        /// Create an async operation in which the data are loaded into CouchDB
        /// </summary>
        /// <param name="lines"></param>
        /// <returns></returns>
        private Task CreateBulkTask(List<string> lines)
        {
            Task bulk_task = new Task(() =>
            {
                this.Bulk(lines);
            });

            bulk_task.Start();

            return bulk_task;
        }

        /// <summary>
        /// HTTP Bulk process
        /// </summary>
        /// <param name="lines">JSON docs to upload</param>
        private void Bulk(List<string> lines)
        {
            HttpWebRequest request = (HttpWebRequest) WebRequest.Create(this.CouchDBURL);
            request.ContentType = "application/json";
            request.Method = "POST";
            request.ProtocolVersion = HttpVersion.Version11;

            string documents = string.Empty;

            foreach(string line in lines)
            {
                documents = string.IsNullOrEmpty(documents) ? line : string.Concat(documents, ", ", line);
            }

            string content = string.Concat("{ \"docs\" : [ ", documents, " ] }");

            StreamWriter writer = new StreamWriter(request.GetRequestStream());
            writer.Write(content);

            writer.Close();
            writer.Dispose();

            try
            {
                HttpWebResponse response = (HttpWebResponse) request.GetResponse();

                StreamReader reader = new StreamReader(response.GetResponseStream());
                string response_content = reader.ReadToEnd();
            }
            catch(WebException)
            {
                // GetResponse error.
                Interlocked.Increment(ref bulk_errors);
            }

            // TODO. Process response data

            lock(BulkLoader.Sync)
            {
                bulk_operations += lines.Count;
                this.ShowBulkState();
            }
        }

        /// <summary>
        /// Show bulk state info during operations
        /// </summary>
        private void ShowBulkState()
        {            
            TimeSpan span = (DateTime.Now - init_time);

            if(bulk_operations == block_limit)
            {
                string message = string.Format("JSON objects loaded: {0}\r\nBulk operation running: {1}\r\nBulk errors: {2}", bulk_operations.ToString("N0"), span.ToString(), bulk_errors.ToString("N0"));
                Console.Write(message);
            }
            else
            {
                Console.CursorTop -= 2;
                Console.CursorLeft = "JSON objects loaded: ".Length;
                Console.Write(bulk_operations.ToString("N0"));
                Console.CursorTop++;
                Console.CursorLeft = "Bulk operation running: ".Length;
                Console.Write(span.ToString());
                Console.CursorTop++;
                Console.CursorLeft = "Bulk errors: ".Length;
                Console.Write(bulk_errors.ToString("N0")); 
            }

        }

        /// <summary>
        /// Stating point...
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
        {
            Console.CursorVisible = false;

            if(args.Length == 1)
            {
                if(!File.Exists(args[0]))
                {
                    Console.WriteLine("File doen't exists. Check file path.");
                    Environment.Exit(-1);
                }

                int block = Convert.ToInt32(ConfigurationManager.AppSettings["couchdb_server_block"]);
                BulkLoader bulkOP = new BulkLoader(args[0], block);
            }
            else
            {
                Environment.Exit(-1);
            }
        }
    }
}
