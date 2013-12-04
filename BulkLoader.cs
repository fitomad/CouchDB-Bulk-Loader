using System;
using System.IO;
using System.Net;
using System.Configuration;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace DesappstreStudio.CouchDB.Bulk
{
    /// <summary>
    /// CouchDB Bulk Loader from file.
    /// Data contained in file must be in JSON valid format
    /// </summary>
    public class BulkLoader : object
    {
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
        /// <param name="lines"></param>
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

            // Format a valid CouchDB bulk JSON document
            string content = string.Concat("{ \"docs\" : [ ", documents, " ] }");

            StreamWriter writer = new StreamWriter(request.GetRequestStream());
            writer.Write(content);

            writer.Close();
            writer.Dispose();

            HttpWebResponse response = (HttpWebResponse) request.GetResponse();

            StreamReader reader = new StreamReader(response.GetResponseStream());
            string response_content = reader.ReadToEnd();

            // TODO. Process response data
        }
        /// <summary>
        /// Stating point...
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
        {
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
