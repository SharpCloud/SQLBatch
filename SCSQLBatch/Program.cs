using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Text;
using SC.API.ComInterop;

namespace SCSQLBatch
{
    class Program
    {
        static void Main(string[] args)
        {
            var userid = ConfigurationManager.AppSettings["userid"];
            var password = ConfigurationManager.AppSettings["password"];
            var password64 = ConfigurationManager.AppSettings["password64"];
            var url = ConfigurationManager.AppSettings["url"];
            var storyid = ConfigurationManager.AppSettings["storyid"];
            var connectionString = ConfigurationManager.AppSettings["connectionString"];
            var queryString = ConfigurationManager.AppSettings["queryString"];

            // basic checks
            if (string.IsNullOrEmpty(userid) || userid == "USERID")
            {
                Console.WriteLine("Error: No username provided.");
                return;
            }
            if (string.IsNullOrEmpty(password))
            {
                // set the password from the encoded password
                password = Encoding.Default.GetString(Convert.FromBase64String(password64));
                if (string.IsNullOrEmpty(password64))
                {
                    Console.WriteLine("Error: No password provided.");
                    return;
                }
            }
            if (string.IsNullOrEmpty(url))
            {
                Console.WriteLine("Error: No URL provided.");
                return;
            }
            if (string.IsNullOrEmpty(storyid) || userid == "00000000-0000-0000-0000-000000000000")
            {
                Console.WriteLine("Error: No storyID provided.");
                return;
            }
            if (string.IsNullOrEmpty(connectionString) || connectionString == "CONNECTIONSTRING")
            {
                Console.WriteLine("Error: No connection string provided.");
                return;
            }
            if (string.IsNullOrEmpty(queryString) || userid == "QUERYSTRING")
            {
                Console.WriteLine("Error: No database query provided.");
                return;
            }

            // do the work

            try
            {
                // create our connection
                var sc = new SharpCloudApi(userid, password, url);
                var story = sc.LoadStory(storyid);

                using (DbConnection connection = GetDb(connectionString))
                {
                    connection.Open();
                    using (DbCommand command = connection.CreateCommand())
                    {
                        command.CommandText = queryString;
                        command.CommandType = CommandType.Text;

                        using (DbDataReader reader = command.ExecuteReader())
                        {
                            var tempArray = new List<List<string>>();
                            while (reader.Read())
                            {
                                var objs = new object[reader.FieldCount];
                                reader.GetValues(objs);
                                var data = new List<string>();
                                foreach (var o in objs)
                                {
                                    if (o is DateTime)
                                    {
                                        var date = (DateTime)o;
                                          data.Add(date.ToString("yyyy MM dd"));

                                    }
                                    else
                                    {
                                        data.Add(o.ToString());
                                    }
                                }
                                tempArray.Add(data);
                            }

                            // create our string arrar
                            var arrayValues = new string[tempArray.Count + 1, reader.FieldCount];
                            // add the headers
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                arrayValues[0, i] = reader.GetName(i);
                            }
                            // add the data values
                            int row = 1;
                            foreach (var list in tempArray)
                            {
                                int col = 0;
                                foreach (string s in list)
                                {
                                    arrayValues[row, col++] = s;
                                }
                                row++;
                            }

                            // pass the array to SharpCloud
                            string errorMessage;
                            if (story.UpdateStoryWithArray(arrayValues, false, out errorMessage))
                            {
                                story.Save();
                                Console.WriteLine(string.Format("{0} rows updated.", row));
                            }
                            else
                            {
                                Console.WriteLine(errorMessage);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }
        }

        private static DbConnection GetDb(string connectionString)
        {
            var dbType = ConfigurationManager.AppSettings["dbType"];
            switch (dbType)
            {
                default:
                case "SQL":
                    return new SqlConnection(connectionString);
                case "ODBC":
                    return new OdbcConnection(connectionString);
                case "OLEDB":
                    return new OleDbConnection(connectionString);
            }
        }
    }
}
