using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using SC.API.ComInterop;
using SC.API.ComInterop.Models;

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
            var queryStringRels = ConfigurationManager.AppSettings["queryStringRels"];

            // basic checks
            if (string.IsNullOrEmpty(userid) || userid == "USERID")
            {
                Log("Error: No username provided.");
                return;
            }
            if (string.IsNullOrEmpty(password))
            {
                // set the password from the encoded password
                password = Encoding.Default.GetString(Convert.FromBase64String(password64));
                if (string.IsNullOrEmpty(password64))
                {
                    Log("Error: No password provided.");
                    return;
                }
            }
            if (string.IsNullOrEmpty(url))
            {
                Log("Error: No URL provided.");
                return;
            }
            if (string.IsNullOrEmpty(storyid) || userid == "00000000-0000-0000-0000-000000000000")
            {
                Log("Error: No storyID provided.");
                return;
            }
            if (string.IsNullOrEmpty(connectionString) || connectionString == "CONNECTIONSTRING")
            {
                Log("Error: No connection string provided.");
                return;
            }
            if (string.IsNullOrEmpty(queryString) || userid == "QUERYSTRING")
            {
                Log("Error: No database query provided.");
                return;
            }

            // do the work

            try
            {
                Log($"Starting process.");
                var start = DateTime.UtcNow;

                // create our connection
                var sc = new SharpCloudApi(userid, password, url);
                var story = sc.LoadStory(storyid);


                using (DbConnection connection = GetDb(connectionString))
                {
                    connection.Open();

                    UpdateItems(connection, story, queryString);

                    UpdateRelationships(connection, story, queryStringRels);

                    Log("Saving");

                    story.Save();

                    Log($"Process completed in {(DateTime.UtcNow-start).Seconds} seconds.");
                }
            }
            catch (Exception e)
            {
                Log("Error: " + e.Message);
            }
        }

        private static void UpdateItems(DbConnection connection, Story story, string queryString)
        {
            if (string.IsNullOrWhiteSpace(queryString)) // nothing to do
                return;

            Log("Updating items");

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
                                DateTime date;
                                double dbl;
                                var s = o.ToString();
                                if (double.TryParse(s, out dbl))
                                {
                                    data.Add($"{dbl:#.##}");
                                }
                                else if (DateTime.TryParse(s, out date))
                                {
                                    data.Add(date.ToString("yyyy MM dd"));
                                }
                                else if (s.ToLower().Trim() == "null")
                                {
                                    data.Add("");
                                }
                                else
                                {
                                    data.Add(s);
                                }
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
                        Log(string.Format("{0} rows processed.", row));
                    }
                    else
                    {
                        Log(errorMessage);
                    }
                }
            }
        }

        private static void UpdateRelationships(DbConnection connection, Story story, string queryString)
        {
            if (string.IsNullOrWhiteSpace(queryString)) // nothing to do
                return;

            Log("Updating relationships");

            string strItem1 = "ITEM1";
            bool bItemName1 = true;
            string strItem2 = "ITEM2";
            bool bItemName2 = true;
            bool bDirection = false;
            bool bComment = false;
            bool bTags = false;

            int row = 1;

            using (DbCommand command = connection.CreateCommand())
            {
                command.CommandText = queryString;
                command.CommandType = CommandType.Text;

                using (DbDataReader reader = command.ExecuteReader())
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var col = reader.GetName(i).ToUpper();
                        if (col == "ITEM 1")
                            strItem1 = "ITEM 1";
                        else if (col == "EXTERNALID1")
                        {
                            bItemName1 = false;
                            strItem1 = "EXTERNALID1";
                        }
                        else if (col == "EXTERNALID 1")
                        {
                            bItemName1 = false;
                            strItem1 = "EXTERNALID 1";
                        }
                        else if (col == "ITEM 2")
                            strItem2 = "ITEM 2";
                        else if (col == "EXTERNALID2")
                        {
                            bItemName2 = false;
                            strItem2 = "EXTERNALID2";
                        }
                        else if (col == "EXTERNALID 2")
                        {
                            bItemName2 = false;
                            strItem2 = "EXTERNALID 2";
                        }
                        else if (col == "COMMENT")
                            bComment = true;
                        else if (col == "DIRECTION")
                            bDirection = true;
                        else if (col == "TAGS")
                            bTags = true;
                    }

                    while (reader.Read())
                    {
                        var t1 = reader[strItem1].ToString();
                        var t2 = reader[strItem2].ToString();

                        var i1 = (bItemName1) ? story.Item_FindByName(t1) : story.Item_FindByExternalId(t1);
                        var i2 = (bItemName2) ? story.Item_FindByName(t2) : story.Item_FindByExternalId(t2);

                        if (i1 == null || i2 == null)
                        {
                            Log($"ERROR: Could not find items '{t1}' or '{t2}' on {row}.");
                        }
                        else
                        {
                            var rel = story.Relationship_FindByItems(i1, i2) ??
                                      story.Relationship_AddNew(i1, i2);
                            if (bComment)
                                rel.Comment = reader["COMMENT"].ToString();
                            if (bDirection)
                            {
                                var txt = reader["DIRECTION"].ToString().Replace(" ", "").ToUpper();
                                if (txt.Contains("BOTH"))
                                    rel.Direction = Relationship.RelationshipDirection.Both;
                                else if (txt.Contains("ATOB") || txt.Contains("1TO2"))
                                    rel.Direction = Relationship.RelationshipDirection.AtoB;
                                else if (txt.Contains("BTOA") || txt.Contains("2TO1"))
                                    rel.Direction = Relationship.RelationshipDirection.Both;
                                else
                                    rel.Direction = Relationship.RelationshipDirection.None;
                            }
                            if (bTags)
                            {
                                // TODO - delete tags - needs implementing in the SDK        
                                var tags = reader["TAGS"].ToString();
                                foreach (var t in tags.Split(','))
                                {
                                    var tag = t.Trim();
                                    if (!string.IsNullOrEmpty(tag))
                                        rel.Tag_AddNew(tag);
                                }
                            }
                        }
                        row++;
                    }
                }
            }
            Log($"{row} rows processed.");

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
         
        private static void Log(string text)
        {
            var now = DateTime.UtcNow;
            text = now.ToShortDateString() + " " + now.ToLongTimeString() + " " + text + "\r\n";
            var LogFile = ConfigurationManager.AppSettings["LogFile"];
            if (!string.IsNullOrEmpty(LogFile) && LogFile != "LOGFILE")
            {
                try
                {

                    File.AppendAllText(LogFile, text);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error writing to {LogFile}");
                    Console.WriteLine($"{ex.Message}");
                }
            }

            Debug.Write(text);
            Console.Write(text);
        }
    }
}
