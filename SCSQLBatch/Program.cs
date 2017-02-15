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
using System.Globalization;

namespace SCSQLBatch
{
    class Program
    {
        static bool unpublishItems = false;

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
            bool unpubItems;
            if (bool.TryParse(ConfigurationManager.AppSettings["unpublishItems"], out unpubItems))
                unpublishItems = unpubItems;
            var proxy = ConfigurationManager.AppSettings["proxy"];
            bool proxyAnonymous = true;
            bool.TryParse(ConfigurationManager.AppSettings["proxyAnonymous"], out proxyAnonymous);
            var proxyUsername = ConfigurationManager.AppSettings["proxyUsername"];
            var proxyPassword = ConfigurationManager.AppSettings["proxyPassword"];
            var proxyPassword64 = ConfigurationManager.AppSettings["proxyPassword64"];

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
            if (!string.IsNullOrEmpty(proxy) && !proxyAnonymous)
            {
                if (string.IsNullOrEmpty(proxyUsername) || string.IsNullOrEmpty(proxyPassword))
                {
                    Log("Error: No proxy username or password provided.");
                }
                if (string.IsNullOrEmpty(proxyPassword))
                {
                    proxyPassword = Encoding.Default.GetString(Convert.FromBase64String(proxyPassword64));
                }
            }
            // do the work

            try
            {
                Log($"Starting process.");
                var start = DateTime.UtcNow;

                // create our connection
                var sc = new SharpCloudApi(userid, password, url, proxy, proxyAnonymous, proxyUsername, proxyPassword);
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
                                    data.Add($"{dbl:0.##}");
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

                    if (unpublishItems)
                    {
                        List<Guid> updatedItems;
                        if (story.UpdateStoryWithArray(arrayValues, false, out errorMessage, out updatedItems))
                        {
                            foreach (var item in story.Items)
                            {
                                item.AsElement.IsInRoadmap = updatedItems.Contains(item.AsElement.ID);
                            }
                            Log(string.Format("{0} rows processed.", row));
                        }
                        else
                        {
                            Log(errorMessage);
                        }
                    }
                    else {
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
        }

        private static bool TypeIsNumeric(Type type)
        {
            return type == typeof(double) || type == typeof(int) || type == typeof(float) || type == typeof(decimal) ||
                type == typeof(short) || type == typeof(long) || type == typeof(byte) || type == typeof(SByte) ||
                type == typeof(UInt16) || type == typeof(UInt32) || type == typeof(UInt64);
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
            var attributeColumns = new List<RelationshipAttribute>();
            var attributesToCreate = new List<string>();
            var updatedRelationships = new List<Relationship>();
            var attributeValues = new Dictionary<string, Dictionary<Relationship, string>>();

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
                        else
                        {
                            if (story.RelationshipAttributes.Any(a => a.Name.ToUpper() == col))
                            {
                                attributeColumns.Add(story.RelationshipAttributes.FirstOrDefault(a => a.Name.ToUpper() == col));
                            }
                            else if (!attributesToCreate.Any(name => name.ToUpper() == col))
                            {
                                var type = reader.GetFieldType(i);

                                if (type == typeof(DateTime))
                                {
                                    var newAttribute = story.RelationshipAttribute_Add(reader.GetName(i), RelationshipAttribute.RelationshipAttributeType.Date);
                                    attributeColumns.Add(newAttribute);
                                }
                                else if (TypeIsNumeric(type))
                                {
                                    var newAttribute = story.RelationshipAttribute_Add(reader.GetName(i), RelationshipAttribute.RelationshipAttributeType.Numeric);
                                    attributeColumns.Add(newAttribute);
                                }
                                else {
                                    attributesToCreate.Add(reader.GetName(i));
                                    attributeValues.Add(reader.GetName(i), new Dictionary<Relationship, string>());
                                }
                            }
                        }
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

                            foreach (var att in attributeColumns)
                            {
                                var val = reader[att.Name];

                                if (val == null || val is DBNull || val.ToString() == "(NULL)")
                                {
                                    rel.RemoveAttributeValue(att);
                                }
                                else {
                                    switch (att.Type)
                                    {
                                        case RelationshipAttribute.RelationshipAttributeType.Date:
                                            rel.SetAttributeValue(att, (DateTime)val);
                                            break;
                                        case RelationshipAttribute.RelationshipAttributeType.Numeric:
                                            rel.SetAttributeValue(att, (double)val);
                                            break;
                                        case RelationshipAttribute.RelationshipAttributeType.List:
                                        case RelationshipAttribute.RelationshipAttributeType.Text:
                                            rel.SetAttributeValue(att, val.ToString());
                                            break;
                                    }
                                }
                            }

                            foreach (var newAtt in attributesToCreate)
                            {
                                // Attributes we don't know the type of, keep all the values
                                attributeValues[newAtt].Add(rel, reader[newAtt].ToString());
                            }
                        }
                        row++;
                    }

                    foreach (var item in attributeValues)
                    {
                        var nullCount = 0;
                        var numCount = 0;
                        var dateCount = 0;
                        var labels = new List<string>();
                        var isText = false;
                        double outDouble;
                        DateTime outDateTime;

                        // Find the attribute type
                        foreach (var rel in item.Value)
                        {
                            if (string.IsNullOrEmpty(rel.Value) || rel.Value == "(NULL)")
                            {
                                nullCount++;
                            }
                            else if (double.TryParse(rel.Value, out outDouble))
                            {
                                numCount++;
                            }
                            else if (DateTime.TryParseExact(rel.Value, "yyyy MM dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "yyyy MMM dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "yyyy-MMM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "yyyy/MM/dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "yyyy/MMM/dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "dd MM yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "dd MMM yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "dd-MM-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "dd-MMM-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "dd/MM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "dd/MMM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                              || DateTime.TryParseExact(rel.Value, "dd/MM/yyyy hh:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime))
                            {
                                dateCount++;
                            }
                            else {
                                if (!isText)
                                {
                                    if (rel.Value.Length > 100)
                                    {
                                        isText = true;
                                    }
                                    else
                                    {
                                        if (!labels.Contains(rel.Value))
                                        {
                                            labels.Add(rel.Value);
                                        }
                                    }
                                }
                            }
                        }

                        RelationshipAttribute newAttribute;
                        if (dateCount > 0 && dateCount + nullCount == item.Value.Count)
                        {
                            newAttribute = story.RelationshipAttribute_Add(item.Key, RelationshipAttribute.RelationshipAttributeType.Date);
                            foreach (var rel in item.Value)
                            {
                                if (!string.IsNullOrEmpty(rel.Value) && rel.Value != "(NULL)")
                                {
                                    if (DateTime.TryParseExact(rel.Value, "yyyy MM dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "yyyy MMM dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "yyyy-MMM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "yyyy/MM/dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "yyyy/MMM/dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "dd MM yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "dd MMM yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "dd-MM-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "dd-MMM-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "dd/MM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "dd/MMM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime)
                          || DateTime.TryParseExact(rel.Value, "dd/MM/yyyy hh:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out outDateTime))
                                    {
                                        rel.Key.SetAttributeValue(newAttribute, outDateTime);
                                    }
                                }
                            }
                        }
                        else if (numCount > 0 && numCount + nullCount == item.Value.Count)
                        {
                            newAttribute = story.RelationshipAttribute_Add(item.Key, RelationshipAttribute.RelationshipAttributeType.Numeric);
                            foreach (var rel in item.Value)
                            {
                                if (!string.IsNullOrEmpty(rel.Value) && rel.Value != "(NULL)")
                                {
                                    if (double.TryParse(rel.Value, out outDouble))
                                    {
                                        rel.Key.SetAttributeValue(newAttribute, outDouble);
                                    }
                                }
                            }
                        }
                        else {
                            if (!isText && (labels.Count + nullCount < item.Value.Count))
                            {
                                newAttribute = story.RelationshipAttribute_Add(item.Key, RelationshipAttribute.RelationshipAttributeType.List);
                            }
                            else
                            {
                                newAttribute = story.RelationshipAttribute_Add(item.Key, RelationshipAttribute.RelationshipAttributeType.Text);
                            }
                            foreach (var rel in item.Value)
                            {
                                if (!string.IsNullOrEmpty(rel.Value) && rel.Value != "(NULL)")
                                {
                                    rel.Key.SetAttributeValue(newAttribute, rel.Value);
                                }
                            }
                        }
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
