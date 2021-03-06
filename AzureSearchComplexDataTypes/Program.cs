﻿using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureSearchComplexDataTypes
{
    class Program
    {
        // You can set up the search service name and search service api key in the App.config file using this code:
        static string searchServiceNameAppConfig = ConfigurationManager.AppSettings[""];
        static string searchServiceApiKeyAppConfig = ConfigurationManager.AppSettings[""];

        // or you can also do it directly in the Program class as shown below:
        static string searchServiceName = "[SearchServiceName]";
        static string searchServiceApiKey = "[SearchServiceApiKey]";
        static string indexName = "[IndexName]";
        static string jsonFileName = "[jsonFileName]";
        static SearchServiceClient searchServiceClient;
        static SearchIndexClient searchIndexClient;

        static void Main(string[] args)
        {
            // This will create an Azure Search index, load a complex JSON data file
            // and perform some search queries.

            if ((searchServiceName == "") || (searchServiceApiKey == ""))
            {
                Console.WriteLine("Please add your searchServiceName and searchServiceApiKey.  Press any key to continue.");
                Console.ReadLine();
                return;
            }

            searchServiceClient = new SearchServiceClient(searchServiceName, new SearchCredentials(searchServiceApiKey));
            searchIndexClient = (SearchIndexClient)searchServiceClient.Indexes.GetClient(indexName);

            Console.WriteLine("Creating index...");
            ReCreateIndex();
            Console.WriteLine("Uploading documents");
            UploadDocuments();
            Console.WriteLine("Waiting 5 seconds for content to be indexed...");
            Thread.Sleep(5000);

            Console.WriteLine("\nFinding all people who work at the 'Adventureworks Headquarters'...");
            SearchContacts results = SearchDocuments(searchText: "*", filter: "locationsDescription / any(t: t eq 'Adventureworks Headquarters')");
            Console.WriteLine("Found matches:");
            foreach (var contact in results.Results)
            {
                Console.WriteLine("- {0}", contact.Document["name"]);
            }

            Console.WriteLine("\nGetting a count of the number of people who work at the 'Adventureworks Headquarters'...");
            results = SearchDocuments(searchText: "*", filter: "locationsDescription / any(t: t eq 'Home Office')");
            Console.WriteLine("{0} people have 'Home Offices'", results.Count);

            Console.WriteLine("\nOf the people who at a 'Home Office' show what other offices they work in with a count of the people in each location.");
            var locationsDescription = results.Facets.Where(item => item.Key == "locationsDescription");
            Console.WriteLine("Found matches:");

            foreach (var facets in locationsDescription)
            {
                foreach (var facet in facets.Value)
                {
                    Console.WriteLine("- Location: {0} ({1})", facet.Value, facet.Count);
                }
            }

            Console.WriteLine("\nGetting a count of people who work ath the 'Home Office' with the 'Id' of 4.");
            results = SearchDocuments(searchText: "*", filter: "locationsCombined / any(t: t eq '4||Home Office')");
            Console.WriteLine("{0} people have Home Offices with a Location Id of '4'", results.Count);

            Console.WriteLine("\nGetting people that have Home Offices with a Location Id of '4':");
            Console.WriteLine("Found matches:");

            foreach (var contact in results.Results)
            {
                Console.WriteLine("- {0}", contact.Document["name"]);
            }

            Console.WriteLine("Press any key to continue.");
            Console.ReadLine();
        }

        public static void ReCreateIndex()
        {
            // Delete and re-create the index.
            if (searchServiceClient.Indexes.Exists(indexName))
            {
                searchServiceClient.Indexes.Delete(indexName);
            }

            var definition = new Index()
            {
                // Index for contacts json file.
                Name = indexName,
                Fields = new[]
                {
                    new Field("id", DataType.String)
                    { IsKey =  true , IsSearchable = false, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },

                    new Field("name", DataType.String)
                    { IsKey = false, IsSearchable = true, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },

                    new Field("comapny", DataType.String)
                    { IsKey = false, IsSearchable = true, IsFilterable = false, IsSortable = false, IsRetrievable = true, IsFacetable = false },

                    new Field("locationsId", DataType.Collection(DataType.String))
                    { IsKey = false,  IsSearchable = true,  IsFilterable = true, IsSortable = false, IsRetrievable = true, IsFacetable = true },

                    new Field("locationsDescription", DataType.Collection(DataType.String))
                    { IsKey = false,  IsSearchable = true, IsFilterable = true, IsSortable = false, IsRetrievable = true, IsFacetable = true },

                    new Field("locationsCombined", DataType.Collection(DataType.String))
                    { IsKey = false,  IsSearchable = true, IsFilterable = true, IsSortable = false, IsRetrievable = true, IsFacetable = true }
                }

            };

            var defintion1 = new Index()
            {
               // Index for interpreterIntelligenceContacts json file.
               Name = indexName,
               Fields = new[]
               {
                        #region interpreterIntelligenceContacts fields.
                        new Field("id", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("versionValue", DataType.Int32)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("uuid", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("createdBy", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("createdDate", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("lastModifiedBy", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("lastModifiedDate", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("company_id", DataType.Int32)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("name", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("displayName", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("salutation", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("firstName", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("middleName", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("lastName", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("nickName", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("suffix", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("gender_id", DataType.Int32)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("businessUnit_id", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("dateOfBirth", DataType.String)
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false },

                        new Field("contactTypes", DataType.Collection(DataType.String))
                        { IsKey = true, IsSortable = false, IsSearchable = false, IsRetrievable = true, IsFilterable = false, IsFacetable = false }
                        #endregion
                    }
            };

            searchServiceClient.Indexes.Create(definition);
            searchServiceClient.Indexes.Create(definition1);
        }

        public static void UploadDocuments()
        {
            // This will open the JSON file, parse it and upload the documents in a batch.
            List<IndexAction> indexOperations = new List<IndexAction>();
            JArray json = JArray.Parse(File.ReadAllText(jsonFileName + ".json"));

            foreach (var contact in json)
            {
                // Parse the JSON object (contact)
                var doc = new Document
                {
                    { "id", contact["id"] },
                    { "name", contact["name"] },
                    { "comapny", contact["comapny"] },
                    { "locationId", contact["locations"].Select(item => item["id"]).ToList() },
                    { "locationsDescription", contact["locations"].Select(item => item["description"]).ToList() },
                    { "locationsCombined", contact["locations"].Select(item => item["id"] + "||" + item["description"]).ToList() }
                };

                // Parse the JSON object (interpreterIntelligenceContacts)
                var doc2 = new Document
                {
                    { "id", contact["id"] },
                    { "versionValue", contact["versionVlaue"] },
                    { "uuid", contact["uuid"] },
                    { "createdBy", contact["createdBy"] },
                    { "createdDate", contact["createdDate"] },
                    { "lastModifiedBy", contact["lastModifiedBy"] },
                    { "lastModifiedDate", contact["lastModifiedDate"] },
                    { "company_id", contact["compnay_id"] },
                    { "name", contact["name"] },
                    { "displayName", contact["displayName"] },
                    { "salutation", contact["salutation"] },
                    { "firstName", contact["firstName"] },
                    { "middleName", contact["middleName"] },
                    { "lastName", contact["lastName"] },
                    { "nickName", contact["nickName"] },
                    { "suffix", contact["suffix"] },
                    { "gender_id", contact["gender_id"] },
                    { "businessUnit_id", contact["businessUnit_id"] },
                    { "dateOfBirth", contact["dateOfBirth"] },
                    { "contactTypes", contact["contactTypes"] }
                };

                indexOperations.Add(IndexAction.Upload(doc));
                indexOperations.Add(IndexAction.Upload(doc2));
            }

            try
            {
                searchIndexClient.Documents.Index(new IndexBatch(indexOperations));
            }
            catch (IndexBatchException ex)
            {
                // Sometimes when your search service is under load, indexing will fail for some of the
                // documents in the batch.  Depending on your application, you can take compensating 
                // actions like delaying and retrying.
                Console.WriteLine("Failed to index some of the documents: {0}",
                    String.Join(", ", ex.IndexingResults.Where(r => !r.Succeeded).Select(r => r.Key)));
            }
        }

        public static SearchContacts SearchDocuments(string searchText, string filter = null)
        {
            // Search using the supplied searchText and output documents that match.
            try
            {
                var searchParameters = new SearchParameters
                {
                    IncludeTotalResultCount = true
                };

                if (!String.IsNullOrEmpty(filter))
                {
                    searchParameters.Filter = filter;
                }

                searchParameters.Facets = new List<String>() { "locationsId", "locationsDescription", "locationsCombined" };

                var response = searchIndexClient.Documents.Search(searchText, searchParameters);
                return new SearchContacts() { Results = response.Results, Facets = response.Facets, Count = Convert.ToInt32(response.Count) };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed search: {0}", ex.Message.ToString());
                return null;
            }
        }
    }

    public class SearchContacts
    {
        public FacetResults Facets { get; set; }
        public IList<SearchResult> Results { get; set; }
        public int? Count { get; set; }
    }
}
