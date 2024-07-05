package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONObject;

import com.jsd.utils.*;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.search.Document;
import redis.clients.jedis.search.FtSearchIteration;

/**
 * Redis Search Client using Jedis
 *
 */
public class AppSearch {

    public static void main(String[] args) throws Exception {

        //set the config file
        String configFile = "./config.properties";

        Properties config = new Properties();
        config.load(new FileInputStream(configFile));

        RedisDataLoader redisDataLoader = new RedisDataLoader(configFile);
        Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();
        
        String indexName = config.getProperty("index.name");
        String indexDefFile = config.getProperty("index.def.file");

        RedisIndexFactory indexFactory = new RedisIndexFactory(configFile);

        Scanner s = new Scanner(System.in);
        System.out.println("\nWould you like to Reload Data for: " + indexName + " ? (y/n)");
        
        String loadData = s.nextLine();

        if ("y".equalsIgnoreCase(loadData)) {

            String prefix = config.getProperty("key.prefix");

            //drop the index
            indexFactory.dropIndex(indexName);

            //delete existing keys
            System.out.println("[AppSearch] Deleting Existing Keys");
            System.out.println("[AppSearch] Deleted " + redisDataLoader.deleteKeys(prefix) + " Keys");

            // load data as JSON
            CSVScanner scanner = new CSVScanner(config.getProperty("data.file"), ",", false);
            redisDataLoader.loadJSON(prefix, "header", config.getProperty("header.field"), 
                                             config.getProperty("detail.field"),
                                             config.getProperty("detail.attr.name"),
                                            scanner, Integer.parseInt(config.getProperty("data.record.limit")));

            // Creating the index
            indexFactory.createIndex(indexName, indexDefFile);

        }

        // perform a searh

        while (true) {

            System.out.println("\n==========================================================================================");
            System.out.println("Enter Search String :");

            String queryStr = s.nextLine();

            if ("bye|quit".indexOf(queryStr) > -1) {
                break;
            }

 
            Query q = new Query(queryStr);
            q.dialect(2);
            q.limit(0, Integer.parseInt(config.getProperty("query.record.limit", "10")));


            Response<SearchResult> res0 = jedisPipeline.ftSearch(indexName, q);
            jedisPipeline.sync();
            SearchResult res = res0.get();

            System.out.println("Number of results: " + res.getTotalResults());
            List<Document> docs = res.getDocuments();

            // loop through the results
            for (Document doc : docs) {
                JSONObject obj = new JSONObject((String) doc.get("$"));

                System.out.print(doc.getId() + " |");
            }
        }


        s.close();
        redisDataLoader.close();

    }


    public static void  runSprint(Pipeline jedisPipeline) {
        System.out.println("Running a Work Load of 500 Queries");
        ArrayList<Response<SearchResult>> resultList = new ArrayList<Response<SearchResult>>();

        for (int i = 100; i < 500; i++) {
            String id = "DEP00" + i;

            String queryStr1 = "(@SubscriberUniqueID:{" + id + "})|(@SubscriberSSN:{" + id
                    + "})|(@DependantSSN:{" + id + "})|(@DependantUniqueID:{" + id + "})";

            Query q1 = new Query(queryStr1);
            q1.limit(0, 10);

            resultList.add(jedisPipeline.ftSearch("idx:healthcare", q1));
        }

        jedisPipeline.sync();

        System.out.println("Query Execution Complete");

        for( Response<SearchResult> res0 : resultList) {
            Document doc = res0.get().getDocuments().get(0);

            JSONObject obj = new JSONObject((String) doc.get("$"));
            System.out.print(obj.getString("SubName") + " " + obj.getString("SubscriberSSN") + "|");
           
        }
    }

    public static void formatResult(String patientID, JSONObject obj) {
        // check for sub or dependant
        if (patientID.equalsIgnoreCase(obj.getString("SubscriberSSN")) ||
                patientID.equalsIgnoreCase(obj.getString("SubscriberUniqueID"))) {
            System.out.println("This is a subscriber:");
            System.out.println(obj.getString("SubscriberUniqueID") + " | " + obj.getString("SubscriberSSN") + " | "
                    + obj.getString("SubName")
                    + " | " + obj.getString("SubscriberDOB") + " | " + obj.getString("SubscriberBalAmount1")
                    + " | " + obj.getString("SubscriberBalAmount2") + " | " + obj.getString("SubscriberBalAmount3")
                    + " | " + obj.getString("SubscriberBalAmount4"));
        } else {
            System.out.println("This is a dependant:");
            System.out.println(obj.getString("SubscriberUniqueID") + " | " + obj.getString("SubscriberSSN") + " | "
                    + obj.getString("SubName")
                    + " | " + obj.getString("SubscriberDOB") + " | " + obj.getString("SubscriberBalAmount1")
                    + " | " + obj.getString("SubscriberBalAmount2") + " | " + obj.getString("SubscriberBalAmount3")
                    + " | " + obj.getString("SubscriberBalAmount4"));

            JSONArray depArray = obj.getJSONArray("dependants");

            for (int d = 0; d < depArray.length(); d++) {
                JSONObject dep = (JSONObject) depArray.get(d);

                if (patientID.equalsIgnoreCase(dep.getString("DependantSSN"))
                        || patientID.equalsIgnoreCase(dep.getString("DependantUniqueID"))) {
                    System.out.println(dep.getString("DependantUniqueID") + " | " + dep.getString("DependantSSN")
                            + " | " + dep.getString("Dependant Name")
                            + " | " + dep.getString("DependantDOB") + " | " + dep.getString("DependantBalAmount1")
                            + " | " + dep.getString("DependantBalAmount2") + " | "
                            + dep.getString("DependantBalAmount3")
                            + " | " + dep.getString("DependantBalAmount4"));

                    break;
                }
            }
        }
    }

}
