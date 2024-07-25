package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.json.JSONObject;

import com.jsd.utils.*;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.search.Document;

/**
 * Redis Search Client using Jedis
 *
 */
public class AppSearch {

    public static void main(String[] args) throws Exception {

        // set the config file
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

            // drop the index
            indexFactory.dropIndex(indexName);

            // delete existing keys
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

            System.out.println(
                    "\n==========================================================================================");
            System.out.println("Enter Search String :");

            String queryStr = s.nextLine();

            if ("bye|quit".indexOf(queryStr) > -1) {
                break;
            }

            Query q = new Query(queryStr);
            q.dialect(2);
            q.limit(0, Integer.parseInt(config.getProperty("query.record.limit", "10")));

            try {
                Response<SearchResult> res0 = jedisPipeline.ftSearch(indexName, queryStr);
                jedisPipeline.sync();
                SearchResult searchResult = res0.get();

                System.out.println("Number of results: " + searchResult.getTotalResults());
                List<Document> docs = searchResult.getDocuments();

                // loop through the results
                for (Document doc : docs) {
                    JSONObject obj = new JSONObject((String) doc.get("$"));

                    //print the keys for each result object
                    System.out.print(doc.getId() + " |");
                }

            } catch (Exception e) {
                System.out.println("[AppSearch] ERROR in Query Syntax, Please Try Again :");
                continue;
            }
        }


        System.out.print("BATCH MODE: Enter number of queries : ");
        int numQueries = Integer.parseInt(s.nextLine());
        
        runSprint(redisDataLoader, numQueries, indexName, indexFactory.getIndexObj(indexDefFile));

        s.close();
        redisDataLoader.close();

    }

    public static void runSprint(RedisDataLoader redisDataLoader, int numQueries, String indexName, JsonArray indexSchema) {

        
        JedisPooled jedisPooled = redisDataLoader.getJedisPooled();
        Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();

        ArrayList<String> tagFieldList = new ArrayList<String>();
        HashMap<String, ArrayList<String>> tagValueMap = new HashMap<String, ArrayList<String>>();

        // loop through the schema, and extract all tag fields and their values.
        for (int i = 0; i < indexSchema.size(); i++) {
            JsonObject fieldObj = indexSchema.getJsonObject(i);

            if ("TAG".equalsIgnoreCase(fieldObj.getString("type"))) {
                String fieldName = fieldObj.getString("alias");
                tagFieldList.add(fieldName);
                tagValueMap.put(fieldName, toArrayList(jedisPooled.ftTagVals(indexName, fieldName)));
            }
        }

        System.out.println("[AppSearch] Executing " + numQueries + " Queries");
        long startTime = System.currentTimeMillis();

        ArrayList<String> queryList = new ArrayList<String>();
        ArrayList<Response<SearchResult>> resultList = new ArrayList<Response<SearchResult>>();
    
        //execute queries
        for (int q = 0; q < numQueries; q++) {

            String queryStr = "";

            ArrayList<String> fieldList = pickRandom(tagFieldList);

            //build query string
            for (int f = 0; f < fieldList.size(); f++) {
                queryStr = queryStr + getFilterClause(fieldList.get(f), pickRandom(tagValueMap.get(fieldList.get(f)))) + " ";
            }

            queryList.add(queryStr);

            Query q1 = new Query(queryStr);
            q1.limit(0, 100);
            q1.dialect(4);
            q1.setNoContent();

            resultList.add(jedisPipeline.ftSearch(indexName, q1));

        }

        jedisPipeline.sync();

        long endTime = System.currentTimeMillis();
        System.out.println("[AppSearch] Query Execution Complete in " + getExecutionTime(startTime, endTime));

        //print results
        for(int r = 0; r < resultList.size(); r++) {
            System.out.print(queryList.get(r) + " #### >>> :");
            Response<SearchResult> res0 = resultList.get(r);
            SearchResult searchResult = res0.get();

            List<Document> docs = searchResult.getDocuments();

            int counter = 0;
            for(Document doc : docs){
                System.out.print(doc.getId() + "|");

                if(counter++ == 3) {
                    break;
                }
            }

            System.out.println(" ...");
            //System.out.println(" #Keys: " + searchResult.getTotalResults());
        }

    }

    private static ArrayList<String> pickRandom(ArrayList<String> superList) {
        ArrayList<String> randomList = new ArrayList<String>();

        int rand = ThreadLocalRandom.current().nextInt(0, superList.size());

        int toggle = ThreadLocalRandom.current().nextInt(0, 3);

        if (toggle == 0) {
            randomList.add(superList.get(rand));
        } else if (toggle == 1) {
            for (int i = 0; i <= rand; i++) {
                randomList.add(superList.get(i));
            }
        } else {
            for (int i = rand; i < superList.size(); i++) {
                randomList.add(superList.get(i));
            }
        }

        return randomList;
    }

    private static ArrayList<String> toArrayList(Set<String> valueSet) {

        ArrayList<String> returnList = new ArrayList<String>();

        for(String val : valueSet) {
            returnList.add(val);
        }

        return returnList;

    }

    private static String getFilterClause(String fieldName , ArrayList<String> valueList) {
        String filter = "";
       
        filter = filter + "@" + fieldName + ":{";

        String valueStr = "";

        for(int v = 0; v < valueList.size(); v++) {
                valueStr = valueStr + valueList.get(v) + ((v < (valueList.size() -1)) ? "|" : "");
        }

        filter = filter + valueStr + "}"; 


        return filter;
    }

    static String getExecutionTime(long startTime, long endTime) {
        long diff = endTime - startTime;

        long sec = Math.floorDiv(diff,1000l);
        long msec = diff % 1000l;

        return "" + sec + " s : " + msec + " ms";
    }

}
