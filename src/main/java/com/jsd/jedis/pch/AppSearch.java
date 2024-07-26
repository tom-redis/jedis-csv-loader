package com.jsd.jedis.pch;

import java.io.FileInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.Json;

import org.json.JSONArray;
import org.json.JSONObject;

import com.jsd.jedis.*;


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
        String configFile = "./config-pch.properties";

        Properties config = new Properties();
        config.load(new FileInputStream(configFile));

        RedisDataLoader redisDataLoader = new RedisDataLoader(configFile);
        Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();

        String indexName = config.getProperty("index.name");
        String indexDefFile = config.getProperty("index.def.file");

        RedisIndexFactory indexFactory = new RedisIndexFactory(configFile);

        Scanner s = new Scanner(System.in);

        System.out.println("\nWould you like reload data: " + indexName + " ? (y/n)");

        String loadData = s.nextLine();

        if ("y".equalsIgnoreCase(loadData)) {

            String prefix = config.getProperty("data.key.prefix");

            // DROP the INDEX
            indexFactory.dropIndex(indexName);

            // DELETE existing KEYS
            System.out.println("[AppSearch] Deleting Existing Keys");
            System.out.println("[AppSearch] Deleted " + redisDataLoader.deleteKeys(prefix) + " Keys");

            // LOAD JSON file from S3
            JsonObject surveyFileObj = Json.createReader(new FileInputStream(config.getProperty("data.file")))
                    .readObject();

            String bodyField = surveyFileObj.getString("body");

            // PROCESS Records
            JSONArray surveyArray = processRecords(bodyField); 

            
            int numRecords = Integer.parseInt(config.getProperty("data.record.limit", "0"));
            int counter = 0;

            //LOAD DATA
            for (int i = 0; i < surveyArray.length(); i++) {
                JSONObject surveyObj = (JSONObject) surveyArray.get(i);
                jedisPipeline.jsonSet(
                        config.getProperty("data.key.prefix") + counter + "-" + surveyObj.getString("survey_id"),
                        surveyObj);
                counter++;
            }

            // REPLICATE RECORDS to get a Larger Data Set for POC purposes only
            while (counter <= numRecords) {
                for (int i = 0; i < surveyArray.length(); i++) {
                    JSONObject surveyObj = (JSONObject) surveyArray.get(i);
                    surveyObj.put("is_live", "false");
                    jedisPipeline.jsonSet(
                            config.getProperty("data.key.prefix") +  counter + "-" + surveyObj.getString("survey_id"),
                            surveyObj);
                    counter++;
                }
            }

            jedisPipeline.sync();

            System.out.println("[PCH AppSearch] Loaded Survey Data " + counter);

        }

        System.out.println("\nWould you like to create index: " + indexName + " ? (y/n)");

        String createIndex = s.nextLine();

        if ("y".equalsIgnoreCase(createIndex)) {

            // CREATE the INDEX
            indexFactory.createIndex(indexName, indexDefFile);

        }

        // PRINT INDEX SCHEMA FOR REF
        System.out.println(printIndexSchema(indexFactory.getIndexObj(indexDefFile), indexName));

        // START SEARCHING

        while (true) {

            System.out.println(
                    "\n[----------------------------------------------------------------------------]");
            System.out.println("Enter Search String :");

            String queryStr = s.nextLine();

            if ("bye|quit".indexOf(queryStr) > -1) {
                break;
            }

            Query q = new Query(queryStr);
            q.dialect(Integer.parseInt(config.getProperty("query.dialect", "1")));
            q.limit(0, Integer.parseInt(config.getProperty("query.record.limit", "10")));
            

            try {
                Response<SearchResult> res0 = jedisPipeline.ftSearch(indexName, q);
                jedisPipeline.sync();
                SearchResult searchResult = res0.get();

                System.out.println("Number of matchning surveys: " + searchResult.getTotalResults());
                List<Document> docs = searchResult.getDocuments();

                // loop through the results
                for (Document doc : docs) {
                    JSONObject obj = new JSONObject((String) doc.get("$"));
                    obj.isEmpty();

                    // print the keys for each result object
                    System.out.print(doc.getId() + " |");
                }

            } catch (Exception e) {
                System.out.println("[AppSearch] ERROR in Query Execution, Please Try Again :");
                System.out.println(e.toString());
                continue;
            }
        }

        System.out.print("BATCH MODE: Enter number of queries : ");
        int numQueries = Integer.parseInt(s.nextLine());

        runSprint(redisDataLoader, numQueries, indexName, config.getProperty("query.file"));

        s.close();
        redisDataLoader.close();

    }

    public static String printIndexSchema(JsonArray indexArray, String indexName) {

        String schemaString = "SCHEMA: " + indexName + "\n";
        HashMap<String, ArrayList<String>> fieldMap = new HashMap<String, ArrayList<String>>();

        for(int i = 0; i < indexArray.size(); i++) {
            JsonObject fieldObj = (JsonObject)indexArray.get(i);

            String fieldType = fieldObj.getString("type");
            String fieldName = fieldObj.getString("alias");

            if(fieldMap.get(fieldType) == null) {
                ArrayList<String> al = new ArrayList<String>();
                al.add(fieldName);
                fieldMap.put(fieldType, al);
            }
            else {
                fieldMap.get(fieldType).add(fieldName);
            }
        }

        for(String type : new String[] {"TEXT", "TAG", "NUMERIC"}) {
            ArrayList<String> fl = fieldMap.get(type);

            schemaString = schemaString + type + ": ";

            for(String field : fl) {
                schemaString = schemaString + field + " | ";
            }

            schemaString = schemaString + "\n";
        }

        return schemaString;
    }

    private static JSONArray processRecords(String objString) {

        JSONArray arrayObj = new JSONArray(objString);

        for (int i = 0; i < arrayObj.length(); i++) {

            try {
                // set numeric ID fields to Strings
                JSONObject surveyObj = (JSONObject) arrayObj.get(i);
                surveyObj.put("survey_id", "" + surveyObj.getInt("survey_id"));
                surveyObj.put("is_live", "" + surveyObj.getBoolean("is_live"));
                surveyObj.put("collects_pii", "" + surveyObj.getBoolean("collects_pii"));

                //Survey Qualifications
                JSONArray qualArray = surveyObj.getJSONArray("survey_qualifications");

                for (int q = 0; q < qualArray.length(); q++) {
                    JSONObject qualObject = (JSONObject)qualArray.get(q);

                    qualObject.put("question_id", "" + qualObject.getInt("question_id"));
                }

                //Survey Quota
                JSONArray quotaArray = surveyObj.getJSONArray("survey_quotas");

                for (int q = 0; q < quotaArray.length(); q++) {
                    JSONObject quotaObj = (JSONObject)quotaArray.get(q);

                    quotaObj.put("survey_quota_id", "" + quotaObj.getInt("survey_quota_id"));

                    JSONArray quotaQuestionArray = quotaObj.getJSONArray("questions");

                    for(int r = 0; r < quotaQuestionArray.length(); r++) {
                        JSONObject qqObj = (JSONObject)quotaQuestionArray.get(r);
                        qqObj.put("question_id", "" + qqObj.getInt("question_id"));
                    }
                }

            } catch (Exception e) {

            }
        }

        return arrayObj;
    }

    public static void runSprint(RedisDataLoader redisDataLoader, int numQueries, String indexName, String queryFile) throws Exception {

        Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();

        //LOAD SAMPLE QUERIES

        Scanner fileScanner = new Scanner(new File(queryFile));

        ArrayList<String> baseQueryList = new ArrayList<String>();

        while(fileScanner.hasNextLine()) {
            baseQueryList.add(fileScanner.nextLine());
        }

        System.out.println("[AppSearch] Executing " + numQueries + " Queries");
        long startTime = System.currentTimeMillis();


        // EXECUTE QUERIES

        ArrayList<String> queryList = new ArrayList<String>();
        ArrayList<Response<SearchResult>> resultList = new ArrayList<Response<SearchResult>>();

        for (int q = 0; q < numQueries; q++) {

            int rand = ThreadLocalRandom.current().nextInt(0, baseQueryList.size());

            //pick a random query to execute
            String queryStr = baseQueryList.get(rand);

            Query q1 = new Query(queryStr);
            q1.limit(0, 10);
            q1.dialect(4);
            q1.setNoContent();

            queryList.add(queryStr);
            resultList.add(jedisPipeline.ftSearch(indexName, q1));

        }

        jedisPipeline.sync();

        long endTime = System.currentTimeMillis();
        System.out.println("[AppSearch] Query Execution Complete in " + getExecutionTime(startTime, endTime));

        // PRINT RESULTS

        for (int r = 0; r < resultList.size(); r++) {
            System.out.print("[" + (r + 1) + "] " + queryList.get(r) + "\n   **RESULTS** ");
            Response<SearchResult> res0 = resultList.get(r);
            SearchResult searchResult = res0.get();

            List<Document> docs = searchResult.getDocuments();

            //System.out.println(" #Keys: " + searchResult.getTotalResults());

            int counter = 0;
            for (Document doc : docs) {
                System.out.print(doc.getId() + " | ");

                if (counter++ == 2) {
                    break;
                }
            }

            System.out.println(" ...");
            
        }

        fileScanner.close();

        System.out.println("===================================================================================");
        System.out.println("[AppSearch] Query Execution Complete in " + getExecutionTime(startTime, endTime));

    }





    static String getExecutionTime(long startTime, long endTime) {
        long diff = endTime - startTime;

        long sec = Math.floorDiv(diff, 1000l);
        long msec = diff % 1000l;

        return "" + sec + " s : " + msec + " ms";
    }

}
