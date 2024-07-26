package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.search.IndexDefinition;
import redis.clients.jedis.search.IndexOptions;

import redis.clients.jedis.search.Schema;

/**
 * Redis Search Client using Jedis
 *
 */
public class RedisIndexFactory {

    private Pipeline jedisPipeline;
    private JedisPooled jedisPooled;

    public RedisIndexFactory(Pipeline jedisPipeline, JedisPooled jedisPooled) throws Exception {
        this.jedisPipeline = jedisPipeline;
        this.jedisPooled = jedisPooled;
    }

    public RedisIndexFactory(String configFile) throws Exception {
        Properties config = new Properties();
        config.load(new FileInputStream(configFile));

        this.jedisPooled = new JedisPooled(config.getProperty("redis.host", "localhost"),
                Integer.parseInt(config.getProperty("redis.port", "6379")),
                config.getProperty("redis.user", "default"), config.getProperty("redis.password"));

        this.jedisPipeline = this.jedisPooled.pipelined();

        System.out.println("[RedisIndexFactory] Connection Successful PING >> " + jedisPooled.ping());

    }

    public void createIndex(String indexName, String indexDefFile) throws Exception {

        JsonReader rdr = Json.createReader(new FileInputStream(indexDefFile));
        JsonObject indexDefObj = rdr.readObject();

        String indexType = indexDefObj.getString("type");
        String prefix = indexDefObj.getString("prefix");

        // define the schema
        Schema schema = new Schema();

        // loop through the fields
        JsonArray fields = indexDefObj.getJsonArray("schema");

        for (int f = 0; f < fields.size(); f++) {
            JsonObject fieldObj = fields.getJsonObject(f);
            String fieldType = fieldObj.getString("type");
            String fieldName = fieldObj.getString("field");
            String fieldAlias = fieldObj.getString("alias");
            boolean sortable = fieldObj.getBoolean("sortable");

            if ("TAG".equalsIgnoreCase(fieldType)) {
                String separator = ",";
                boolean caseSensitive = false;

                try {

                    fieldObj.getString("separator");
                    caseSensitive = fieldObj.getBoolean("caseSensitive");

                } catch (Exception e) {
                }

                schema.addField(new Schema.TagField(fieldName, separator, caseSensitive, sortable)).as(fieldAlias);
            }
            if ("TEXT".equalsIgnoreCase(fieldType)) {
                double weight = 1.0;
                boolean nostem = fieldObj.getBoolean("nostem", true);
                schema.addField(new Schema.TextField(fieldName, weight, sortable, nostem)).as(fieldAlias);
            } else if ("NUMERIC".equalsIgnoreCase(fieldType)) {
                schema.addSortableNumericField(fieldName).as(fieldAlias);
            }
        }

        // set the options
        IndexDefinition def = new IndexDefinition(IndexDefinition.Type.valueOf(indexType.toUpperCase()))
                .setPrefixes(new String[] { prefix });

        // drop index if exists
        dropIndex(indexName);

        // create the Index
        jedisPipeline.ftCreate(indexName, IndexOptions.defaultOptions().setDefinition(def), schema);
        jedisPipeline.sync();

        System.out.println("[RedisIndexFactory] Indexing Started: " + indexName);

    }

    public void dropIndex(String indexName) {
        try {

            this.jedisPipeline.ftDropIndex(indexName);
            this.jedisPipeline.sync();

            System.out.println("[RedisIndexFactory] Dropped Index");

        } catch (Exception e) {
        }
    }

    public JsonArray getIndexObj(String indexDefFile) throws Exception {
        JsonReader rdr = Json.createReader(new FileInputStream(indexDefFile));
        JsonObject indexDefObj = rdr.readObject();


        // loop through the fields
        return indexDefObj.getJsonArray("schema");
    }

    public double getIndexMetric(String indexName, String attr)  {

        //Response<Map<java.lang.String,java.lang.Object>> resp = this.jedisPooled.ftInfo(indexName);
        //Map<java.lang.String,java.lang.Object> infoMap = resp.get();

        Map<java.lang.String,java.lang.Object> infoMap = this.jedisPooled.ftInfo(indexName);

        String attrVal = (String)infoMap.get(attr);
        
        return Double.parseDouble(attrVal);
    }


    public void monitorIndex(String indexName, AppSearch appSearch) throws Exception {

        Thread anonymousThread = new Thread() {
            @Override
            public void run()  {
                try{
                while(getIndexMetric(indexName, "percent_indexed") < 1.0) {
                    Thread.sleep(1000l);
                }

                System.err.println("[RedisIndexFactory] Index Completed");;
            }
            catch(Exception e) {}
            }
        };

        anonymousThread.start(); 
    }



    public static void main(String[] args) throws Exception {

        RedisIndexFactory indexFactory = new RedisIndexFactory("./config.properties");
        indexFactory.createIndex("idx:support", "./index-def-support.json");

    }
}
