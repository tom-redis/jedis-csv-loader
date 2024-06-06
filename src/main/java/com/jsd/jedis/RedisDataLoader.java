package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Properties;


import org.json.JSONObject;

import com.jsd.utils.*;

import redis.clients.jedis.*;



/**
 * Simple Jedis Client
 *
 */
public class RedisDataLoader {

    private Pipeline jedisPipeline;
    private JedisPooled jedisPooled;

    public RedisDataLoader(String configFile) throws Exception{
        Properties config = new Properties();
        config.load(new FileInputStream(configFile));

        this.jedisPooled = new JedisPooled(config.getProperty("redis.host", "localhost"), 
                                            Integer.parseInt(config.getProperty("redis.port", "6379")),
                                            config.getProperty("redis.user", "default"), config.getProperty("redis.password"));

        this.jedisPipeline = this.jedisPooled.pipelined();

        System.out.println("[RedisDataLoader] Connection Successful PING >> " + jedisPooled.ping());
          
    }

    public Pipeline getJedisPipeline() {
        return this.jedisPipeline;
    }

    public JedisPooled getJedisPooled() {
        return this.jedisPooled;
    }

    public void close() {
        this.jedisPooled.close();
    }



    public  void loadGEO(String key, CSVScanner csvScanner, String geoCol, String longCol, String latCol, String entity) {

        try {
            int record = 0;
            while (csvScanner.hasMoreRecords()) {
                jedisPipeline.geoadd(key, Double.parseDouble(csvScanner.getColumnValue(longCol)),
                        Double.parseDouble(csvScanner.getColumnValue(latCol)),
                        csvScanner.getColumnValue(entity));

                if (record % 1000 == 0) {
                    jedisPipeline.sync();
                }

                record++;
            }

            jedisPipeline.sync();
            System.out.println("[loadGEO] Successfully Loaded " + --record + " Records into GEO Index");

        } catch (Exception e) {

        }
        return;
    }

    /*
     * Loads a CSV file into Hash objects.
     * The dataset must have a unique identified to serve as the key
     */

    public  void loadHash(String keyPrefix, String idCol, CSVScanner csvScanner) throws Exception {

        try {

            int record = 0;
            int numCols = csvScanner.getNumColumns();
            String[] columnNames = csvScanner.getColumnNames();

            while (csvScanner.hasMoreRecords()) {
            
                //load the record into redis

                try {
                    // load the record in a Map
                    HashMap<String, String> recordMap = new HashMap<String, String>();

                    // set the id column
                    recordMap.put("_id", csvScanner.getColumnValue(idCol));
                
                    //add the other columns
                    for (int c = 0; c < numCols; c++) {
                        if (!columnNames[c].equals(idCol)) {
                            recordMap.put(columnNames[c], csvScanner.getColumnValue(columnNames[c]));   
                        }
                    }

                    jedisPipeline.hset(keyPrefix + csvScanner.getColumnValue(idCol), recordMap);  
                }
                catch(Exception e) {
                    jedisPipeline.del(keyPrefix + csvScanner.getColumnValue(idCol));
                }

                record++;
            }

            //jedisPipeline.sync();

            System.out.println("Successfully Loaded " + --record + " Records into Hashes");

        } catch (Exception e) {
            System.err.println(e);
        }
    }


    /*
     * Loads a CSV file into JSON objects, with upto 1 level of nesting.
     * It assumes the dataset is sorted by the headerID and all columns on and after the detailID are line items of the header
     */

    public  void loadJSON(String keyPrefix, String keyType, String headerID, String detailID, String detailName, CSVScanner csvScanner, int numRows) throws Exception {

        int numCols = csvScanner.getNumColumns();
        String[] columnNames = csvScanner.getColumnNames();
        int record = 1;

        String currentHeader = "NA";
        String prevHeader = "NA";

        JSONObject headerObj = new JSONObject();
        JSONObject detailObj = new JSONObject();
        String keyID = "";

        String sysTime = "" + System.currentTimeMillis();

        while (csvScanner.hasMoreRecords() && (record < numRows || numRows < 1)) {

            currentHeader = csvScanner.getColumnValue(headerID);

            //new header record
            if(!currentHeader.equals(prevHeader) && !"NA".equals(prevHeader)) {

                keyID = ("header".equalsIgnoreCase(keyType)) ? prevHeader : "" + record + "-" + sysTime;

                //write the previous record
                jedisPipeline.jsonSet(keyPrefix + keyID, headerObj);

                //start new header record
                headerObj = new JSONObject();

                record++;
            }

            detailObj = new JSONObject();

            boolean loadingHeader = true;

            // add the other columns
            for (int c = 0; c < numCols; c++) {
                
                if(detailID.equals(columnNames[c])) {
                    loadingHeader = false;
                }

                if(loadingHeader) {
                    headerObj.put(columnNames[c], csvScanner.getColumnValue(c));
                }
                else {
                    detailObj.put(columnNames[c], csvScanner.getColumnValue(c));
                }
            }

            //load details as an array, if the dataset has a 1 - Many relationship
            if(!loadingHeader) {
                headerObj.append(detailName, detailObj);
            }
           

            prevHeader = currentHeader;
        }



        //write the last final record
        keyID = ("header".equalsIgnoreCase(keyType)) ? currentHeader : "" + record + "-" + sysTime;
        jedisPipeline.jsonSet(keyPrefix + keyID, headerObj);

        //flush the pipeline
        jedisPipeline.sync();

        System.out.println("[RedisDataLoader] Loaded " + record + " record objects");

    }

    public static void main(String[] args) throws Exception {

      


    }
}
