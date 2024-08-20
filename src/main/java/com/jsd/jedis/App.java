package com.jsd.jedis;


import java.io.FileInputStream;
import java.util.Properties;




import com.jsd.utils.*;




/**
 * Simple Jedis Client
 * This client runs the RedisDataLoader utility which can load data from a CSV file into Redis Hash or JSON objects.
 * It uses JedisPipelining.
 * The JSON loader supports the loading on nested JSON.
 */
public class App {



    public static void main(String[] args) throws Exception {

        Properties config = new Properties();

        String configFile = "./config-load.properties";

        config.load(new FileInputStream(configFile));
        RedisDataLoader redisDataLoader = new RedisDataLoader(configFile);

        String filePath =  config.getProperty("data.file");
        String keyPrefix = config.getProperty("data.key.prefix");


        CSVScanner scanner = new CSVScanner(filePath,",",false);

        //load into hash
        //redisDataLoader.loadHash("hash:support:tickets:", "TicketID", scanner);

        //load into JSON
        String keyType = "header"; // header or random

        redisDataLoader.loadJSON(keyPrefix, keyType, "SubscriberUniqueID", "DependantUniqueID", "dependants", scanner, 0);
        //redisDataLoader.loadJSON(keyPrefix, keyType, "TicketID", "NA", "NA", scanner, 0);

        redisDataLoader.close();

    }
    
}
