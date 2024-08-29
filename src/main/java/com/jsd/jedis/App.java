package com.jsd.jedis;


import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;

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

        Scanner s = new Scanner(System.in);

        // set the config file
        String configFile = "./config.properties";

        System.out.print("\nEnter the config file path (Defaults to ./config.properties): ");
        String configFile1  = s.nextLine();

        if(!"".equals(configFile1)) {
            configFile = configFile1;
        }

        config.load(new FileInputStream(configFile));
        RedisDataLoader redisDataLoader = new RedisDataLoader(configFile);

        String filePath =  config.getProperty("data.file");
        String keyPrefix = config.getProperty("data.key.prefix");


        CSVScanner scanner = new CSVScanner(filePath,",",false);



        //load into JSON
        String keyType = "header"; // header or random

        String headerID = config.getProperty("data.header.field");
        String detailID = config.getProperty("data.detail.field");
        String detailName = config.getProperty("data.detail.attr.name");

        //load into hash
        //redisDataLoader.loadHash(keyPrefix, headerID, scanner);        

        redisDataLoader.loadJSON(keyPrefix, keyType, headerID, detailID, detailName, scanner, 0);


        redisDataLoader.close();

        s.close();

    }
    
}
