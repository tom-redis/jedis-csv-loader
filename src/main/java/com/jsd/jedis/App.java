package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;

import com.jsd.utils.*;

import redis.clients.jedis.*;

import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import redis.clients.jedis.csc.Cache;

/**
 * Simple Jedis Client
 * This client runs the RedisDataLoader utility which can load data from a CSV
 * file into Redis Hash or JSON objects.
 * It uses JedisPipelining.
 * The JSON loader supports the loading on nested JSON.
 */
public class App {

    public static void main(String[] args) throws Exception {

        //set the DNS cache timeout to facilitate faster failover to replica
        java.security.Security.setProperty("networkaddress.cache.ttl", "2");


        Properties config = new Properties();

        Scanner s = new Scanner(System.in);

        // set the config file
        String configFile = "./config.properties";

        System.out.print("\nEnter the config file path (Defaults to ./config.properties): ");
        String configFile1 = s.nextLine();

        if (!"".equals(configFile1)) {
            configFile = configFile1;
        }

        config.load(new FileInputStream(configFile));
        RedisDataLoader redisDataLoader = new RedisDataLoader(configFile);

        int numRecords = Integer.parseInt(config.getProperty("data.record.limit"));

        String keyPrefix = config.getProperty("data.key.prefix");

        String recordType = config.getProperty("data.record.type", "JSON");

        // load into JSON
        String keyType = "header"; // header or random

        System.err.print("\nChoose Option:\n[1] Generate Random Data\n[2] Load File Data\n[3] HA - Failover\n[4] Client-Side Caching\nSelect: ");

        String option = s.nextLine();

        if ("1".equalsIgnoreCase(option)) {
            // RANDOM DATA
            String randomFilePath = config.getProperty("random.def.file");
            RandomDataGenerator dataGenerator = new RandomDataGenerator(randomFilePath);

            System.out.print("Single Batch or Continuous Load(s/c): ");

            String loadType = s.nextLine();

            int batchSize = 1;

            if("c".equalsIgnoreCase(loadType)) {
                numRecords = 10000;
                batchSize = 300;
            }

            for(int b = 0; b < batchSize; b++)  {
                if ("HASH".equalsIgnoreCase(recordType)) {
                    redisDataLoader.loadHash(keyPrefix, dataGenerator, numRecords);
                } else {
                    redisDataLoader.loadJSON(keyPrefix, dataGenerator, numRecords);
                }

                Thread.sleep(1000l);
            }

        } else if ("2".equalsIgnoreCase(option)) {
            // FILE DATA
            String headerID = config.getProperty("data.header.field");
            String detailID = config.getProperty("data.detail.field");
            String detailName = config.getProperty("data.detail.attr.name");
            String filePath = config.getProperty("data.file");
            CSVScanner scanner = new CSVScanner(filePath, ",", false);
            // redisDataLoader.loadHash(keyPrefix, headerID, scanner);
            redisDataLoader.loadJSON(keyPrefix, keyType, headerID, detailID, detailName, scanner, 0);

        } 
        else  if ("3".equalsIgnoreCase(option)) {
            writeFailover(configFile);
        }
        else {
            // CLIENT-SIDE CACHING

            // find the first key, the random generator adds the uts to the key prefix
            ScanParams scanParams = new ScanParams().count(10)
                    .match(config.getProperty("client.cache.key.prefix") + "*"); // Set the chunk size
            String cursor = ScanParams.SCAN_POINTER_START;

            JedisPooled jedisPooled = redisDataLoader.getJedisPooled();
            Cache clientCache = redisDataLoader.getClientCache();

            ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
            String keyPrefix0 = scanResult.getResult().get(0);


            keyPrefix0 = keyPrefix0.substring(0, keyPrefix0.indexOf("-") + 1);

            int cacheSize = Integer.parseInt(config.getProperty("client.cache.size"));

            while(true) {

                System.out.print("Read Records? (y/n): ");

                option = s.nextLine();

                if("n".equalsIgnoreCase(option)) {
                    break;
                }

                long startTime = System.currentTimeMillis();

                for (int k = 0; k < cacheSize; k++) {
                    jedisPooled.jsonGet(keyPrefix0 + k);
                }

                long endTime = System.currentTimeMillis();

                System.out.println("[App] Read Time ms : " + (endTime - startTime));
                System.out.println("[App] Client Cache Size : " + clientCache.getSize());
                System.out.println("[App] Client Cache Stats : " + clientCache.getAndResetStats());
            }


        }

        redisDataLoader.close();

        s.close();

    }

    public static void writeFailover(String configFile) throws Exception {

        Thread t = new Thread() {
            public void run() {

                Properties config = new Properties();
                RedisDataLoader redisDataLoader = null;

                Pipeline pipeline = null;

                try {

                    try {
                        redisDataLoader = new RedisDataLoader(configFile);
                        pipeline = redisDataLoader.getJedisPipeline();
                    } catch (Exception e) {
                    }

                    config.load(new FileInputStream(configFile));

                    String filePath = config.getProperty("random.def.file");

                    RandomDataGenerator dataGenerator = new RandomDataGenerator(filePath);

                    int numBatches = 100;
                    int batchSize = 1000;

                    boolean reconnect = false;

                    for (int batch = 0; batch < numBatches; batch++) {
                        try {

                            if (reconnect) {
                                // establish a new connection
                                redisDataLoader = new RedisDataLoader(configFile);
                                pipeline = redisDataLoader.getJedisPipeline();
                                reconnect = false;
                            }

                            for (int r = 0; r < batchSize; r++) {
                                pipeline.jsonSet("Batch-" + batch + ":Record-" + r,
                                        dataGenerator.generateRecord("header"));
                            }

                            pipeline.sync();
                        } catch (Exception e1) {
                            System.err.println("[App] Connection Failure; Retrying Connection: ");
                            System.err.println("[App] Re-Writing Batch : " + batch);
                            batch--;
                            reconnect = true;
                            Thread.sleep(5000l);
                        }

                        Thread.sleep(1000l);
                    }

                    System.out.println("[App] Successfully Written " + (numBatches * batchSize) + " Records");

                } catch (Exception e) {
                    System.out.println("[App] Exception in writeFailover() for\n" + e);
                }
            }
        };

        t.start();
    }

}
