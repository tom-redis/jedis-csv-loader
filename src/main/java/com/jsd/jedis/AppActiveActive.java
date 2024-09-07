package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;

import com.jsd.utils.*;

import redis.clients.jedis.JedisPooled;

/**
 * Simple Jedis Client
 * This client runs the RedisDataLoader utility which can load data from a CSV
 * file into Redis Hash or JSON objects.
 * It uses JedisPipelining.
 * The JSON loader supports the loading on nested JSON.
 */
public class AppActiveActive {

    public void loadData(String configFile) throws Exception {

        Thread t = new Thread() {
            public void run() {
                try {
                    Properties config = new Properties();
                    config.load(new FileInputStream(configFile));

                    RedisDataLoader redisDataLoader = new RedisDataLoader(configFile);
                    String filePath = config.getProperty("random.def.file");
                    String keyPrefix = config.getProperty("data.key.prefix");
                    int numRecords = Integer.parseInt(config.getProperty("data.record.limit"));

                    RandomDataGenerator dataGenerator = new RandomDataGenerator(filePath);
                    redisDataLoader.loadJSON(keyPrefix, dataGenerator, numRecords);
                    redisDataLoader.close();

                } catch (Exception e) {
                    System.out.println("[AppActiveActive] Exception in loadData()\n" + e);
                }
            }
        };

        t.start();
        System.out.println("[AppActiveActive] Started Load " + configFile);
    }

    public void watchClusters(String configFile1, String configFile2) throws Exception {

        Thread t = new Thread() {
            public void run() {
                try {
                    RedisDataLoader redisDataLoader1 = null;
                    RedisDataLoader redisDataLoader2 = null;
                    JedisPooled jedisPool1 = null;
                    JedisPooled jedisPool2 = null; 
                    System.out.println("[AppActiveActive] Watching Cluster Updates: ");

                    try {
                        redisDataLoader1 = new RedisDataLoader(configFile1);
                        jedisPool1 = redisDataLoader1.getJedisPooled();
                    }
                    catch(Exception e) {
                        System.err.println("[AppActiveActive] Error in watchClusters() For " + configFile1);
                    }

                    try {
                        redisDataLoader2 = new RedisDataLoader(configFile2);
                        jedisPool2 = redisDataLoader2.getJedisPooled();
                    }
                    catch(Exception e) {
                        System.err.println("[AppActiveActive] Error in watchClusters() For " + configFile2);
                    }

                    long clusterOld1 = 0;
                    long clusterOld2 = 0;

                    while (true) {
                        long clusterCurrent2 = (jedisPool2 == null) ? 0 : jedisPool2.dbSize();
                        long clusterCurrent1 = (jedisPool1 == null) ? 0 : jedisPool1.dbSize();
                        
                    
                        System.out.println("[AppActiveActive] Cluster1 Keys: " + clusterCurrent1 + " Cluster2 Keys:"
                                + clusterCurrent2);
                        Thread.sleep(300l);

                        if (clusterCurrent1 == clusterOld1 && clusterCurrent2 == clusterOld2) {
                            break;
                        }

                        clusterOld1 = clusterCurrent1;
                        clusterOld2 = clusterCurrent2;

                    }

                } catch (Exception e) {
                    System.out.println("[AppActiveActive] Error in watchClusters()\n" + e);
                }
            }
        };

        t.start();

    }

    public static void main(String[] args) throws Exception {

        // set the config file
        String configFile1 = "./config-aa1.properties";
        String configFile2 = "./config-aa2.properties";

        AppActiveActive aa = new AppActiveActive();

        aa.watchClusters(configFile1, configFile2);
        aa.loadData(configFile1);
        aa.loadData(configFile2);



    }

}
