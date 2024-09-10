package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

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

    public void loadData(String configFile, RedisDataLoader redisDataLoader) throws Exception {

        Thread t = new Thread() {
            public void run() {
                Properties config = new Properties();

                try {

                    config.load(new FileInputStream(configFile));

                    System.out.println("[AppActiveActive] Started Load " + config.getProperty("cluster.name"));

                    String filePath = config.getProperty("random.def.file");
                    String keyPrefix = config.getProperty("data.key.prefix");
                    int numRecords = Integer.parseInt(config.getProperty("data.record.limit"));

                    RandomDataGenerator dataGenerator = new RandomDataGenerator(filePath);
                    redisDataLoader.loadJSON(keyPrefix, dataGenerator, numRecords);
                    redisDataLoader.close();

                } catch (Exception e) {
                    System.out.println("[AppActiveActive] Exception in loadData() for "
                            + config.getProperty("cluster.name") + "\n" + e);
                }
            }
        };

        t.start();
    }

    public void conflictResolution(Scanner scanner, RedisDataLoader redisDataLoader1, RedisDataLoader redisDataLoader2) {

        

        try {
            System.err.print("Select an initial value: ");
            
            JedisPooled jedisPool1 =  redisDataLoader1.getJedisPooled();
            JedisPooled jedisPool2 =  redisDataLoader2.getJedisPooled();

            String key = "aa:counter";
                       
            jedisPool1.set(key, "" + Long.parseLong(scanner.nextLine()));

            for(int i = 0; i < 5; i++) {
                long incr = ThreadLocalRandom.current().nextLong(1l, 5l);
                System.out.println("Cluster1 Incrementing the counter by " + incr);
                jedisPool1.incrBy(key, incr);
                incr = ThreadLocalRandom.current().nextLong(1l, 5l);
                System.out.println("Cluster2 Incrementing the counter by " + incr);
                jedisPool2.incrBy(key, incr);
            } 

            while(true) {
                String vc1 = jedisPool1.get(key);
                String vc2 = jedisPool2.get(key);
                System.out.println("Cluster1 Value: " + vc1 + " Cluster2 Value:" + vc2);

                if(vc1.equals(vc2)) {
                    break;
                }
            }


            key = "aa:sorted:set";

            jedisPool1.del(key);
            System.err.print("\nEnter a list of values in Cluster 1: ");
            String[] values = scanner.nextLine().split(",");
            for(String item : values) {
                jedisPool1.zincrby(key, 1.0, item);
            }

            System.err.print("Enter a list of values in Cluster 2: ");
            values = scanner.nextLine().split(",");
            for(String item : values) {
                jedisPool2.zincrby(key, 1.0, item);
            }

            Thread.sleep(250l);

            List<String> ss = jedisPool1.zrevrangeByScore(key, Double.MAX_VALUE, 0.0);

            System.err.println("Merged Values of Sorted Set by Count : ");


            for(String s : ss) {
                System.out.print(s + ", ");
            }

            redisDataLoader1.close();
            redisDataLoader2.close();

            
        }
        catch(Exception e) {

        }
    }

    public void watchClusters(String configFile1, String configFile2, RedisDataLoader redisDataLoader1, RedisDataLoader redisDataLoader2) throws Exception {

        Thread t = new Thread() {
            public void run() {
                try {
                    JedisPooled jedisPool1 = null;
                    JedisPooled jedisPool2 = null;
                    System.out.println("[AppActiveActive] Watching Cluster Updates: ");

                    try {
                        jedisPool1 = redisDataLoader1.getJedisPooled();
                    } catch (Exception e) {
                        System.err.println("[AppActiveActive] Error in watchClusters() For " + configFile1);
                    }

                    try {
                        jedisPool2 = redisDataLoader2.getJedisPooled();
                    } catch (Exception e) {
                        System.err.println("[AppActiveActive] Error in watchClusters() For " + configFile2);
                    }

                    long clusterOld1 = 0;
                    long clusterOld2 = 0;

                    while (true) {
                        Thread.sleep(1000l);
                        long clusterCurrent1 = (jedisPool1 == null) ? -1 : jedisPool1.dbSize();
                        long clusterCurrent2 = (jedisPool2 == null) ? -1 : jedisPool2.dbSize();

                        System.out.println("[AppActiveActive] Cluster1 Keys: " + clusterCurrent1 + " Cluster2 Keys:" + clusterCurrent2);


                        if (clusterCurrent1 == clusterOld1 && clusterCurrent2 == clusterOld2) {
                            break;
                        }

                        clusterOld1 = clusterCurrent1;
                        clusterOld2 = clusterCurrent2;

                    }

                    redisDataLoader1.close();
                    redisDataLoader2.close();

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

        RedisDataLoader redisDataLoader1 = new RedisDataLoader(configFile1);
        RedisDataLoader redisDataLoader2 = new RedisDataLoader(configFile2);
        RedisDataLoader redisDataLoader11 = new RedisDataLoader(configFile1);
        RedisDataLoader redisDataLoader22 = new RedisDataLoader(configFile2);

        Scanner scanner = new Scanner(System.in);

        System.err.println("Select a test option:\n[1] Simple Replication\n[2] Active Active Replication\n[3] Conflict Resolution");

        String option = scanner.nextLine();

        if("1".equals(option)) {
            aa.loadData(configFile1, redisDataLoader1);
            aa.watchClusters(configFile1, configFile2, redisDataLoader11, redisDataLoader22); 
        }
        else if("2".equals(option))  {
            aa.loadData(configFile1, redisDataLoader1);
            aa.loadData(configFile2, redisDataLoader2);
            aa.watchClusters(configFile1, configFile2, redisDataLoader11, redisDataLoader22);
        }
        else if("3".equals(option))  {

            aa.conflictResolution(scanner, redisDataLoader1, redisDataLoader2);
        }

        scanner.close();
    }

}
