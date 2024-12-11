package com.jsd.jedis;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.jsd.utils.*;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.MultiClusterClientConfig;
import redis.clients.jedis.MultiClusterClientConfig.ClusterConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.MultiClusterPooledConnectionProvider;

/**
 * Simple Jedis Client
 * This client runs the RedisDataLoader utility which can load data from a CSV
 * file into Redis Hash or JSON objects.
 * It uses JedisPipelining.
 * The JSON loader supports the loading on nested JSON.
 */
public class AppActiveActive {

    public void writeFailover(String configFile1, String configFile2) throws Exception {

        Thread t = new Thread() {
            public void run() {

                Properties config1 = new Properties();
                Properties config2 = new Properties();
                RedisDataLoader redisDataLoader = null;
                
                Pipeline pipeline = null;

                try {

                    try {
                        redisDataLoader = new RedisDataLoader(configFile1);
                        pipeline = redisDataLoader.getJedisPipeline();
                    }
                    catch(Exception e) {}
                    

                    config1.load(new FileInputStream(configFile1));
                    config2.load(new FileInputStream(configFile2));

                    System.out.println("[AppActiveActive] Started Load " + config1.getProperty("cluster.name"));

                    String filePath = config1.getProperty("random.def.file");
                    String keyPrefix = config1.getProperty("data.key.prefix");
                    
                    RandomDataGenerator dataGenerator = new RandomDataGenerator(filePath);
                    keyPrefix = config1.getProperty("data.key.prefix.burst");

                    
                    for (int batch = 0; batch < 900; batch++) {
                        String sysTime = "" + System.currentTimeMillis();

                        try {
                            for (int r = 0; r < 500; r++) {
                                pipeline.jsonSet(keyPrefix + "Batch-" + batch + ":Record-" + r, dataGenerator.generateRecord("header"));
                            }
                            pipeline.sync();
                        }
                        catch(Exception e1) {
                            System.err.println("[AppActiveActive] Cluster Connection Failure: " + config1.getProperty("cluster.name"));
                            System.err.println("[AppActiveActive] Failover to Cluster2: " + config2.getProperty("cluster.name"));
                            System.err.println("[AppActiveActive] Re-Writing Batch : " + batch);
                            batch--;
                            redisDataLoader = new RedisDataLoader(configFile2);
                            pipeline = redisDataLoader.getJedisPipeline();
                        }

                        Thread.sleep(3000l);
                    }

                } catch (Exception e) {
                    System.out.println("[AppActiveActive] Exception in writeFailover() for "
                            + config1.getProperty("cluster.name") + "\n" + e);
                }
            }
        };

        t.start();
    }

    public void loadData(String configFile, RedisDataLoader redisDataLoader, String mode, String objType)
            throws Exception {

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

                    if ("burst".equalsIgnoreCase(mode)) {
                        keyPrefix = config.getProperty("data.key.prefix.burst");

                        for (int i = 0; i < 900; i++) {
                            if ("JSON".equalsIgnoreCase(objType)) {
                                redisDataLoader.loadJSON(keyPrefix, dataGenerator, 500);
                            } else {
                                redisDataLoader.loadHash(keyPrefix, dataGenerator, 500);
                            }

                            Thread.sleep(1000l);
                        }
                    } else {
                        if ("JSON".equalsIgnoreCase(objType)) {
                            redisDataLoader.loadJSON(keyPrefix, dataGenerator, numRecords);
                        } else {
                            redisDataLoader.loadHash(keyPrefix, dataGenerator, numRecords);
                        }

                    }

                    redisDataLoader.close();

                } catch (Exception e) {
                    System.out.println("[AppActiveActive] Exception in loadData() for "
                            + config.getProperty("cluster.name") + "\n" + e);
                }
            }
        };

        t.start();
    }

    public void conflictResolution(Scanner scanner, RedisDataLoader redisDataLoader1,
            RedisDataLoader redisDataLoader2) {

        try {
            System.err.print("[Counter] Select an initial value: ");

            JedisPooled jedisPool1 = redisDataLoader1.getJedisPooled();
            JedisPooled jedisPool2 = redisDataLoader2.getJedisPooled();

            String key = "aa:counter";

            jedisPool1.set(key, "" + Long.parseLong(scanner.nextLine()));

            for (int i = 0; i < 3; i++) {
                long incr = ThreadLocalRandom.current().nextLong(1l, 5l);
                System.out.println("Cluster1 Incrementing the counter by " + incr);
                jedisPool1.incrBy(key, incr);
                incr = ThreadLocalRandom.current().nextLong(1l, 5l);
                System.out.println("Cluster2 Incrementing the counter by " + incr);
                jedisPool2.incrBy(key, incr);
            }

            while (true) {
                String vc1 = jedisPool1.get(key);
                String vc2 = jedisPool2.get(key);
                System.out.println("Cluster1 Value: " + vc1 + " Cluster2 Value:" + vc2);

                if (vc1.equals(vc2)) {
                    break;
                }
            }

            key = "aa:sorted:set";

            jedisPool1.del(key);
            System.err.print("\n[Sorted Set] Enter a list of values in Cluster 1: ");
            String[] values = scanner.nextLine().split(",");
            for (String item : values) {
                jedisPool1.zincrby(key, 1.0, item);
            }

            System.err.print("[Sorted Set] Enter a list of values in Cluster 2: ");
            values = scanner.nextLine().split(",");
            for (String item : values) {
                jedisPool2.zincrby(key, 1.0, item);
            }

            Thread.sleep(200l);

            List<String> ss = jedisPool1.zrevrangeByScore(key, Double.MAX_VALUE, 0.0);

            System.err.println("[Sorted Set] Merged Values : ");

            for (String s : ss) {
                System.out.print(s + ", ");
            }

            redisDataLoader1.close();
            redisDataLoader2.close();

        } catch (Exception e) {

        }
    }

    public void watchClusters(String configFile1, String configFile2, RedisDataLoader redisDataLoader1,
            RedisDataLoader redisDataLoader2) throws Exception {

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

                        System.out.println("[AppActiveActive] Cluster1 Keys: " + clusterCurrent1 + " Cluster2 Keys:"
                                + clusterCurrent2);

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

    public void loadSets(String configFile, RedisDataLoader redisDataLoader) throws Exception {

        Thread t = new Thread() {
            public void run() {
                Properties config = new Properties();

                try {

                    config.load(new FileInputStream(configFile));

                    int numRecords = Integer.parseInt(config.getProperty("data.record.limit"));

                    Pipeline jedisPipeline = redisDataLoader.getJedisPipeline();

                    System.out.println("[AppActiveActive] Loading Sets: " + numRecords);

                    String batch = "" + System.currentTimeMillis();

                    // creating base sets
                    for (int r = 1; r <= numRecords; r++) {
                        int randLength = ThreadLocalRandom.current().nextInt(1, 800);
                        for (int i = 0; i < randLength; i++) {
                            jedisPipeline.sadd("sets:Set-" + batch + "-" + r, "Member-" + r + "-" + i);

                        }
                    }

                    jedisPipeline.sync();

                    redisDataLoader.close();

                } catch (Exception e) {
                    System.out.println("[AppActiveActive] Exception in loadSets() for "
                            + config.getProperty("cluster.name") + "\n" + e);
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

        RedisDataLoader redisDataLoader1 = null;
        RedisDataLoader redisDataLoader2 = null;
        RedisDataLoader redisDataLoader11 = null;
        RedisDataLoader redisDataLoader22 = null;

        Scanner scanner = new Scanner(System.in);

        System.err.print("Select a test option:\n[1] Simple Replication\n[2] Active Active Replication\n[3] Conflict Resolution\n[4] Continuous Ingestion\n[5] Replica Failover\n> ");

        String option = scanner.nextLine();

        System.err.print("Object Type [JSON/HASH] (default JSON): ");

        String objectType = scanner.nextLine();

        if ("".equals(objectType)) {
            objectType = "JSON";
        }

        if ("1".equals(option)) {

            redisDataLoader1 = new RedisDataLoader(configFile1);
            redisDataLoader11 = new RedisDataLoader(configFile1);
            redisDataLoader22 = new RedisDataLoader(configFile2);

            aa.loadData(configFile1, redisDataLoader1, "batch", objectType);
            aa.watchClusters(configFile1, configFile2, redisDataLoader11, redisDataLoader22);

        } else if ("2".equals(option)) {

            redisDataLoader1 = new RedisDataLoader(configFile1);
            redisDataLoader2 = new RedisDataLoader(configFile2);
            redisDataLoader11 = new RedisDataLoader(configFile1);
            redisDataLoader22 = new RedisDataLoader(configFile2);

            aa.loadData(configFile1, redisDataLoader1, "batch", objectType);
            aa.loadData(configFile2, redisDataLoader2, "batch", objectType);
            aa.watchClusters(configFile1, configFile2, redisDataLoader11, redisDataLoader22);

        } else if ("3".equals(option)) {

            redisDataLoader1 = new RedisDataLoader(configFile1);
            aa.conflictResolution(scanner, redisDataLoader1, redisDataLoader2);

        } else if ("4".equals(option)) {
            redisDataLoader1 = new RedisDataLoader(configFile1);
            aa.loadData(configFile1, redisDataLoader1, "burst", objectType);
        } 
        else if ("5".equals(option)) {
            aa.writeFailover(configFile1, configFile2);
        }
        else {
            aa.loadSets(configFile1, redisDataLoader1);
        }

        scanner.close();
    }

}
