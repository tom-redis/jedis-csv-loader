package com.jsd.jedis;


import java.util.HashSet;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.JedisPubSub;

public class AppPubSub {

 
    HashSet<String> keys = new HashSet<>();

    String keyPrefix;
    String lastKey;

    int eventCount = 0;


    public AppPubSub(String configFile) throws Exception {
        RedisDataLoader dataLoader = new RedisDataLoader(configFile);
        JedisPooled jedisPooled = dataLoader.getJedisPooled();

        // Subscriber definition
        JedisPubSub jedisPubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                System.out.println("Channel: " + channel + " | Message: " + message);
            }

            @Override
            public void onPMessage(String pattern, String channel, String message) {
                addEvent(channel);
                //System.out.println("Pattern: " + pattern + " | Channel: " + channel + " | Message: " + message);
            }
        };

        // Subscribe to keyspace changes on all keys
        new Thread(() -> jedisPooled.psubscribe(jedisPubSub, "__keyspace@0__:*")).start();
    }

    private void addEvent(String event) {
        //System.out.println("[KeySpaceListener] Event " + event);

        if(event.startsWith("__keyspace@0__:" + this.keyPrefix)) {
            keys.add(event);
            eventCount++;
            lastKey = event.substring(15);
            //System.out.println("[KeySpaceListener] Added Event " + keys.size() + " " + event);
        }
    }

    public void trackKeys(int targetKeys, boolean continuousTracking) throws Exception {
        System.out.println("[KeySpaceListener] Tracking Keys");
        Thread t = new Thread() {
            public void run()  {
                int marker = targetKeys/10;
                while(true) {
                    if(keys.size() == targetKeys) {
                        System.out.println("[KeySpaceListener] Detected " + keys.size() + " Keys in Redis");
                        System.out.println("[KeySpaceListener] Last Key Added: " + lastKey);
                        break;
                    }
                    else if(continuousTracking && (keys.size() >= marker)) {
                        marker = marker + (targetKeys/10);
                        System.out.println("[KeySpaceListener] Detected " + keys.size() + " Keys in Redis");
                        System.out.println("[KeySpaceListener] Last Key Added: " + lastKey);                     
                    }  

                    try {Thread.sleep(1000l);} catch(Exception e) {}
                }
            }
        };

        t.start();
    }

    public void setPrefix(String  keyPrefix) {
        this.keyPrefix = keyPrefix;
    }



    public static void main(String[] args) throws Exception {
        AppPubSub ps = new AppPubSub("./config.properties");
        ps.setPrefix("NA");
    }
}

