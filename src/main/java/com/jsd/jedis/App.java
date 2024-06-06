package com.jsd.jedis;


import com.jsd.utils.*;




/**
 * Simple Jedis Client
 * This client runs the RedisDataLoader utility which can load data from a CSV file into Redis Hash or JSON objects.
 * It uses JedisPipelining.
 * The JSON loader supports the loading on nested JSON.
 */
public class App {


    public static void main(String[] args) throws Exception {

        //disable check for self signed certs when using TLS
        System.setProperty("jdk.internal.httpclient.disableHostnameVerification", "true");

        RedisDataLoader redisDataLoader = new RedisDataLoader("./config.properties");

        CSVScanner subscriptions = new CSVScanner("C:/Users/Jay Datsur/OneDrive/Tech/Redis/DataSets/subscriptions.csv",",",false);
        CSVScanner tickets = new CSVScanner("C:/Users/Jay Datsur/OneDrive/Tech/Redis/DataSets/support_tickets_demo.csv",",",false);


        //load into hash
        redisDataLoader.loadHash("support:tickets:", "TicketID", tickets);

        //load into JSON
        redisDataLoader.loadJSON("healthcare:subscriptions:","random", "SubscriberUniqueID", "DependantUniqueID", "dependants", subscriptions, 10);


        redisDataLoader.close();

    }
    
}
