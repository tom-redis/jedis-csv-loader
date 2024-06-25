package com.jsd.jedis;


import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;


import com.jsd.utils.*;




/**
 * Simple Jedis Client
 * This client runs the RedisDataLoader utility which can load data from a CSV file into Redis Hash or JSON objects.
 * It uses JedisPipelining.
 * The JSON loader supports the loading on nested JSON.
 */
public class App {

    static void disableCertValidaton() {

        
        javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
                new javax.net.ssl.HostnameVerifier() {
                    public boolean verify(String hostname,
                            javax.net.ssl.SSLSession sslSession) {
                            return true; // or return true
                    }
                });
        

        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[] {

                new X509TrustManager() {

                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };

        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (GeneralSecurityException e) {
        }
    }

    public static void main(String[] args) throws Exception {

        //disable check for self signed certs when using TLS
        disableCertValidaton();

        RedisDataLoader redisDataLoader = new RedisDataLoader("./config.properties");


        //load into hash
        //CSVScanner tickets = new CSVScanner("C:/Users/Jay Datsur/OneDrive/Tech/Redis/DataSets/support_tickets_demo.csv",",",false);
        //redisDataLoader.loadHash("support:tickets:", "TicketID", tickets);

        //load into JSON
        String keyType = "random"; // header or random
        CSVScanner subscriptions = new CSVScanner("C:/Users/Jay Datsur/OneDrive/Tech/Redis/DataSets/subscriptions.csv",",",false);
        redisDataLoader.loadJSON("healthcare:subscriptions:", keyType, "SubscriberUniqueID", "DependantUniqueID", "dependants", subscriptions, 0);


        redisDataLoader.close();

    }
    
}
