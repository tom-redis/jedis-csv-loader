package com.jsd.utils;



import java.io.OutputStream;

import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;


import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;


/**
 * Hello world!
 */
public  class RedisCloudAPI {


    private static final String DB_API_URL = "https://api.redislabs.com/v1/subscriptions/{subscriptionId}/databases/{databaseId}";


    public static void main(String[] args) throws Exception {


        // For dev purposes only
        //disableCertValidaton();


        String subscriptionID = "XXXXXXX";
        String dbID = "YYYYYYY";

        RedisCloudAPI redisCloudAPI = new RedisCloudAPI();

        // JSON body for the update request
        String jsonPayload = "{ \"dryRun\": false,  \"datasetSizeInGb\": 4}";
        redisCloudAPI.updateDatabase(subscriptionID, dbID, jsonPayload);

        JsonObject dbObj = redisCloudAPI.getRedisDB(subscriptionID, dbID);

        double datasetSize = redisCloudAPI.getDatasetSize(subscriptionID, dbID);
        System.out.println("Dataset Size: " + datasetSize);

    }

    public void setAuthTokens(HttpsURLConnection conn) {
        conn.setRequestProperty("x-api-key", System.getenv("REDIS_API_ACCOUNT"));
        conn.setRequestProperty("x-api-secret-key", System.getenv("REDIS_API_USER"));
    }

    public boolean updateDatabase(String subscriptionId, String databaseId, String jsonPayload) throws Exception {

        URL url = new URL(DB_API_URL.replace("{subscriptionId}", subscriptionId).replace("{databaseId}", databaseId));
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Type", "application/json");
        setAuthTokens(conn);
        conn.setDoOutput(true);



        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonPayload.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        int responseCode = conn.getResponseCode();

        if (responseCode >= 200 &&   responseCode <= 300) {
            System.out.println("[RedisCloudAPI] Database updated successfully." + responseCode);
            return true;
        } else {
            System.out.println("[RedisCloudAPI] Failed to update database. Response code: " + responseCode);
            return false;
        }
    }

    public JsonObject getRedisDB(String subscriptionId, String databaseId) throws Exception {

        String urlString = DB_API_URL.replace("{subscriptionId}", subscriptionId).replace("{databaseId}", databaseId);
        JsonObject jobj =  getJsonObject(urlString);
        return jobj;
    }

    public double getDatasetSize(String subscriptionId, String databaseId) throws Exception {

        JsonObject jobj = getRedisDB(subscriptionId, databaseId);

        double dsize = jobj.getJsonNumber("datasetSizeInGb").doubleValue();

        return dsize;
    }

    public HttpsURLConnection executeGet(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setDoOutput(false);
        conn.setDoInput(true);
        conn.setRequestMethod("GET");
        conn.setUseCaches(false);
        conn.setRequestProperty("Accept", "application/json");
        setAuthTokens(conn);
        conn.connect();

        return conn;
    }

    public JsonArray getJsonArray(String urlStr) throws Exception {

        HttpsURLConnection conn = executeGet(urlStr);
        JsonReader rdr = Json.createReader(conn.getInputStream());
        JsonArray rec = rdr.readArray();
        conn.disconnect();
        return rec;
    }

    public JsonObject getJsonObject(String urlStr) throws Exception {

        HttpsURLConnection conn = executeGet(urlStr);
        JsonReader rdr = Json.createReader(conn.getInputStream());
        JsonObject rec = rdr.readObject();
        conn.disconnect();
        return rec;
    }


    private boolean getBoolean(JsonObject jobj, String key) {
        boolean keyVal = false;

        try {
            keyVal = jobj.getBoolean(key);
        }
        catch(Exception e){}

        return keyVal;
    }

    private String getString(JsonObject jobj, String key) {
        String keyVal = "";

        try {
            keyVal = jobj.getString(key);
        }
        catch(Exception e){}

        return keyVal;
    }


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



}

