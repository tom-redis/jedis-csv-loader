package com.jsd.utils;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class DBLoader {

    private Properties config;
    private Connection conn;

    private RandomDataGenerator gen;

    public DBLoader(String configFile) throws Exception {
        config = new Properties();
        config.load(new FileInputStream(configFile));
        gen = new RandomDataGenerator(config.getProperty("data.template.file"));
        createConnection();
    }

    private void createConnection() throws Exception {
        Class.forName(config.getProperty("db.driver.name"));
        String url = config.getProperty("db.url");
        String user = config.getProperty("db.user");
        String pwd = config.getProperty("db.password");
        conn = DriverManager.getConnection(url, user, pwd);
        conn.setAutoCommit(false);
    }

    public void createTables() {

        System.out.println("[DBLoader] Creating Tables:");
        String[] tables = { "header.table", "detail.table" };

        try {

            Statement stmt = conn.createStatement();


            for (String t : tables) {
                String tableSQL = config.getProperty(t, "NA");

                try {
                    if (!"NA".equalsIgnoreCase(tableSQL)) {
                        stmt.executeUpdate(tableSQL);
                        System.out.println("[DBLoader] Created " +  t + " Table:");
                    }

                } catch (SQLException se) {
                    if (955 == se.getErrorCode()) {
                        System.err.println("[DBLoader] " + t + " Already Exists");
                    }
                }
            }
    
        } catch (SQLException se) {
            System.err.println("[DBLoader] Error Creating Tables:\n" + se);
        }

    }

    private void truncateTables()  {
        System.out.println("[DBLoader] Creating Tables:");
        String[] tables = { "header.table.trunc", "detail.table.trunc" };

        try {

            Statement stmt = conn.createStatement();

            for (String t : tables) {
                String tableSQL = config.getProperty(t, "NA");

                try {
                    if (!"NA".equalsIgnoreCase(tableSQL)) {
                        stmt.executeUpdate(tableSQL);
                        System.out.println("[DBLoader] Truncated " +  t + " Table:");
                    }

                } catch (SQLException se) {

                }
            }    
        } catch (SQLException se) {
            System.err.println("[DBLoader] Error Truncating Tables:\n" + se);
        }     
    }

    private void insertRow(JSONObject record, String colNamesStr, String colTypesStr, PreparedStatement stmt) throws Exception {
        String[] colTypes = colTypesStr.split(",");
        String[] colNames = colNamesStr.split(",");

        for (int c = 0; c < colTypes.length; c++) {
            if("i".equalsIgnoreCase(colTypes[c])) {
                stmt.setInt(c + 1, record.getInt(colNames[c]));
            }
            else if("f".equalsIgnoreCase(colTypes[c])) {
                stmt.setFloat(c + 1, record.getFloat(colNames[c]));
            }
            else {
                stmt.setString(c + 1, record.getString(colNames[c]));
            }
        }

        stmt.executeUpdate();
    }

    public void loadData() throws Exception {

        truncateTables();

        System.out.println("[DBLoader] Loading Data:");

        JSONObject record = null;

        config.getProperty("");

        String headerUID = config.getProperty("header.json.uid");
        String detailArr = config.getProperty("detail.json.arr", "NA");

        String headerCols = config.getProperty("header.json.col.names");
        String detailCols = headerUID + "," + config.getProperty("detail.json.col.names","NA");

        String headerColTypes = config.getProperty("header.json.col.types");
        String detailColTypes = headerColTypes.substring(0, 1) + "," + config.getProperty("detail.json.col.types","NA");

        PreparedStatement headerInsertStmt = conn.prepareStatement(config.getProperty("header.insert"));
        PreparedStatement detailInsertStmt = null;

        if(!"NA".equalsIgnoreCase(config.getProperty("detail.insert","NA"))) {
            detailInsertStmt = conn.prepareStatement(config.getProperty("detail.insert","NA"));
        }

       
        int batchSize = Integer.parseInt(config.getProperty("batch.size","500"));

        for(int i = 0; i < batchSize; i++) {
            record = gen.generateRecord("header");

            //insert the header record first
            insertRow(record, headerCols, headerColTypes, headerInsertStmt);

            //insert the detail records
            if(!"NA".equalsIgnoreCase(detailArr)) {
                JSONArray detailRecords = record.getJSONArray(detailArr);

                for(int d = 0; d < detailRecords.length(); d++) {
                    JSONObject detailRecord = detailRecords.getJSONObject(d);

                    //add the pk of the header table
                    detailRecord.put(headerUID, record.getString(headerUID));

                    insertRow(detailRecord, detailCols, detailColTypes, detailInsertStmt);
                }
            }

            conn.commit();
        }

        
        conn.close();

    }

    public static void main(String[] args) throws Exception {
        DBLoader dbLoader = new DBLoader("./jdbc-loader-config-ora.properties");
        dbLoader.createTables();
        dbLoader.loadData();

    }
}
