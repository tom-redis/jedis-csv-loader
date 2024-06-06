package com.jsd.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Hashtable;



public class CSVScanner {

    private String[] columnNames;
    private Hashtable<String,String> currentRecord = new Hashtable<String, String>();
    private BufferedReader br;
    private int numCols;
    private boolean hasQuotes;
    private String delimiter;
    private String currentRow;

    public CSVScanner(String file, String delimiter, boolean hasQuotes) throws Exception {
        try {

            this.delimiter = delimiter;
            this.hasQuotes = hasQuotes;

            br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String header = br.readLine();

            this.columnNames = splitRecord(header);
            this.numCols = this.columnNames.length;
            
            System.out.println("[CSVScanner] Loaded Header: " + columnNames.length);
           
        }
        catch(Exception e) {
            System.err.println("Error Instantiating File\n" + e);
        }
    }

    public int getNumColumns() {
        return this.numCols;
    }

    public String[] getColumnNames() {
        return this.columnNames;
    }

    public void close() {
        try {
            this.br.close();
        } catch (Exception e) {
        }
  
        return;
    }

    public boolean hasMoreRecords() {
        boolean hasMoreRecords = false;

        try {
            currentRow = br.readLine();
            hasMoreRecords = (currentRow != null);

           if(hasMoreRecords) {
                String[] cols = splitRecord(currentRow);
                
                for(int c = 0; c < cols.length; c++) {
                    this.currentRecord.put(columnNames[c], cols[c]);
                }
            }
            else {
                br.close();
            }
            
        } catch (Exception e) {}

        return hasMoreRecords;
    }

    public String getColumnValue(String colName) {
            return this.currentRecord.get(colName);
    }

    public String getColumnValue(int colNum) {
        return this.currentRecord.get(columnNames[colNum]);
    }

    private String[] splitRecord(String record) {

        String recordValues[] = record.split(this.delimiter);
        int numCols = recordValues.length;
        
        for(int c = 0; c < numCols; c++) {
            String val = recordValues[c];

            if(this.hasQuotes) {
                if(val.startsWith("\"") || val.startsWith("'")) {
                    val = val.substring(1);
                }

                if(val.endsWith("\"") || val.endsWith("'")) {
                    val = val.substring(0, val.length() -1);
                }
                
                recordValues[c] = val;
            }
        }  
        
        return recordValues;
    }


    public static void main(String[] args) throws Exception {

    }

}
