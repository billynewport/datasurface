// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.aws.dmsingestor.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.datasurface.dmsingestor.SparkTypeConversion;

public class DataFrameChecker 
{
    String tableName;
    String keyColumnName;
    SparkSession sparkSession;

    Map<String, String> basePkHashes = new HashMap<String, String>();
    Map<String, String> baseRecHashes = new HashMap<String, String>();
    Map<String, String> newPkHashes = new HashMap<String, String>();
    Map<String, String> newRecHashes = new HashMap<String, String>();
    Dataset<Row> df;

    public DataFrameChecker(SparkSession sparkSession, String tableName, String keyColumnName)
    {
        this.sparkSession = sparkSession;
        this.tableName = tableName;
        this.df = null;
        this.keyColumnName = keyColumnName;
    }

    /** This reads the table and constructs a Set of primary key hashs
        and another set of all column hashes. It also checks the number
     of rows is whats expected
    */
    
    public Dataset<Row> refreshDataFrame(int expectedRows)
    {
        rebaseHashes();
        df = JobTester.sparkSession.read().format("iceberg").load(tableName);
        // Check All records were loaded
        assertEquals(expectedRows, df.count());
        // Copy all all records into a Map of HashMaps, one for each record
        newPkHashes = new HashMap<String, String>();
        newRecHashes = new HashMap<String, String>();
        
        for (Row r : df.collectAsList()) {
            newPkHashes.put(r.getAs(keyColumnName), r.getAs(SparkTypeConversion.PK_HASH_COL_NAME));
            newRecHashes.put(r.getAs(keyColumnName), r.getAs(SparkTypeConversion.ALL_COLS_HASH_COL_NAME));
        }
        assertEquals(expectedRows, newPkHashes.size());
        assertEquals(expectedRows, newRecHashes.size());
        return df;
    }

    public void rebaseHashes()
    {
        basePkHashes = newPkHashes;
        baseRecHashes = newRecHashes;
    }

    public void verifyCRUD(Set<String> insertedKeys, Set<String> updatedKeys, Set<String> deletedKeys)
    {
        for( String keyColValue : newPkHashes.keySet() )
        {
            if(updatedKeys.contains(keyColValue))
            {
                // Verify key hash is the same, but record hash is different
                assertEquals(basePkHashes.get(keyColValue), newPkHashes.get(keyColValue));
                assertNotEquals(baseRecHashes.get(keyColValue), newRecHashes.get(keyColValue));
            }
            else if(insertedKeys.contains(keyColValue))
            {
                // It's a new record, nothing to do yet
            }
            else
            {
                // Verify key and all column hashes are the same as previous version
                assertEquals(basePkHashes.get(keyColValue), newPkHashes.get(keyColValue));
                assertEquals(baseRecHashes.get(keyColValue), newRecHashes.get(keyColValue));
            }
        }
        // Verify deleted records are not present
        for(String v: deletedKeys)
        {
            assertFalse(newRecHashes.containsKey(v));
        }
    }
}
