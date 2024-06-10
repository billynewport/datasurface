// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.aws.dmsingestor.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.core.JsonProcessingException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.cli.ParseException;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.datasurface.dmsingestor.Job;
import org.datasurface.dmsingestor.JobOptions;
import org.datasurface.dmsingestor.JobState;
import org.datasurface.dmsingestor.SparkTypeConversion;


class ColumnDef {
    String name;
    Supplier<String> dataTypeCreator;
    boolean isNullable;
    String sparkTypeName;

    ColumnDef(String name, Supplier<String> dataTypeCreator, boolean isNullable, String sparkTypeName) {
        this.name = name;
        this.dataTypeCreator = dataTypeCreator;
        this.isNullable = isNullable;
        this.sparkTypeName = sparkTypeName;
    }
}

public class JobTester 
{
    static SparkSession sparkSession = null;

    // Create a Local SparkSession before any tests are run
    @BeforeClass
    public static void setUp() {

        JobTester.sparkSession = SparkSession.builder()
            .appName("test")
            .master("local")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName())
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", "file:/Users/billy/Documents/Code/datasurface/java/dmsingester/ice_warehouse")
            .getOrCreate();

        JobTester.sparkSession.catalog().setCurrentCatalog("spark_catalog");
    }

    // Stop the local spark Session when all tests finish
    @AfterClass
    public static void tearDown() {
        if (JobTester.sparkSession != null) {
            JobTester.sparkSession.stop();
        }
    }
    
    
    JobOptions createJobOptions()
        throws ParseException
    {
        String[] args = {
            "-h", "localhost", 
            "-p", "8080", 
            "-s", "secret",
            "-st", "staging",
            "-ib", "iceberg",
            "-ds", "datastore",
            "-dset", "dataset",
            "-eco", "ecosystem"
        };
        JobOptions rc = new JobOptions(args);
        return rc;
    }

    String toJson(Map<String, Object> map)
    {
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(map);
            return json;
        }
        catch(JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }
    }

    List<ColumnDef> createUS_State_Columns()
    {
        return Arrays.asList(
            new ColumnDef("state_id", () -> toJson(DataSurfaceJsonTypes.createSmallInt()), false, "short"),
            new ColumnDef("state_name", () -> toJson(DataSurfaceJsonTypes.createString(100)), true, "string"),
            new ColumnDef("state_abbr", () -> toJson(DataSurfaceJsonTypes.createString(2)), true, "string"),
            new ColumnDef("state_region", () -> toJson(DataSurfaceJsonTypes.createString(50)), true, "string")
        );
    }

    datasurface.Api.Datastore createDatastore()
        throws JsonProcessingException
    {
        datasurface.Api.DDLSchema.Builder ddlSchemaBuilder = datasurface.Api.DDLSchema.newBuilder();

        // Create a list of lambdas
        List<ColumnDef> dataTypeCreators = this.createUS_State_Columns();

        // Loop over the list and create a DDLColumn for each one
        for (int i = 0; i < dataTypeCreators.size(); i++) {
            ColumnDef col = dataTypeCreators.get(i);
            datasurface.Api.DDLColumn.Builder ddlColumnBuilder = datasurface.Api.DDLColumn.newBuilder();
            ddlColumnBuilder.setName(col.name);
            ddlColumnBuilder.setType(col.dataTypeCreator.get());  // Use the lambda to create the data type
            ddlColumnBuilder.setIsNullable(col.isNullable);
            ddlSchemaBuilder.addColumns(ddlColumnBuilder.build());
        }

        datasurface.Api.Schema.Builder schemaBuilder = datasurface.Api.Schema.newBuilder();
        schemaBuilder.setDdlSchema(ddlSchemaBuilder.build());
        schemaBuilder.addPrimaryKeys("state_id");

        datasurface.Api.Dataset.Builder datasetBuilder = datasurface.Api.Dataset.newBuilder();
        datasetBuilder.setName("us_states");
        datasetBuilder.setDatastoreName("NW_Data");
        datasetBuilder.setSchema(schemaBuilder.build());

        datasurface.Api.Datastore.Builder builder = datasurface.Api.Datastore.newBuilder();
        builder.setName("datastore");
        builder.addDatasets(datasetBuilder);
        return builder.build();
    }

    @Test
    public void testSparkSchemaConversion() throws Exception 
    {
        // Create a test GRPC Datastore with a schema
        datasurface.Api.Datastore datastore = createDatastore();
        assertEquals("datastore", datastore.getName());

        // Put the datasets in a Map
        Map<String, datasurface.Api.Dataset> datasets = new HashMap<String, datasurface.Api.Dataset>();
        for (datasurface.Api.Dataset dataset : datastore.getDatasetsList()) {
            datasets.put(dataset.getName(), dataset);
        }

        // Check us_states is there
        assertTrue(datasets.containsKey("us_states"));
        datasurface.Api.Dataset us_states = datasets.get("us_states");
        assertEquals("us_states", us_states.getName());

        // Check the spark schema equivalent seems correct
        StructType spark_us_states = SparkTypeConversion.calculateSparkSchemaForDataset(us_states);
        assertEquals(4, spark_us_states.fields().length);

        StructField state_id = spark_us_states.fields()[0];
        assertEquals("state_id", state_id.name());
        assertEquals("short", state_id.dataType().typeName());
        assertEquals(false, state_id.nullable());

        StructField state_name = spark_us_states.fields()[1];
        assertEquals("state_name", state_name.name());
        assertEquals("string", state_name.dataType().typeName());
        assertEquals(true, state_name.nullable());

        StructField state_abbr = spark_us_states.fields()[2];
        assertEquals("state_abbr", state_abbr.name());
        assertEquals("string", state_abbr.dataType().typeName());
        assertEquals(true, state_abbr.nullable());

        StructField state_region = spark_us_states.fields()[3];
        assertEquals("state_region", state_region.name());
        assertEquals("string", state_region.dataType().typeName());
        assertEquals(true, state_region.nullable());
    }

    @Test
    public void testRowHashCalculations()
        throws JsonProcessingException
    {
        // Create a test GRPC Datastore with a schema
        datasurface.Api.Datastore datastore = createDatastore();
        assertEquals("datastore", datastore.getName());

        // Put the datasets in a Map
        Map<String, datasurface.Api.Dataset> datasets = new HashMap<String, datasurface.Api.Dataset>();
        for (datasurface.Api.Dataset dataset : datastore.getDatasetsList()) {
            datasets.put(dataset.getName(), dataset);
        }

        // Check us_states is there
        assertTrue(datasets.containsKey("us_states"));
        datasurface.Api.Dataset us_states = datasets.get("us_states");
        assertEquals("us_states", us_states.getName());

        // Check the spark schema equivalent seems correct
        StructType spark_us_states = SparkTypeConversion.calculateSparkSchemaForDataset(us_states);
        assertEquals(4, spark_us_states.fields().length);

        // Create test rows
        List<org.apache.spark.sql.Row> rows = new ArrayList<org.apache.spark.sql.Row>();
        org.apache.spark.sql.Row row = org.apache.spark.sql.RowFactory.create((short) 1, "Alabama", "AL", "South");
        rows.add(row);
        row = org.apache.spark.sql.RowFactory.create((short) 2, "Florida", "FL", "South");
        rows.add(row);
        row = org.apache.spark.sql.RowFactory.create((short) 3, "Texas", "TX", "Central");
        rows.add(row);
        row = org.apache.spark.sql.RowFactory.create((short) 4, "California", "CA", "West");
        rows.add(row);
        // Create a Spark Dataset and add the above row
        Dataset<Row> df = JobTester.sparkSession.createDataFrame(rows, spark_us_states);
        // Add extra hash columns with calculated values
        df = SparkTypeConversion.addMaterializedHashesToDataFrame(us_states, df);

        // Check the hash columns are there and the values are correct
        Set<String> pkHashes = new HashSet<String>();
        Set<String> recHashes = new HashSet<String>();
        for (Row r : df.collectAsList()) {
            String pkHash = r.getAs(SparkTypeConversion.PK_HASH_COL_NAME);
            assertEquals(32, pkHash.length()); // 256 bit hash means 32 hex digits
            pkHashes.add(pkHash);
            String allColHash = r.getAs(SparkTypeConversion.ALL_COLS_HASH_COL_NAME);
            assertEquals(32, allColHash.length()); // 256 bit hash means 32 hex digits
            recHashes.add(allColHash);
        }
        assertEquals(rows.size(), pkHashes.size()); // Verify each row has unique pk hash
        assertEquals(rows.size(), recHashes.size()); // Verify each row has unique all cols hash
    }

    @Test
    public void testInitialLoad()
        throws Exception
    {
        List<String> loadFiles = new ArrayList<String>();
        loadFiles.add("src/test/resources/states/load/LOAD_0000.csv");
        loadFiles.add("src/test/resources/states/load/LOAD_0001.csv");

        String databaseName = "default";

        // Create a test GRPC Datastore with a schema
        datasurface.Api.Datastore datastore = createDatastore();
        assertEquals("datastore", datastore.getName());

        // Put the datasets in a Map
        Map<String, datasurface.Api.Dataset> datasets = new HashMap<String, datasurface.Api.Dataset>();
        for (datasurface.Api.Dataset dataset : datastore.getDatasetsList()) {
            datasets.put(dataset.getName(), dataset);
        }

        // Check us_states is there
        assertTrue(datasets.containsKey("us_states"));
        datasurface.Api.Dataset us_states = datasets.get("us_states");
        assertEquals("us_states", us_states.getName());

        Job.dropTable(sparkSession, us_states, databaseName);

        JobState jobState = new JobState();
        
        Job.copyLoadDataToIceberg(sparkSession, jobState, us_states, databaseName, loadFiles);
        assertEquals(2, jobState.getNumProcessedFiles());

        // Read contents of initial load and check number of rows
        DataFrameChecker dfc = new DataFrameChecker(sparkSession, databaseName + ".NW_Data_us_states", "state_abbr");
        dfc.refreshDataFrame(10);

        // Apply first set of changes, Update FL, Insert ID, IL and IN
        Job.mergeCDCDataIntoIceBerg(sparkSession, jobState, us_states, databaseName, Collections.singletonList("src/test/resources/states/cdc/0000.csv"));
        dfc.refreshDataFrame(13);
        assertEquals(3, jobState.getNumProcessedFiles()); // 2 LOAD files and one CDC file

        dfc.verifyCRUD(new HashSet<String>(Arrays.asList("ID", "IL", "IN")), new HashSet<String>(Arrays.asList("FL")), Collections.emptySet());

        // Apply 2nd set of changes, Update MA and MI
        Job.mergeCDCDataIntoIceBerg(sparkSession, jobState, us_states, databaseName, Collections.singletonList("src/test/resources/states/cdc/0001.csv"));
        assertEquals(4, jobState.getNumProcessedFiles()); // 2 LOAD files and 2 CDC file
        dfc.refreshDataFrame(13);
        dfc.verifyCRUD(new HashSet<String>(Arrays.asList("MA", "MI")), Collections.emptySet(), Collections.emptySet());

        // Apply 3rd set of changes, Delete FL, GA and NY
        Job.mergeCDCDataIntoIceBerg(sparkSession, jobState, us_states, databaseName, Collections.singletonList("src/test/resources/states/cdc/0002.csv"));
        assertEquals(5, jobState.getNumProcessedFiles()); // 2 LOAD files and 3 CDC file
        dfc.refreshDataFrame(10);
        dfc.verifyCRUD(Collections.emptySet(), Collections.emptySet(), new HashSet<String>(Arrays.asList("FL", "GA", "NY")));

        // Apply 3rd set of changes, Delete FL, GA and NY
        Job.mergeCDCDataIntoIceBerg(sparkSession, jobState, us_states, databaseName, Collections.singletonList("src/test/resources/states/cdc/0002.csv"));
        assertEquals(5, jobState.getNumProcessedFiles()); // 2 LOAD files and 3 CDC file
        dfc.refreshDataFrame(10);
        dfc.verifyCRUD(Collections.emptySet(), Collections.emptySet(), new HashSet<String>(Arrays.asList("FL", "GA", "NY")));
    }

}
