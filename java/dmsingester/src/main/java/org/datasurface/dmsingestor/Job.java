// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.dmsingestor;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import datasurface.Api.DataStoreRequest;
import datasurface.Api.Datastore;

import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.io.InputStream;

import org.apache.commons.cli.ParseException;
import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.log4j.Logger;

/**
 * This job handles 2 kinds of data. LOAD data for initial loading of a dataset and CDC data for handling changes once the full load has
 * completed. The first step in an ingest is to copy all the current data. Once complete, then we can start to handle CDC data. This code
 * creates an iceberg table in storage and uses a storage based iceberg catalog.
 * 
 * The job is parameterized on the datastore/datset to be ingested. DataSurface either asks for a single dataset to be ingest per job or
 * ALL datasets in a datastore to be ingested within a single job. The name of the datastore/data is provided as an argument as well
 * as the host:port for the owning DataSurface GRPC server. The job asks the GRPC server for the datastore and its datasets. It creates
 * the IceBerg/Spark schema for the dataset(s) and then either does a full load of data or CDC data.
 * 
 * The job reads data from the file system. It needs to track which files have already been processed and skip those. The state for this processedFile
 * list is also stored in the file system.
 * 
 * The job stores the created/maintained iceberg table in the filesystem. DataSurface is responsible to create links to the table in any
 * vendor catalogs such as SnowFlake or AWS Glue etc. The iceberg tables can be queried directly using Spark SQL or using SQL by pointing
 * an Athena service at the glue catalog or similar.
 * 
 * 2 extra columns are added to the dataset schema by this job. Both columns are hashes and are used for performance reasons. The first is
 * a hash of all the primary key columns. This means for tables with compound keys we can compare records using a single column rather than
 * multiple columns. The second column is a hash of all columns in the record. This is used to compare records for changes. Rather than
 * comparing every column in a record we can compare a single column. This is a performance optimization.
 * 
 * A goal for this job is that it's cloud vendor agnostic.
 * 
 *  The job has 2 phases. A Full load phase which quickly appends records from a full table dump. Files from the LOAD phase must be prefixed with "LOAD". 
 * A CDC phase where records marked with a IUD flag
 * are MERGED in to existing records. It's assumed that files in staging are lexically ordered. This means they should have a prefix of a time stamp
 * or sequence number to allow ordering. It's assumed that there will be no CDC files until all LOAD file are present in staging. The code uses this
 * assumption to know when to switch from the initial LOAD phase to the CDC phase. This happens when there are no unprocessed LOAD files remaining.
 */

public class Job {
    private JobOptions options = null;

    // The GRPC client to the DataSurface server
    private ProducerServiceClient modelClient = null;

    // The Spark session to use for this job
    private SparkSession sparkSession = null;

    // The Hadoop file system to use for this job
    private FileSystem fs = null;

    // These are the AWS secrets to access S3 primarily
    private String awsAccessKey = null;
    private String awsSecretKey = null;

    // The Datastore metadata from the GRPC service for the datastore to handle with this job
    private Datastore dataStore = null;

    private static final Logger logger = Logger.getLogger(Job.class);

    Job()
    {

    }

    Job(String[] args)
        throws ParseException
    {
        // Collect all arguments
        this.options = new JobOptions(args);

        // Start DataSurface client
        this.modelClient = new ProducerServiceClient(options.getDataSurfaceServerHost(), Integer.parseInt(options.getDataSurfaceServerPort()));

        // Fetch the Datastore that we will ingest
        this.dataStore = modelClient.getDatastore(
            DataStoreRequest.newBuilder()
                .setDataStoreName(options.getDatastoreName())
                .setEcoSystemName(options.getEcosystemName())
                .build());
    }

    private void initFileSystem()
    {
        try
        {
            this.fs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
        }
        catch (IOException e)
        {
            logger.error("error initializing hadoop file system: " + e.getMessage());
            System.exit(-1);
        }
    }

    /**
     * This returns a list of all files in the folder pattern. This can include a wildcard
     */
    public FileStatus[] getFilesInFolder(String folder)
        throws IOException
    {
        return fs.listStatus(new Path(folder));
    }

    // Initialize the Spark Session, authenticate, setup catalog
    private void initSpark()
        throws IOException
    {
        try
        {
            DockerSecretPair secretPair = new DockerSecretPair(options.getAwsSecret());
            awsAccessKey = secretPair.getAccessKey();
            awsSecretKey = secretPair.getSecretKey();
        }
        catch (IOException e)
        {
            logger.error("error reading secrets: " + e.getMessage());
            throw e;
        }

        sparkSession = SparkSession.builder()
            .appName(this.calculateJobName())
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName())
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")

            // Set the base location of the iceberg tables which are created.
            .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://" + options.getIcebergS3Bucket())

            // Set the credentials for AWS
            .config("spark.hadoop.fs.s3a.access.key", awsAccessKey)
            .config("spark.hadoop.fs.s3a.secret.key", awsSecretKey)
            .getOrCreate();
    }

    String calculateJobName()
    {
        return "DMSIngestor-" + options.getDatastoreName() + options.getDatasetName() != null ? "-" + options.getDatasetName() : "";
    }

    /**
        Staging data is stored in the staging bucket in a 2 level folder structure of store name and dataset name
    */
    String calculateStagingFolder(String storeName, String datasetName)
    {
        return "s3a://" + options.getStagingS3Bucket() + "/" + storeName + "/" + datasetName;
    }

    String calculateMetaStagingFolder(String storeName, String datasetName)
    {
        return "s3a://" + options.getStagingS3Bucket() + "_job_md/" + storeName + "/" + datasetName;
    }

    String calculateMetaStagingFolder(String storeName)
    {
        return "s3a://" + options.getStagingS3Bucket() + "_job_md/" + storeName;
    }

    /** The table name is a join of store and data set name. This may be a little redundant given
       its also stored in a 2 level folder structure in S3 with the store and dataset name
    */
    static String calculateTableName(String storeName, String datasetName)
    {
        return storeName + "_" + datasetName;
    }

    String calculateJobStatePath()
    {
        // For multi-dataset there is one jobstate per Datastore. For single dataset there is one jobstate per dataset per datastore
        String metaFolder = options.getDatasetName() != null ? calculateMetaStagingFolder(options.getDatastoreName(), options.getDatasetName()) : calculateMetaStagingFolder(options.getDatastoreName());
        return metaFolder;
    }

    /**
     * This reads the job state from the filesystem. The job state is a JSON file which contains the list of processed files
     * and the start and end time of the job. The job state is used to track which files have been processed and which have not.
     */
    JobState getJobStatus()
    {
        // For multi-dataset there is one jobstate per Datastore. For single dataset there is one jobstate per dataset per datastore
        String metaFolder = calculateJobStatePath();
        try
        {
            FileStatus[] files = getFilesInFolder(metaFolder);
            if (files.length == 0)
            {
                return new JobState();
            }
            // Find the most recent file in that folder. The batch number is path of the file name
            FileStatus file = Arrays.stream(files).max(Comparator.comparing(FileStatus::getPath, Comparator.comparing(Path::getName)))
                .orElseThrow(() -> new FileNotFoundException("No files found in " + metaFolder));
            InputStream is = fs.open(file.getPath());
            try
            {
                JobState state = JobState.readFromStream(is);
                return state;
            }
            finally
            {
                is.close();
            }
        }
        catch (IOException e)
        {
            logger.error("error reading job state: " + e.getMessage());
            return new JobState();
        }
    }
    
    static datasurface.Api.Dataset findDataset(datasurface.Api.Datastore dataStore, String datasetName)
        throws RuntimeException
    {
        for (datasurface.Api.Dataset dataset : dataStore.getDatasetsList())
        {
            if (dataset.getName().equals(datasetName))
            {
                return dataset;
            }
        }
        logger.error(datasetName + " not found in datastore " + dataStore.getName());
        throw new RuntimeException(String.format("Dataset %s not found in store %s", datasetName, dataStore.getName()));
    }

    /**
     * This creates a DataFrameWriter for the specified DataFrame with the correct partitioning columns
     * if needed
     */
    static DataFrameWriter<Row> createIcebergWriterWithPartitioning(Dataset<Row> df, datasurface.Api.Dataset dataset)
    {
        String[] partitionCols = (String[])dataset.getSchema().getIngestionPartitionKeysList().toArray(new String[0]);
        DataFrameWriter<Row> w = df.write();
        if(partitionCols.length > 0)
        {
            w = w.partitionBy(partitionCols);
        }
        return w;
    }

    /**
     * The drops the iceberg table for the dataset. This is typically done during testing.
     */
    static public void dropTable(SparkSession sparkSession, datasurface.Api.Dataset dataset, String databaseName)
        throws AnalysisException
    {
        String tableName = Job.calculateTableName(dataset.getDatastoreName(), dataset.getName());
        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        if(sparkSession.catalog().tableExists(tableIdentifier.toString()))
            sparkSession.sql("DROP TABLE IF EXISTS " + tableIdentifier.toString());
    }

    /**  Copy the data from the staging bucket to the iceberg table
     */
    static public void copyLoadDataToIceberg(SparkSession sparkSession, JobState jobState, datasurface.Api.Dataset dataset, String databaseName, List<String> stagingFileNames)
        throws IOException
    {
        String datasetName = dataset.getName();
        // The data files from AWS staging bucket are loaded into a DataFrame.
        // DMS uses CSV as the default

        StructType schema = SparkTypeConversion.calculateSparkSchemaForDataset(dataset);

        // Add new input files we haven't seen before to a DataFrame
        Dataset<Row> df = sparkSession.createDataFrame(Collections.emptyList(), schema);
        // Maybe specify LOAD* here. DMS seems to use a LOAD prefix for Full load and incr for CDC data
        for (String file : stagingFileNames)
        {
            // If its new
            if (!jobState.isProcessed(file))
            {
                // Union it in
                Dataset<Row> newDf = sparkSession.read().format("csv").load(file);
                df = df.union(newDf);
                // Remember to skip it next time
                jobState.markProcessed(file);
            }
        }
        // Add the hash columns to the DataFrame
        df = SparkTypeConversion.addMaterializedHashesToDataFrame(dataset, df);

        String tableName = Job.calculateTableName(dataset.getDatastoreName(), datasetName);
        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);

        DataFrameWriter<Row> w = createIcebergWriterWithPartitioning(df, dataset);
        if (!sparkSession.catalog().tableExists(tableIdentifier.toString())) {
            // Create the table
            w.format("iceberg").mode("overwrite").saveAsTable(databaseName + "." + tableName);
        } else {
            // Specify file format, append and table name
            w.format("iceberg").mode("append").save(databaseName + "." + tableName);
        }
    }

    /**
     * This collects all the staging files which have not been seen in to a DataFrame. It adds the hash columns
     * for primary keys and all columns.
     */
    static public Dataset<Row> collectAllStagingFiles(SparkSession sparkSession, datasurface.Api.Dataset dataset, JobState jobState, List<String> fileList)
        throws FileNotFoundException, IOException
    {
        // The data files from AWS staging bucket are loaded into a DataFrame.
        // DMS uses CSV as the default
        
        StructType cdcSchema = SparkTypeConversion.calculateSparkSchemaForCDCDataset(dataset);
    
        // Add new input files we haven't seen before to a DataFrame
        Dataset<Row> cdcDF = sparkSession.createDataFrame(Collections.emptyList(), cdcSchema);
        // Maybe specify LOAD* here. DMS seems to use a LOAD prefix for Full load and incr for CDC data
        for (String file : fileList)
        {
            // If its new
            if (!jobState.isProcessed(file))
            {
                // Union it in
                Dataset<Row> newDf = sparkSession.read().format("csv").load(file);
                cdcDF = cdcDF.union(newDf);
                // Remember to skip it next time
                jobState.markProcessed(file);
            }
        }
        // add computed hash columns to each record for primary key hash and all columns hash
        cdcDF = SparkTypeConversion.addMaterializedHashesToDataFrame(dataset, cdcDF);
        return cdcDF;

    }

    /** This method takes the data in staging which contains an Op column with I/U or D. It filters the files
     * to only be unprocessed files and then MERGEs them in to an Iceberg Table. The iceberg Table does
     * NOT have an Ops column, just the CDC data being merged in. The MERGE code matches records from the
     * iceberg table and the staging. This is typically done with the primary keys. However, for performance reasons
     * we precalculate a pk hash on each record. This speeds up the MERGE operation as we can compare records using
     * a single string rather then multiple strings as we would with a primary key.
     */
    static public void mergeCDCDataIntoIceBerg(SparkSession sparkSession, JobState jobState, datasurface.Api.Dataset dataset, String databaseName, List<String> stagingFileNames)
        throws IOException, AnalysisException
    {
        // Get a stored list of stagingFileNames
        List<String> sortedStagingFileNames = new ArrayList<String>(stagingFileNames);
        Collections.sort(sortedStagingFileNames);

        String datasetName = dataset.getName();
        try
        {
            // Add new input files we haven't seen before to a DataFrame
            Dataset<Row> cdcDF = collectAllStagingFiles(sparkSession, dataset, jobState, sortedStagingFileNames);
    
            String tableName = Job.calculateTableName(dataset.getDatastoreName(), datasetName);
            TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);

            if (sparkSession.catalog().tableExists(tableIdentifier.toString())) {
                // Within a job all these view names must be unique
                // The iceberg table is the table we are merging into
                String icebergDFViewName = tableName;

                // View name for the CDC files we will merge in to the iceberg table
                String cdcDFViewName = "cdc_" + tableName;
                cdcDF.createOrReplaceTempView(cdcDFViewName);

                Map<String, String> sqlVars = new HashMap<String, String>();
                sqlVars.put("ICE_TABLE", icebergDFViewName); // You cannot use a view on a Dataset which reads the iceberg table. It has to be a table name
                sqlVars.put("STAGE_TABLE", cdcDFViewName);
                sqlVars.put("PK_HASH", SparkTypeConversion.PK_HASH_COL_NAME);
                sqlVars.put("ALL_HASH", SparkTypeConversion.ALL_COLS_HASH_COL_NAME);
                sqlVars.put("OP_COLUMN", SparkTypeConversion.OP_COLUMN);

                datasurface.Api.DDLColumn[] allOrigColNames =(datasurface.Api.DDLColumn[])dataset.getSchema().getDdlSchema().getColumnsList().toArray(new datasurface.Api.DDLColumn[0]);

                // Create a comma seperated list of assigning newData columns to icebergTable columns
                // This is used for an UPDATE operation so copy all the non PK cols from the staging record to the iceberg record
                String setColumns = Arrays.stream(allOrigColNames)
                    .map(column -> String.format("${ICE_TABLE}.%s = ${STAGE_TABLE}.%s", column.getName(), column.getName()))
                    .collect(Collectors.joining(", "));
                setColumns += ", ${ICE_TABLE}.${ALL_HASH} = ${STAGE_TABLE}.${ALL_HASH}";
                sqlVars.put("setColumns", setColumns);

                // Create a comma seperated list of columns for the icebergTable
                String dstColumns = Arrays.stream(allOrigColNames)
                    .map(column -> String.format("${ICE_TABLE}.%s", column.getName()))
                    .collect(Collectors.joining(", "));
                // Also need to assign the hash columns
                dstColumns += ", ${ICE_TABLE}.${PK_HASH}, ${ICE_TABLE}.${ALL_HASH}";
                sqlVars.put("dstColumns", dstColumns);

                // Create a comma seperated list of columns for the newData
                String srcColumns = Arrays.stream(allOrigColNames)
                    .map(column -> String.format("${STAGE_TABLE}.%s", column.getName()))
                    .collect(Collectors.joining(", "));
                // Also need to assign the hash columns
                srcColumns += ", ${STAGE_TABLE}.${PK_HASH}, ${STAGE_TABLE}.${ALL_HASH} ";
                sqlVars.put("srcColumns", srcColumns);

                // Merge the records from newData to icebergTable. We check records whose PK hash columns match
                // and then if the newData record Op column is I then we insert it to icebergTable
                // if it's U then we update the icebergTable record
                // if it's D then we delete it

                // Merge records with common keys OR it's an insert

                

                String sql = "MERGE INTO ${ICE_TABLE} USING ${STAGE_TABLE} ON ${ICE_TABLE}.${PK_HASH} = ${STAGE_TABLE}.${PK_HASH} ";
                        sql += "WHEN MATCHED AND ${STAGE_TABLE}.${OP_COLUMN}='D' THEN DELETE ";
                        sql += "WHEN MATCHED AND ${STAGE_TABLE}.${OP_COLUMN}='U' AND ${ICE_TABLE}.${ALL_HASH} <> ${STAGE_TABLE}.${ALL_HASH} THEN UPDATE SET ${setColumns} ";
                        sql += "WHEN NOT MATCHED AND ${STAGE_TABLE}.${OP_COLUMN}='I' THEN INSERT (${dstColumns}) VALUES (${srcColumns})";

                StringSubstitutor sub = new StringSubstitutor(sqlVars);
                sql = sub.replace(sql);
                sparkSession.sql(sql);
            }
        }
        catch (IOException e)
        {
            logger.error("error reading files from staging: " + e.getMessage());
            throw e;
        }
    }

    /**
     * This ingests a single dataset. If the staging area has unprocessed LOAD files then continue doing an append
     * style ingest of the remaining load files. If there are NO unprocessed LOAD files then we have entered CDC mode
     * and we do MERGE based CDC ingestion
     */
    void ingestDataset(JobState jobState, datasurface.Api.Dataset dataset)
        throws IOException, AnalysisException
    {
        AWS_DMS_StagingFileCollector stagingCollector = new AWS_DMS_StagingFileCollector(this, dataset);

        List<String> stagingFilesForFullLoad = stagingCollector.getLoadFileSet();
        stagingFilesForFullLoad.removeIf(file -> jobState.isProcessed(file));
        // If there are still unprocessed LOAD files
        if(!stagingFilesForFullLoad.isEmpty())
            Job.copyLoadDataToIceberg(this.sparkSession, jobState, dataset, options.getDatabaseName(), stagingFilesForFullLoad);
        else
        {
            // Otherwise, there are no unprocessed LOAD files so do CDC
            List<String> stagingFilesForCDC = stagingCollector.getCdcFileSet();
            // The files are filtered within the mergeCDC call so no need to filter here
            Job.mergeCDCDataIntoIceBerg(sparkSession, jobState, dataset, options.getDatabaseName(), stagingFilesForCDC);
        }
    }

    public void run()
        throws Exception
    {
        try
        {
            initSpark();
            initFileSystem();

            // Read the last JobState or make a new one
            JobState lastJobState = getJobStatus();

            // Create new successor to last job's state
            JobState jobState = new JobState(lastJobState);

            // If dataset is not specified, ingest all datasets
            if (options.getDatasetName() == null)
            {
                for (datasurface.Api.Dataset dataset : dataStore.getDatasetsList())
                {
                    this.ingestDataset(jobState, dataset);
                }
            }
            else
            {
                datasurface.Api.Dataset dataset = Job.findDataset(dataStore, options.getDatasetName());
                ingestDataset(jobState, dataset);
            }
            // Write the job state to the filesystem
            String jobStateFileName = calculateJobStatePath() + "/" + jobState.calculateFileName();
            OutputStream os = fs.create(new Path(jobStateFileName));
            try
            {
                jobState.writeToStream(os);
            }
            finally
            {
                os.close();
            }

            // Write new jobstate file to filesystem
        }
        catch(AnalysisException e)
        {
            logger.error("Error running job", e);
            throw e;
        }
        catch(IOException e)
        {
            logger.error("Error running job", e);
            throw e;
        }
        finally
        {
            if (this.fs != null)
            {
                try
                {
                    this.fs.close();
                }
                catch (IOException e)
                {
                    logger.error("error closing hadoop file system: " + e.getMessage());
                }
            }
            if (sparkSession != null)
            {
                sparkSession.stop();
            }
        }
    }
}