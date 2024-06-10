// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.dmsingestor;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

/**
 * This class parses the command line arguments for the job.
 */
public class JobOptions {
    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;

    public JobOptions(String[] args)
        throws ParseException
    {
        cmd = this.parser.parse(createOptions(), args);
    }   

    private Options createOptions() {
        Options options = new Options();

        // host of DataSurface Server for retrieval of model fragments needed for the job
        Option dataSurfaceServerHost = new Option("h", "data-surface-host", true, "Data Surface Server host");
        dataSurfaceServerHost.setRequired(true);
        options.addOption(dataSurfaceServerHost);

        // Port number for DataSurface Server
        Option dataSurfaceServerPort = new Option("p", "data-surface-port", true, "Data Surface Server port");
        dataSurfaceServerPort.setRequired(true);
        dataSurfaceServerPort.setType(Integer.class);
        options.addOption(dataSurfaceServerPort);

        Option awsSecretOption = new Option("s", "aws-secret", true, "AWS Secret Key");
        awsSecretOption.setRequired(true);
        options.addOption(awsSecretOption);

        Option stagingS3BucketOption = new Option("st", "staging-s3-bucket", true, "S3 Bucket for staging");
        stagingS3BucketOption.setRequired(true);
        options.addOption(stagingS3BucketOption);

        Option icebergS3BucketOption = new Option("ib", "iceberg-s3-bucket", true, "S3 Bucket for iceberg");
        icebergS3BucketOption.setRequired(true);
        options.addOption(icebergS3BucketOption);

        Option datastoreOption = new Option("ds", "datastore", true, "Name of Datastore to ingest");
        datastoreOption.setRequired(true);
        options.addOption(datastoreOption);

        Option datasetOption = new Option("dset", "dataset", true, "Name of Datset to ingest otherwise ALL");
        datasetOption.setRequired(false);
        options.addOption(datasetOption);

        Option ecosystemOption = new Option("eco", "ecosystem", true, "Ecosystem name");
        ecosystemOption.setRequired(true);
        options.addOption(ecosystemOption);

        Option databaseOption = new Option("db", "database-name", true, "Iceberg database name");
        databaseOption.setRequired(true);
        options.addOption(databaseOption);

        return options;
    }

    public String getEcosystemName()
    {
        return cmd.getOptionValue("ecosystem");
    }

    public String getDatastoreName()
    {
        return cmd.getOptionValue("datastore");
    }

    public String getDatasetName()
    {
        return cmd.getOptionValue("dataset");
    }

    public String getDataSurfaceServerHost()
    {
        return cmd.getOptionValue("data-surface-server-host");
    }

    public String getDataSurfaceServerPort()
    {
        return cmd.getOptionValue("data-surface-server-port");
    }

    public String getDatabaseName()
    {
        return cmd.getOptionValue("database-name");
    }
    
    public String getAwsSecret()
    {
        return cmd.getOptionValue("aws-secret");
    }

    public String getStagingS3Bucket()
    {
        return cmd.getOptionValue("staging-s3-bucket");
    }

    public String getIcebergS3Bucket()
    {
        return cmd.getOptionValue("iceberg-s3-bucket");
    }
    
}
