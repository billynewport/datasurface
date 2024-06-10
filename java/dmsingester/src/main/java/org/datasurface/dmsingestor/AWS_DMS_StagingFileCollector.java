// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.dmsingestor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;

/**
 * This has the logic to collect staging files and split them in to LOAD files and CDC files. They both have similar schemas
 * but the CDC files have an extra first string column called "Op" which indicates I, U or D.
 */
public class AWS_DMS_StagingFileCollector {
    Job job;
    datasurface.Api.Dataset dataset;
    List<String> loadFileSet;
    List<String> cdcFileSet;

    public AWS_DMS_StagingFileCollector(Job job, datasurface.Api.Dataset dataset)
        throws IOException
    {
        this.job = job;
        this.dataset = dataset;
        this.calculateStagingFiles(dataset);
    }

    /**
     * This returns a list of all files in the staging folder for the dataset that match the filePattern
     */
    private List<String> getAllFilesFromFS(datasurface.Api.Dataset dataset, String filePattern)
        throws IOException
    {
        List<String> fileList = new ArrayList<String>();
        FileStatus[] files = job.getFilesInFolder(job.calculateStagingFolder(dataset.getDatastoreName(), dataset.getName()) + "/" + filePattern);
        for (FileStatus file : files)
        {
            fileList.add(file.getPath().getName());
        }
        return fileList;
    }

    /**
     * This returns a Map with all staging files for LOAD and CDC phases at present. The files are in a common
     * location and LOAD files must have a "LOAD" prefix on the file/object names
     */
    private void calculateStagingFiles(datasurface.Api.Dataset dataset)
        throws IOException
    {
        this.loadFileSet = new ArrayList<String>();
        this.cdcFileSet = new ArrayList<String>();

        List<String> fileList = getAllFilesFromFS(dataset, "*");
        for(String file : fileList)
        {
            if (file.startsWith("LOAD"))
                loadFileSet.add(file);
            else
                cdcFileSet.add(file);
        }
    }

    /**
     * This returns the list of files for full LOAD in staging. This will grow until
     * all records for full load are present. Then it should stay constant
     */
    public List<String> getLoadFileSet()
    {
        return loadFileSet;
    }

    /**
     * This returns the list of files for CDC in staging. This will grow indefinitely
     * as its expected records will continually change, be inserted or deleted.
     */
    public List<String> getCdcFileSet()
    {
        return cdcFileSet;
    }
}
