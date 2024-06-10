// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.dmsingestor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This tracks the state of a job. This is stored in a file and loaded when the job is started. It
 * is modified to reflect the new state and then written to a new file.
 */
public class JobState implements Serializable {
    /**
     * This is the set of files that have been processed by the job. This is used to avoid ingesting
     * the same file twice.
     */
    Set<String> processedFiles;
    /**
     * This is the time the job started. This is used to calculate the time the job took to run.
     */
    long startTime;
    /**
     * This is the time the job finished. This is used to calculate the time the job took to run.
     */
    long finishTime;
    /**
     * This is the time the job took to run. This is calculated from the startTime and finishTime.
     */
    long jobDuration;
    /**
     * Input Batch ID
     */
    long inputBatchID;
    /**
     * Output Batch ID
     */
    long batchID;
    
    public JobState()
    {
        this.startTime = System.currentTimeMillis();
        this.finishTime = 0;
        this.jobDuration = 0;
        this.processedFiles = new HashSet<String>();
        this.batchID = 1;
        this.inputBatchID = -1;
    }

    public JobState(JobState previous)
    {
        this.startTime = System.currentTimeMillis();
        this.finishTime = -1;
        this.jobDuration = -1;
        this.processedFiles = new HashSet<String>(previous.processedFiles);
        this.batchID = previous.batchID + 1;
        this.inputBatchID = previous.batchID;
    }

    /**
     * This tests for equality against another JobState instance
     */
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || getClass() != obj.getClass())
        {
            return false;
        }
        else
        {
            JobState other = (JobState) obj;
            return this.startTime == other.startTime &&
                this.finishTime == other.finishTime &&
                this.jobDuration == other.jobDuration &&
                this.processedFiles.equals(other.processedFiles) &&
                this.batchID == other.batchID &&
                this.inputBatchID == other.inputBatchID;
        }
    }

    public long getBatchID()
    {
        return batchID;
    }

    public long getInputBatchID()
    {
        return inputBatchID;
    }

    public Set<String> getProcessedFiles()
    {
        return processedFiles;
    }

    /**
     * This checks if a file has already been marked as processed or not.
     */
    public boolean isProcessed(String fileName)
    {
        return processedFiles.contains(fileName);
    }

    /**
     * This marks a file as processed. The JobState should be persisted between jobs
     * so that subsequent jobs do not reprocess the same files.
     */
    public void markProcessed(String fileName)
    {
        processedFiles.add(fileName);
    }

    /**
     * This returns the number of files that have been processed by the job.
     */
    @JsonIgnore
    public int getNumProcessedFiles()
    {
        return processedFiles.size();
    }

    /**
     * This marks the job as finished. It sets the finishTime and calculates the jobDuration.
     */
    void markJobFinished()
    {
        finishTime = System.currentTimeMillis();
        jobDuration = finishTime - startTime;
    }

    public long getStartTime()
    {
        return startTime;
    }

    public long getFinishTime()
    {
        return finishTime;
    }

    public long getJobDuration()
    {
        return jobDuration;
    }

    static public JobState readFromStream(InputStream is)
    {
        // Read the JobState from the input stream encoded as JSON
        try{
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(is, JobState.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void writeToStream(OutputStream os)
        throws IOException
    {
        markJobFinished();
        // Writes this object state as JSON to the stream os
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            String v = mapper.writeValueAsString(this);
            os.write(v.getBytes());
        } catch (IOException e) 
        {
            e.printStackTrace();
            throw e;
        }
    }

    public String calculateFileName()
    {
        // Returns the filename with the batch id formatted with leading zeros
        return String.format("jobstate-%010d.json", batchID);
    }
}
