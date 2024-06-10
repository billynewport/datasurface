// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.aws.dmsingestor.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.datasurface.dmsingestor.JobState;
import org.junit.Test;

public class JobStateTests {
    /**
     * Test the job state can be:
     *  - Initialized and written to a stream
     *  - read back from the stream
     *  - copied to a new job state
     *  - new JobState is correctly initialized
     */
    @Test
    public void testJobStateSerialization()
        throws InterruptedException
    {
        JobState state = new JobState();
        // Check initial batchID is 1
        assertEquals(1, state.getBatchID());
        assertEquals(0, state.getNumProcessedFiles());
        assertEquals(-1, state.getInputBatchID());

        // Kill some time to make sure finish time is later than start time
        Thread.sleep(100); // Wait 100ms

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            state.writeToStream(baos);
            baos.close();
        }
        catch(IOException e)
        {
            fail();
        }

        // Read back as an InputStream and deserialize the JobState
        InputStream is = new ByteArrayInputStream(baos.toByteArray());
        JobState sameBatch = JobState.readFromStream(is);
        assertTrue(sameBatch.getJobDuration() > 0);
        assertTrue(sameBatch.getFinishTime() > sameBatch.getStartTime());
        assertNotNull(sameBatch);

        // Check it inflated back exactly
        assertEquals(state, sameBatch);

        // Make the next job state from a completed one
        JobState nextBatch = new JobState(sameBatch);
        Thread.sleep(100); // Wait 100ms

        // Check the new batch is correctly initialized
        assertEquals(sameBatch.getBatchID() + 1, nextBatch.getBatchID());
        assertEquals(sameBatch.getProcessedFiles(), nextBatch.getProcessedFiles());
        assertTrue(nextBatch.getStartTime() > sameBatch.getStartTime());
        assertTrue(sameBatch.getFinishTime() < nextBatch.getStartTime());
        assertEquals(nextBatch.getInputBatchID(), sameBatch.getBatchID());
        assertEquals(-1, nextBatch.getJobDuration());

    }
    
}
