// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.aws.dmsingestor.tests;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.InputStream;

import org.datasurface.dmsingestor.AWSSecretPair;

public class DockerSecretPairTest {

    @Test
    public void testDockerSecretPair() throws Exception {
        String fileName = "dockerAccessSecretTest.txt";
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        AWSSecretPair secretPair = new AWSSecretPair(is);

        // Replace these with the actual values in your test_secret file
        String expectedAccessKey = "ACCESS_KEY";
        String expectedSecretKey = "SECRET_KEY";

        assertEquals(expectedAccessKey, secretPair.getAccessKey());
        assertEquals(expectedSecretKey, secretPair.getSecretKey());
    }
}