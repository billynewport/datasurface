// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.dmsingestor;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * This constructs the name of the Docker secret holding the AWS secrets the job will use. This file converts
 * from the secret name in the Datastores configuration to the path of the secret file in the Docker container.
 */
public class DockerSecretPair extends AWSSecretPair {
    public DockerSecretPair(String secretName) throws IOException, StreamReadException, JsonMappingException, JsonProcessingException, DatabindException
    {
        super(String.format("/run/secrets/%s", secretName));
    }
}

