// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.dmsingestor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Scanner;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This reads a file which holds an AWS secret as a json structure string. The structure has 2 fields accessKey and secretKey.
 */
public class AWSSecretPair {
    private String accessKey;
    private String secretKey;

    public AWSSecretPair(String fileName) throws IOException
    {
        try
        {
            String secretJson = new String(Files.readAllBytes(Paths.get(fileName)), StandardCharsets.UTF_8);
            this.parseJson(secretJson);
        }
        catch (IOException e)
        {
            throw e;
        }

    }

    /**
     * This parses the string read from the file. The file contains a json value with 2 fields, accessKey and secretKey
     */
    void parseJson(String secretJson) throws IOException
    {
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            @SuppressWarnings("unchecked")
            Map<String, String> secrets = mapper.readValue(secretJson, Map.class);
            
            this.accessKey = secrets.get("accessKey");
            this.secretKey = secrets.get("secretKey");
        }
        catch (IOException e)
        {
            throw e;
        }
    }

    public AWSSecretPair(InputStream inputStream) throws IOException
    {
        try (Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name())) {
            String secretJson = scanner.useDelimiter("\\A").next();
            this.parseJson(secretJson);
        }
        catch (IOException e)
        {
            throw e;
        }
    }

    // Public Getter to return the accessKey variables
    public String getAccessKey()
    {
        return this.accessKey;
    }

    // Public Getter to return the secretKey variables
    public String getSecretKey()
    {
        return this.secretKey;
    }

}

