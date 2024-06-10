// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1

package org.datasurface.aws.dmsingestor.tests;

import java.util.HashMap;
import java.util.Map;

public class DataSurfaceJsonTypes
{
    public static Map<String, Object> createArray(Map<String, Object> elementType)
    {
        Map<String, Object> typeMap = new HashMap<String, Object>();
        typeMap.put("type", "ARRAYTYPE");
        typeMap.put("dataType", elementType);
        return typeMap;
    }

    public static Map<String, Object> createMap(Map<String, Object> keyType, Map<String, Object> valueType)
    {
        Map<String, Object> typeMap = new HashMap<String, Object>();
        typeMap.put("type", "MAPTYPE");
        typeMap.put("keyType", keyType);
        typeMap.put("valueType", valueType);
        return typeMap;
    }

    public static Map<String, Object> createFloat()
    {
        Map<String, Object> typeMap = new HashMap<String, Object>();
        typeMap.put("type", "FLOAT");
        return typeMap;
    }

    public static Map<String, Object> createSmallInt()
    {
        Map<String, Object> typeMap = new HashMap<String, Object>();
        typeMap.put("type", "SMALLINT");
        return typeMap;
    }

    public static Map<String, Object> createDouble()
    {
        Map<String, Object> typeMap = new HashMap<String, Object>();
        typeMap.put("type", "DOUBLE");
        return typeMap;
    }

    public static Map<String, Object> createString(int size)
    {
        Map<String, Object> typeMap = new HashMap<String, Object>();
        typeMap.put("type", "STRING");
        typeMap.put("maxSize", Integer.toString(size));
        return typeMap;
    }

    
}
