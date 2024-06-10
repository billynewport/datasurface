// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.aws.dmsingestor.tests;

import org.apache.spark.sql.types.DataTypes;
import org.datasurface.dmsingestor.SparkTypeConversion;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.StructType;

public class SparkDataTypeConversionTest {
    @Test
    public void testPrimitiveSparkDataTypeConversion() 
    {
        assertEquals(SparkTypeConversion.getSparkDataType(DataSurfaceJsonTypes.createString(20)), DataTypes.StringType);
        assertEquals(SparkTypeConversion.getSparkDataType(DataSurfaceJsonTypes.createFloat()), DataTypes.FloatType);
        assertEquals(SparkTypeConversion.getSparkDataType(DataSurfaceJsonTypes.createDouble()), DataTypes.DoubleType);
    }

    @Test
    public void testArrayTypeConversion()
    {
        assertEquals(SparkTypeConversion.getSparkDataType(DataSurfaceJsonTypes.createArray(DataSurfaceJsonTypes.createString(20))), DataTypes.createArrayType(DataTypes.StringType, true));
        assertEquals(SparkTypeConversion.getSparkDataType(DataSurfaceJsonTypes.createArray(DataSurfaceJsonTypes.createFloat())), DataTypes.createArrayType(DataTypes.FloatType, true));
        assertEquals(SparkTypeConversion.getSparkDataType(DataSurfaceJsonTypes.createArray(DataSurfaceJsonTypes.createDouble())), DataTypes.createArrayType(DataTypes.DoubleType, true));
    }

    @Test
    public void testMapTypeConversion()
    {
        assertEquals(SparkTypeConversion.getSparkDataType(DataSurfaceJsonTypes.createMap(DataSurfaceJsonTypes.createString(20), DataSurfaceJsonTypes.createString(20))), DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true));
        assertEquals(SparkTypeConversion.getSparkDataType(DataSurfaceJsonTypes.createMap(DataSurfaceJsonTypes.createFloat(), DataSurfaceJsonTypes.createFloat())), DataTypes.createMapType(DataTypes.FloatType, DataTypes.FloatType, true));
        assertEquals(SparkTypeConversion.getSparkDataType(DataSurfaceJsonTypes.createMap(DataSurfaceJsonTypes.createDouble(), DataSurfaceJsonTypes.createDouble())), DataTypes.createMapType(DataTypes.DoubleType, DataTypes.DoubleType, true));
    }

    @Test
    public void testStructTypeConversion()
    {
        StructType target = new StructType();
        target.add("a", DataTypes.DoubleType);
        target.add("b", DataTypes.StringType);

        Map<String, Object> typeMap = new HashMap<String, Object>();
        typeMap.put("type", "STRUCTTYPE");
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("a", DataSurfaceJsonTypes.createDouble());
        fields.put("b", DataSurfaceJsonTypes.createString(20));
        typeMap.put("fields", fields);

        assertEquals(SparkTypeConversion.getSparkDataType(typeMap), target);
    }
}
