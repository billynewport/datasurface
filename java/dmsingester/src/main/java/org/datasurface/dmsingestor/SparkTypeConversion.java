// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
package org.datasurface.dmsingestor;


import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.expr;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class converts DataSurface types to Spark types. This is used when creating a Spark schema from a DataSurface schema. The DataSurface
 * schema is fetched from the GRPC service. It contains the schema for each dataset. The schema includes column schema info, primary and partition
 * key column names. DataSurface encoded datatypes as JSON formatted strings.
 * Not all DataSurface column types are supported by Spark, these are mostly the AI/ML types which don't have Spark type equivalents.
 * 
 * This also adds 2 new columns to a spark schema for the hashs which are materielized calculated values.
  */
public class SparkTypeConversion {

    private static final Logger logger = Logger.getLogger(SparkTypeConversion.class);

    // This takes a DataSurface type string and converts it to a Spark DataType
    static public DataType getSparkDataType(String dataSurfaceType)
    {
        try
        {
            // DataSurfacer type string is a json encoding of the python DataType.
            ObjectMapper mapper = new ObjectMapper();
            @SuppressWarnings("unchecked")
            Map<String, Object> typeMap = (Map<String, Object>) mapper.readValue(dataSurfaceType, Map.class);
            return getSparkDataType(typeMap);
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    // Convert a DataSurface Json encoded type to a Spark DataType
    static public DataType getSparkDataType(Map<String, Object> typeMap)
    {
        String rootType = typeMap.get("type").toString().toUpperCase();
        
        switch (rootType)
        {
            case "CHAR":
            case "NCHAR":
            case "STRING":
            case "VARCHAR":
            case "NVARCHAR":
                return DataTypes.StringType;
            case "TINYINT":
                return DataTypes.ByteType;
            case "SMALLINT":
                return DataTypes.ShortType;
            case "INTEGER":
                return DataTypes.IntegerType;
            case "BIGINT":
                return DataTypes.LongType;
            case "FLOAT":
            case "IEEE32":
                return DataTypes.FloatType;
            case "DOUBLE":
            case "IEEE64":
                return DataTypes.DoubleType;
            case "BOOLEAN":
                return DataTypes.BooleanType;
            case "DATE":
                return DataTypes.DateType;
            case "TIMESTAMP":
                return DataTypes.TimestampType;
            case "BINARY":
            case "VARIANT":
                return DataTypes.BinaryType;
            case "ARRAYTYPE":
                return getSparkArrayType(typeMap);
            case "STRUCTTYPE":
                return getSparkStructType(typeMap);
            case "MAPTYPE":
                return getSparkMapType(typeMap);
            case "DECIMAL":
                // Parse out precision and scale from Decimal(precision,scale)
                return getSparkDecimalType(typeMap);
            default:
                logger.error("Unknown data type: " + typeMap);
                throw new RuntimeException("Unknown data type: " + typeMap);
        }
    }

    // Maps are encoded as a MAP(keytype,valuetype) string. Example keytypes might be STRING(20) and value type
    // might be STRING(50) or DECIMAL(10,2)
    static public DataType getSparkMapType(Map<String, Object> dataSurfaceType)
    {
        @SuppressWarnings("unchecked")
        Map<String, Object> keyType = (Map<String, Object>) dataSurfaceType.get("keyType");
        @SuppressWarnings("unchecked")
        Map<String, Object> valueType = (Map<String, Object>) dataSurfaceType.get("valueType");

        return DataTypes.createMapType(getSparkDataType(keyType), getSparkDataType(valueType));
    }

    // DataSurface encodes structs as STRUCTTYPE({'field1':'Type1', 'field2':'Type2'}) where field is the field name
    static public DataType getSparkStructType(Map<String, Object> dataSurfaceType)
    {
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) dataSurfaceType.get("fields");

        StructType struct = new StructType();
        // Iterator over the entries in fields and add them to the struct
        for (Map.Entry<String, Object> e : fields.entrySet())
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> fieldType = (Map<String, Object>) e.getValue();
            struct.add(e.getKey(), getSparkDataType(fieldType), true);
        }
        return struct;
    }

    // DataSurface Arrays are encoded as ARRAYTYPE(maxSize,elementtype) where maxSize is the maximum size of the array
    static public DataType getSparkArrayType(Map<String, Object> dataSurfaceType)
    {
        @SuppressWarnings("unchecked")
        Map<String, Object> dataType = (Map<String, Object>) dataSurfaceType.get("dataType");

        return DataTypes.createArrayType(getSparkDataType(dataType), true);
    }

    // This converts a DataSurface DecimalType spec to a Spark Decimal Type
    // The original type is in the Decimal(P,S) format with P equal to precision
    // and s the scale digits.
    static public DataType getSparkDecimalType(Map<String, Object> dataSurfaceType)
    {
        int maxSize = Integer.parseInt(dataSurfaceType.get("maxSize").toString());
        int scale = Integer.parseInt(dataSurfaceType.get("precision").toString());
        return DataTypes.createDecimalType(maxSize, scale);
    }

    /** This returns the equivalent Spark Schema for the specified dataset
     */
    public static StructType calculateSparkSchemaForDataset(datasurface.Api.Dataset dataset)
    {
        StructType schema = new StructType();

        datasurface.Api.DDLSchema ddlSchema = dataset.getSchema().getDdlSchema();

        // Convert the DDL schema to a Spark schema by looping over attributes and converting
        // them to the equivalent spark datatype column
        for (datasurface.Api.DDLColumn attribute : ddlSchema.getColumnsList())
        {
            schema = schema.add(attribute.getName(), SparkTypeConversion.getSparkDataType(attribute.getType()), attribute.getIsNullable()); // name, type, nullable
        }
        return schema;
    }

    /**
        This create a schema for a Dataset with the additional Op CDC column added.
    */
    public static StructType calculateSparkSchemaForCDCDataset(datasurface.Api.Dataset dataset)
    {
        StructType schema = new StructType();
        schema = schema.add(OP_COLUMN, DataTypes.StringType, false); // first column is the Op Column, either I, U or D.

        datasurface.Api.DDLSchema ddlSchema = dataset.getSchema().getDdlSchema();

        // Convert the DDL schema to a Spark schema by looping over attributes and converting
        // them to the equivalent spark datatype column
        for (datasurface.Api.DDLColumn attribute : ddlSchema.getColumnsList())
        {
            schema = schema.add(attribute.getName(), SparkTypeConversion.getSparkDataType(attribute.getType()), attribute.getIsNullable()); // name, type, nullable
        }
        return schema;
    }

    // The name of the extra column containing the hash of the primary keys
    static public final String PK_HASH_COL_NAME = "ds_key_hash";

    // The name of the extra column containing the hash of every column in the record
    static public final String ALL_COLS_HASH_COL_NAME = "ds_all_cols_hash";

    // The name of the CDC column that AWS DMS adds to the data, it contains an I, U or D
    static public final String OP_COLUMN = "Op";

    /** This adds 2 internal columns for performance reasons. These allow 2 records to be compared quickly
        by primary key as well as to compare all columns for changes. We just use the appropriate hash rather
        than an AND equals for every column between the 2 records
    */
    public static StructType createIceBergSchemaForDataset(datasurface.Api.Dataset dataset)
    {
        StructType schema = SparkTypeConversion.calculateSparkSchemaForDataset(dataset);
        // This is a generated column with the md5 hash of all primary key columns
        // if there is no pk then ALL columns are used
        schema = schema.add(PK_HASH_COL_NAME, DataTypes.StringType, false);
        // This is a generated column with the md5 hash of all columns in it.
        schema = schema.add(ALL_COLS_HASH_COL_NAME, DataTypes.StringType, false);
        return schema;
    }

    /** This adds a hash column to the DataFrame for the specified columns. It filters
        out the Op column. Only data columns should be included in this hash
    */
    public static Dataset<Row> addHashForColumns(Dataset<Row> df, String hashColName, String[] colsToHash)
    {
        String columns = Arrays.stream(colsToHash)
            .filter(column -> !column.equals(OP_COLUMN))
            .map(column -> ("coalesce(cast(" + column + " as string), '')"))
            .collect(Collectors.joining(", "));
        df = df.withColumn(hashColName, functions.md5(expr(String.format("concat(%s)", columns))));
        return df;
    }

    /** This adds 2 internal columns to a staging file. These columns are added using
        a calculated value of the md5 hash of the pk and all columns
        This needs to be done to ALL staging files as these hash columns are used
        for performance reasons when MERGING the data into the Iceberg table. We can check
        record equality and group by PK using a single column rather than ALL columns
    */
    public static Dataset<Row> addMaterializedHashesToDataFrame(datasurface.Api.Dataset dataset, Dataset<Row> df)
    {
        // Save original columns, we don't want to include the PK hash column in the all_cols hash
        String[] origDFCols = df.columns();
        // Concatenate all primary key columns into a single string
        String[] pkCols = (String[])dataset.getSchema().getPrimaryKeysList().toArray(new String[0]);
        df = addHashForColumns(df, PK_HASH_COL_NAME, pkCols);

        // Concatenate all columns into a single string
        df = addHashForColumns(df, ALL_COLS_HASH_COL_NAME, origDFCols);
        return df;
    }
}
