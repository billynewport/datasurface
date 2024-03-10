# Introduction

DataSurface has been designed with an extensible data type system. The system is designed to support the traditional data types such as strings, integers, dates and times. It also supports the multiple of floating point formats (low precision floating point types) widely using in machine learning and artificial intelligence research. The genesis of this design comes from how LLVM deals with types. The DataSurface types system is designed to describe types precisely and allow them to be compared with each other in terms of can type A be represented by type B.

It does this by introducing type meta attributes. For example, an integer type has a notion of how many bits are available and is it signed. A floating type number has a number of bits assigned to the mantissa and exponent and whether its signed and so on.

The type system is designed to be extensible. It's possible to add new types to the system. This is useful for adding new types of data that are not currently supported by the system. For example, a new type of floating point number could be added to the system. This would allow the system to support new types of data that are not currently supported.

## Schema attributes

Schema attributes use this DataType system. DataSurface supports traditional DDL like schemas and Avro like schemas and can be extended to others. Data Containers will offer a set of supported built in types and the data types used by data producers will need to be mapped on to the data container type system.

## Producer schemas are usually defined in terms of the producer data container type system

Producers register schemas which are usually as close as possible to the producers origin data container for this data. Some data containers have more expressive types than others, for example, maximum length of a string.

A DataPlatform with internal storage will need to be able to project the producer schemas on to their internal schema representation and data types. This projection may be lossy. For example, such a schema may not include length limits on strings. If the Dataplatform schema was later used to materielize the data in a relational database then this would be an issue because such databases usually require a maximum lenght for a string and this was lost.

Alternatively, a data producer may be using a columnar store such as AWS Glue Tables which don't store the maximum length of strings. If this data needs to be materielized in a SQL database then the maximum length of the string will need to be inferred from the data. This is not an exact science.

## Data Containers have limitations on schemas

Similarly, data containers have limitations. A SQL Database may limit strings to 64k characters. If a producer schema has a string with a maximum length of 100k characters then this will need to be truncated when it's projected on to the SQL database schema and the consumer may be forced to acklowedge this truncation or the DataPlatform will refuse to hydrate the Workspace for the consumer.
