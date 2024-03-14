# Introduction

DataSurface has been designed with an extensible data type system. The system is designed to support the traditional data types such as strings, integers, dates and times as well as arrays, maps and structures. It also supports the multiple of floating point formats (low precision floating point types) widely using in machine learning and artificial intelligence research. The genesis of this design comes from how LLVM deals with types. The DataSurface types system is designed to describe types precisely and allow them to be compared with each other in terms of can type A be represented by type B.

It does this by introducing type meta attributes. For example, an integer type has a notion of how many bits are available and is it signed. A floating type number has a number of bits assigned to the mantissa and exponent and whether its signed and so on.

The type system is designed to be extensible. It's possible to add new types to the system. This is useful for adding new types of data that are not currently supported by the system. For example, a new type of floating point number could be added to the system. This would allow the system to support new types of data that are not currently supported.

## Schema attributes

Schema attributes use this DataType system. DataSurface supports traditional DDL like schemas and Avro like schemas and can be extended to others. Data Containers will offer a set of supported built in types and the data types used by data producers will need to be mapped on to the data container type system.

## Producer schemas are usually defined in terms of the producer data container type system

Producers register schemas which are usually as close as possible to the producers origin data container for this data. Some data containers have more expressive types than others, for example, maximum length of a string.

A DataPlatform with internal storage will need to be able to project the producer schemas on to their internal schema representation and data types. This projection may be lossy. For example, such a schema may not include length limits on strings. If the Dataplatform schema was later used to materielize the data in a relational database then this would be an issue because such databases usually require a maximum length for a string and this was lost. For this reason, it may be necessary for a producer to specify a string maximum length when their data is stored in a data container which does not have a string limit. If this isn't done then a consumer will need to compensate by specifying a maximum length to truncate strings to when the data is projected on to a SQL database. The producer is the best person to specify this maximum length as they are the ones who know the data best.

Alternatively, a data producer may be using a columnar store such as AWS Glue Tables which don't store the maximum length of strings. If this data needs to be materielized in a SQL database then the maximum length of the string will need to be inferred from the data. This is not an exact science.

## Data Containers have limitations on schemas

Similarly, data containers have limitations. A SQL Database may limit strings to 64k characters. If a producer schema has a string with a maximum length of 100k characters then this will need to be truncated when it's projected on to the SQL database schema and the consumer may be forced to acklowedge this truncation or the DataPlatform will refuse to hydrate the Workspace for the consumer.

Names are similarly constrained. Some DataContainers may have shorter limits than the producer data container. Maybe, it's a legacy database with restrictions based on its reserved words, characters allowed and so on. This means that when the names used for Datasets/Workspaces/Attributes are 'mapped' on to those objects on a data container, the platform will need to figure out a compatible alias which is permissible on that container that doesn't clash with other dataplatforms also placed potentially the same datasets with a different SLA on the same data container.

## Schema depends on the context of the caller

DataSurface supports a tabular style schema call DDLTable and an Avro schema with a record as the primary attribute. The DDLTable is designed to be a close representation of a SQL table. Both schemas can specify primary keys and primary partition attributes for ingestion.

DataSurface will provide caller specific schema support. Caller context is key to returning the correct schema. For example, a data platform may have a schema for a SQL database and a slightly different schema for an iceberg table stored in AWS Glue. The data platform will need to be able to produce both these schemas from producer schema as well a highlight issues with the mapping. Thus, a dataset may have several schemas associated with it but always has a single authorative schema which is provided by the data producer. The other schemas are derivative schemas generated from the authorative schema to support the restrictions of a specific data container.
