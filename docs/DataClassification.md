# Data Classifications

This document explains data classification and how the ecosystem can be governed using policies controlling what data can be stored where. Most firms will have at least one way of encoding what type of data is stored in a column or table/record. Thus, the data classification system of DataSurface is designed to handle this.

How can a firm have multiple data classification systems?

* A firm may have had siloed efforts previously and it's been decided to integrate them. Here, there may be N different classification schemes which can be migrated automatically or manually.
* A firm may have tens of thousands of datasets and been encoding their classifications diligently. If an org change occurs and it's decided a newer better data classification scheme is desired then the problem is how to migrate. This is unlikely to happen in one step. It may take weeks or months if the new classifications have to be reviewed and approved. In the meantime, you have 2 ways of classifying data, the old way and the new way. Indeed, org changes may occur before the migration completes...

When defining a new data classification scheme, thinking about automatic conversions from the old to the new is important. Thinking about how long a data classification scheme lasts in terms of months/years is important also. The firm should have a policy of how often such a migration will be tolerated as part of a contract with producers/consumers using the catalog.

## Data classification types

Data classifications are per attribute/column. They specify the privacy level of that column.

* PUB
* IP
* PC1
* PC2
* CPI
* CSI
* MNPI
* PC3

### PUB or Public data

This is the least restrictive data classification. Data tagged with this can be freely shared

### IP Internally Public

This is data that can be shared throughout an enterprise but not disclosed publicly outside the enterprise

### PC1 Personal connfidential information

### PC2

The is more sensitive that PC1. Names, address, email addresses and telephone numbers.

### CPI

This is corporate information which can be shared within an enterprise on a need to know basis.

### MNPI Non materiel public information

This is data about public companies which is known to the enterprise but cannot be disclosed publicly. This data should be shared within the enterprise
on a need to know basis.

### CSI

### PC3

This is personal confidential information such as bank account numbers, credit card numbers, passport or national identity data, social security numbers.

## Associating a set of DataClassifications with an element

Why have a set of data classifications with a single element such as a Column or a Table? Columns may be a VarChar but can encode a json document or similar. A single classification may not be sufficient to describe whats happening in the Column JSON.

Tables are the same thing, when classifying a table, it's better to say this table has PII and MNPI information in it rather than having to choose either PII or MNPI.

## Marking attributes on schemas with data classification tags

Schemas are collections of records with identical schemas. The attributes/columns defined in the schema can be marked with a set of data classification values. This can be done on a column by column basis. It can also be overridden on the Dataset classification. If a Dataset has set of dataclassifications then all columns in the schema will also used that classifications even if the column is explicitly tagged. These unneeded tags on columns will be flagged as unnecessary during lint.

## Governance Zone Classification Policies

Governance Zones own teams which own Datastore/datasets. A Governance Zone can specify a DataClassificationPolicy which can control which types of data can be stored within the zone. This allows a Governance Zone to specify that (PC1,PC2,PC3) tagged attributes are not allowed. Governance Zones can also limit vendors whose products store data. For example, a Governance Zone may be defined to restrict the storage of PC1,PC2,PC3 data to internal servers only. Those teams/datastores defined within such a Zone are impacted by the Zone policy. A firm might setup two or more GovernanceZones to seperate data with different privacy levels.
