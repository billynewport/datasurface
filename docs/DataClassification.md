# Data Classifications

This document explains data classification and how the ecosystem can be governed using policies controlling what data can be stored where.

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

## Marking attributes on schemas with data classification tags

Schemas are collections of records with identical schemas. The attributes/columns defined in the schema can be marked with data classification values. This can be done on a column by column basis. It can also be overridden on the Dataset classification. If a Dataset has a dataclassification then all columns in the schema will also used that classification even if the column is explicitly tagged. These unneeded tags on columns will be flagged as unnecessary during lint.

## Governance Zone Classification Policies

Governance Zones own teams which own Datastore/datasets. A Governance Zone can specify a DataClassificationPolicy which can control which types of data can be stored within the zone. This allows a Governance Zone to specify that (PC1,PC2,PC3) tagged attributes are not allowed. Governance Zones can also limit vendors whose products store data. For example, a Governance Zone may be defined to restrict the storage of PC1,PC2,PC3 data to internal servers only. Those teams/datastores defined within such a Zone are impacted by the Zone policy. A firm might setup two or more GovernanceZones to seperate data with different privacy levels.
