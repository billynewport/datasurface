"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.credential import Credential, CredentialType
from datasurface.md import CronTrigger, LocationKey, \
        Dataset, DatasetGroup, Datastore, Ecosystem, GovernanceZone, \
        IngestionConsistencyType, Team, \
        Workspace, HostPortPair, DataPlatform, DataPlatformKey, DataPlatformChooser, DatasetSink, DataTransformer, PythonCodeArtifact
from datasurface.md.governance import DSGDataPlatformAssignment, DatasetGroupDataPlatformAssignments, DatasetGroupDataPlatformMappingStatus, ProductionStatus
from datasurface.md.governance import DeprecationsAllowed, SQLSnapshotIngestion
from datasurface.md import DataContainer, PostgresDatabase

from datasurface.md.policy import SimpleDC, SimpleDCTypes
from datasurface.md.schema import DDLColumn, DDLTable, NullableStatus, PrimaryKeyStatus
from datasurface.md.types import IEEE32, Date, Integer, VarChar, Variant, BigInt, Boolean, Geography, GeometryType, SpatialReferenceSystem


def defineTables(eco: Ecosystem, index: int, gz: GovernanceZone, t: Team):
    t.add(
        Datastore(
            f"WWI_Data_{index}",
            documentation=PlainTextDocumentation(f"WWI_Data_{index} is a datastore that contains the Wide World Importers database"),
            capture_metadata=SQLSnapshotIngestion(
                PostgresDatabase(
                    "WWI_DB",
                    hostPort=HostPortPair("hostName", 5432),
                    locations={LocationKey("MyCorp:USA/NY_1")},
                    databaseName="WideWorldImporters"
                ),
                CronTrigger("WWI_Data Every 15 mins", "0,15,30,45 * * * *"),
                IngestionConsistencyType.MULTI_DATASET,
                Credential("wwi_cred", CredentialType.USER_PASSWORD),
                ),
            datasets=[
                # Application Schema Tables
                Dataset(
                    "cities",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("city_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("city_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("state_province_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("location", Geography(geometryType=GeometryType.POINT, srs=SpatialReferenceSystem(4326, "WGS84"))),
                            DDLColumn("latest_recorded_population", BigInt()),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "countries",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("country_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("country_name", VarChar(60), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("formal_name", VarChar(60), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("iso_alpha3_code", VarChar(3)),
                            DDLColumn("iso_numeric_code", Integer()),
                            DDLColumn("country_type", VarChar(20)),
                            DDLColumn("latest_recorded_population", BigInt()),
                            DDLColumn("continent", VarChar(30), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("region", VarChar(30), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("subregion", VarChar(30), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("border", Geography(geometryType=GeometryType.MULTIPOLYGON, srs=SpatialReferenceSystem(4326, "WGS84"))),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "state_provinces",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("state_province_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("state_province_code", VarChar(5), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("state_province_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("country_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("sales_territory", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("border", Geography(geometryType=GeometryType.MULTIPOLYGON, srs=SpatialReferenceSystem(4326, "WGS84"))),
                            DDLColumn("latest_recorded_population", BigInt()),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "people",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("full_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("preferred_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("search_name", VarChar(101), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("is_permitted_to_logon", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("logon_name", VarChar(50)),
                            DDLColumn("is_external_logon_provider", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("hashed_password", Variant()),
                            DDLColumn("is_system_user", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("is_employee", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("is_salesperson", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("user_preferences", VarChar()),
                            DDLColumn("phone_number", VarChar(20)),
                            DDLColumn("fax_number", VarChar(20)),
                            DDLColumn("email_address", VarChar(256)),
                            DDLColumn("photo", Variant()),
                            DDLColumn("custom_fields", VarChar()),
                            DDLColumn("other_languages", VarChar()),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PC3)],
                    documentation=PlainTextDocumentation("This data includes people information including employees and contacts. Contains PII data.")
                ),
                Dataset(
                    "delivery_methods",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("delivery_method_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("delivery_method_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "payment_methods",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("payment_method_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("payment_method_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "transaction_types",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("transaction_type_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("transaction_type_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "system_parameters",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("system_parameter_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("delivery_address_line_1", VarChar(60), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_address_line_2", VarChar(60)),
                            DDLColumn("delivery_city_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_postal_code", VarChar(10), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("postal_address_line_1", VarChar(60), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("postal_address_line_2", VarChar(60)),
                            DDLColumn("postal_city_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("postal_postal_code", VarChar(10), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("application_settings", VarChar(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),

                # Sales Schema Tables
                Dataset(
                    "customers",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("customer_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customer_name", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("bill_to_customer_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("customer_category_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("buying_group_id", Integer()),
                            DDLColumn("primary_contact_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("alternate_contact_person_id", Integer()),
                            DDLColumn("delivery_method_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_city_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("postal_city_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("credit_limit", IEEE32()),
                            DDLColumn("account_opened_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("standard_discount_percentage", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("is_statement_sent", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("is_on_credit_hold", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("payment_days", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("phone_number", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("fax_number", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_run", VarChar(5)),
                            DDLColumn("run_position", VarChar(5)),
                            DDLColumn("website_url", VarChar(256), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_address_line_1", VarChar(60), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_address_line_2", VarChar(60)),
                            DDLColumn("delivery_postal_code", VarChar(10), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("postal_address_line_1", VarChar(60), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("postal_address_line_2", VarChar(60)),
                            DDLColumn("postal_postal_code", VarChar(10), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PC3)],
                    documentation=PlainTextDocumentation("Customer information including contact details and credit information. Contains PII data.")
                ),
                Dataset(
                    "customer_categories",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("customer_category_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customer_category_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "buying_groups",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("buying_group_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("buying_group_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "orders",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("order_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customer_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("salesperson_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("picked_by_person_id", Integer()),
                            DDLColumn("contact_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("backorder_order_id", Integer()),
                            DDLColumn("order_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("expected_delivery_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("customer_purchase_order_number", VarChar(20)),
                            DDLColumn("is_undersupply_backordered", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("comments", VarChar()),
                            DDLColumn("delivery_instructions", VarChar()),
                            DDLColumn("internal_comments", VarChar()),
                            DDLColumn("picking_completed_when", Date()),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "order_lines",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("order_line_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("order_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("stock_item_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("description", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("package_type_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("quantity", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("unit_price", IEEE32()),
                            DDLColumn("tax_rate", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("picked_quantity", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("picking_completed_when", Date()),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "invoices",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("invoice_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customer_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("bill_to_customer_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("order_id", Integer()),
                            DDLColumn("delivery_method_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("contact_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("accounts_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("salesperson_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("packed_by_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("invoice_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("customer_purchase_order_number", VarChar(20)),
                            DDLColumn("is_credit_note", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("credit_note_reason", VarChar()),
                            DDLColumn("comments", VarChar()),
                            DDLColumn("delivery_instructions", VarChar()),
                            DDLColumn("internal_comments", VarChar()),
                            DDLColumn("total_dry_items", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("total_chiller_items", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_run", VarChar(5)),
                            DDLColumn("run_position", VarChar(5)),
                            DDLColumn("returned_delivery_data", VarChar()),
                            DDLColumn("confirmed_delivery_time", Date()),
                            DDLColumn("confirmed_received_by", VarChar(4000)),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "invoice_lines",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("invoice_line_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("invoice_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("stock_item_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("description", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("package_type_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("quantity", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("unit_price", IEEE32()),
                            DDLColumn("tax_rate", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("tax_amount", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("line_profit", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("extended_price", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "customer_transactions",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("customer_transaction_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("customer_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("transaction_type_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("invoice_id", Integer()),
                            DDLColumn("payment_method_id", Integer()),
                            DDLColumn("transaction_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("amount_excluding_tax", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("tax_amount", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("transaction_amount", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("outstanding_balance", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("finalization_date", Date()),
                            DDLColumn("is_finalized", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "special_deals",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("special_deal_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("stock_item_id", Integer()),
                            DDLColumn("customer_id", Integer()),
                            DDLColumn("buying_group_id", Integer()),
                            DDLColumn("customer_category_id", Integer()),
                            DDLColumn("stock_group_id", Integer()),
                            DDLColumn("deal_description", VarChar(30), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("start_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("end_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("discount_amount", IEEE32()),
                            DDLColumn("discount_percentage", IEEE32()),
                            DDLColumn("unit_price", IEEE32()),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),

                # Purchasing Schema Tables
                Dataset(
                    "suppliers",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("supplier_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("supplier_name", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("supplier_category_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("primary_contact_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("alternate_contact_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_method_id", Integer()),
                            DDLColumn("delivery_city_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("postal_city_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("supplier_reference", VarChar(20)),
                            DDLColumn("bank_account_name", VarChar(50)),
                            DDLColumn("bank_account_branch", VarChar(50)),
                            DDLColumn("bank_account_code", VarChar(20)),
                            DDLColumn("bank_account_number", VarChar(20)),
                            DDLColumn("bank_international_code", VarChar(20)),
                            DDLColumn("payment_days", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("internal_comments", VarChar()),
                            DDLColumn("phone_number", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("fax_number", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("website_url", VarChar(256), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_address_line_1", VarChar(60), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_address_line_2", VarChar(60)),
                            DDLColumn("delivery_postal_code", VarChar(10), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("postal_address_line_1", VarChar(60), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("postal_address_line_2", VarChar(60)),
                            DDLColumn("postal_postal_code", VarChar(10), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PC3)],
                    documentation=PlainTextDocumentation("Supplier information including contact details and banking information. Contains some PII data.")
                ),
                Dataset(
                    "supplier_categories",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("supplier_category_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("supplier_category_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "purchase_orders",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("purchase_order_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("supplier_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("order_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("delivery_method_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("contact_person_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("expected_delivery_date", Date()),
                            DDLColumn("supplier_reference", VarChar(20)),
                            DDLColumn("is_order_finalized", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("comments", VarChar()),
                            DDLColumn("internal_comments", VarChar()),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "purchase_order_lines",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("purchase_order_line_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("purchase_order_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("stock_item_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("ordered_outers", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("description", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("received_outers", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("package_type_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("expected_unit_price_per_outer", IEEE32()),
                            DDLColumn("last_receipt_date", Date()),
                            DDLColumn("is_order_line_finalized", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "supplier_transactions",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("supplier_transaction_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("supplier_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("transaction_type_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("purchase_order_id", Integer()),
                            DDLColumn("payment_method_id", Integer()),
                            DDLColumn("supplier_invoice_number", VarChar(20)),
                            DDLColumn("transaction_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("amount_excluding_tax", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("tax_amount", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("transaction_amount", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("outstanding_balance", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("finalization_date", Date()),
                            DDLColumn("is_finalized", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),

                # Warehouse Schema Tables
                Dataset(
                    "stock_items",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("stock_item_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("stock_item_name", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("supplier_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("color_id", Integer()),
                            DDLColumn("unit_package_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("outer_package_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("brand", VarChar(50)),
                            DDLColumn("size", VarChar(20)),
                            DDLColumn("lead_time_days", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("quantity_per_outer", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("is_chiller_stock", Boolean(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("barcode", VarChar(50)),
                            DDLColumn("tax_rate", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("unit_price", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("recommended_retail_price", IEEE32()),
                            DDLColumn("typical_weight_per_unit", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("marketing_comments", VarChar()),
                            DDLColumn("internal_comments", VarChar()),
                            DDLColumn("photo", Variant()),
                            DDLColumn("custom_fields", VarChar()),
                            DDLColumn("tags", VarChar()),
                            DDLColumn("search_details", VarChar(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "stock_item_holdings",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("stock_item_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("quantity_on_hand", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("bin_location", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_stocktake_quantity", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_cost_price", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("reorder_level", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("target_stock_level", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "stock_groups",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("stock_group_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("stock_group_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "stock_item_stock_groups",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("stock_item_stock_group_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("stock_item_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("stock_group_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "colors",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("color_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("color_name", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "package_types",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("package_type_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("package_type_name", VarChar(50), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "stock_item_transactions",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("stock_item_transaction_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("stock_item_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("transaction_type_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("customer_id", Integer()),
                            DDLColumn("invoice_id", Integer()),
                            DDLColumn("supplier_id", Integer()),
                            DDLColumn("purchase_order_id", Integer()),
                            DDLColumn("transaction_occurred_when", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("quantity", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_by", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("last_edited_when", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "vehicle_temperatures",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("vehicle_temperature_id", BigInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("vehicle_registration", VarChar(20), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("chiller_sensor_number", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("recorded_when", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("temperature", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("full_sensor_data", VarChar())
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                ),
                Dataset(
                    "cold_room_temperatures",
                    schema=DDLTable(
                        columns=[
                            DDLColumn("cold_room_temperature_id", BigInt(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                            DDLColumn("cold_room_sensor_number", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("recorded_when", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("temperature", IEEE32(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_from", Date(), nullable=NullableStatus.NOT_NULLABLE),
                            DDLColumn("valid_to", Date(), nullable=NullableStatus.NOT_NULLABLE)
                        ]
                    ),
                    classifications=[SimpleDC(SimpleDCTypes.PUB)]
                )
            ]
        )
    )


def addDSGPlatformMappingForWorkspace(eco: Ecosystem, workspace: Workspace, dsg: DatasetGroup, dp: DataPlatform):
    """Add a DSG platform mapping for a workspace/dsg pair, gets set to the current chooser for the dsg"""
    if eco.dsgPlatformMappings.get(f"{workspace.name}#{dsg.name}") is None:
        eco.dsgPlatformMappings[f"{workspace.name}#{dsg.name}"] = DatasetGroupDataPlatformAssignments(
            workspace=workspace.name,
            dsgName=dsg.name,
            assignments=[
                DSGDataPlatformAssignment(
                    workspace=workspace.name,
                    dsgName=dsg.name,
                    dp=DataPlatformKey(dp.name),
                    doc=PlainTextDocumentation("Test docs"),
                    productionStatus=ProductionStatus.PRODUCTION,
                    deprecationsAllowed=DeprecationsAllowed.NEVER,
                    status=DatasetGroupDataPlatformMappingStatus.PROVISIONED)]
        )
    else:
        eco.dsgPlatformMappings[f"{workspace.name}#{dsg.name}"].assignments.append(
            DSGDataPlatformAssignment(
                workspace=workspace.name,
                dsgName=dsg.name,
                dp=DataPlatformKey(dp.name),
                doc=PlainTextDocumentation("Test docs"),
                productionStatus=ProductionStatus.PRODUCTION,
                deprecationsAllowed=DeprecationsAllowed.NEVER, status=DatasetGroupDataPlatformMappingStatus.PROVISIONED))


def defineWorkspaces(eco: Ecosystem, index: int, t: Team, locations: set[LocationKey], chooser: DataPlatformChooser, dp: DataPlatform):
    """Create Workspaces for Wide World Importers scenarios"""

    # Warehouse for Workspaces
    ws_db: DataContainer = PostgresDatabase(
        f"WWI_Workspace_{index}",
        hostPort=HostPortPair("localhost", 5432),
        locations=locations,
        databaseName="wwi_workspace"
    )

    # Sales Analysis Workspace
    w: Workspace = Workspace(
        f"SalesAnalytics_{index}",
        ws_db,
        DatasetGroup(
            "SalesData",
            platform_chooser=chooser,
            sinks=[
                DatasetSink(f"WWI_Data_{index}", "customers"),
                DatasetSink(f"WWI_Data_{index}", "orders"),
                DatasetSink(f"WWI_Data_{index}", "order_lines"),
                DatasetSink(f"WWI_Data_{index}", "invoices"),
                DatasetSink(f"WWI_Data_{index}", "invoice_lines"),
                DatasetSink(f"WWI_Data_{index}", "customer_transactions")
            ]
        ))
    t.add(w)
    addDSGPlatformMappingForWorkspace(eco, w, w.dsgs["SalesData"], dp)

    # Inventory Management Workspace
    w = Workspace(
        f"InventoryManagement_{index}",
        ws_db,
        DatasetGroup(
            "InventoryData",
            platform_chooser=chooser,
            sinks=[
                DatasetSink(f"WWI_Data_{index}", "stock_items"),
                DatasetSink(f"WWI_Data_{index}", "stock_item_holdings"),
                DatasetSink(f"WWI_Data_{index}", "stock_groups"),
                DatasetSink(f"WWI_Data_{index}", "stock_item_stock_groups"),
                DatasetSink(f"WWI_Data_{index}", "stock_item_transactions"),
                DatasetSink(f"WWI_Data_{index}", "suppliers"),
                DatasetSink(f"WWI_Data_{index}", "purchase_orders"),
                DatasetSink(f"WWI_Data_{index}", "purchase_order_lines")
            ]
        ))
    t.add(w)
    addDSGPlatformMappingForWorkspace(eco, w, w.dsgs["InventoryData"], dp)

    # Customer Analytics Workspace (with masked customer data)
    w = Workspace(
        f"CustomerAnalytics_{index}",
        ws_db,
        DatasetGroup(
            "CustomerInsights",
            sinks=[
                DatasetSink(f"WWI_Data_{index}", "customers"),
                DatasetSink(f"WWI_Data_{index}", "customer_categories"),
                DatasetSink(f"WWI_Data_{index}", "buying_groups"),
                DatasetSink(f"WWI_Data_{index}", "cities"),
                DatasetSink(f"WWI_Data_{index}", "state_provinces"),
                DatasetSink(f"WWI_Data_{index}", "countries")
            ]
        ),
        DataTransformer(
            "MaskCustomerData",
            Datastore(
                f"Masked_WWI_Data_{index}",
                datasets=[
                    Dataset(
                        "customers_masked",
                        schema=DDLTable(
                            documentation=PlainTextDocumentation("Masked version of customers table with PII data removed or anonymized"),
                            columns=[
                                DDLColumn("customer_id", Integer(), nullable=NullableStatus.NOT_NULLABLE, primary_key=PrimaryKeyStatus.PK),
                                DDLColumn("customer_name", VarChar(100), nullable=NullableStatus.NOT_NULLABLE),
                                DDLColumn("customer_category_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                                DDLColumn("delivery_city_id", Integer(), nullable=NullableStatus.NOT_NULLABLE),
                                DDLColumn("credit_limit", IEEE32()),
                                DDLColumn("account_opened_date", Date(), nullable=NullableStatus.NOT_NULLABLE),
                                DDLColumn("standard_discount_percentage", IEEE32(), nullable=NullableStatus.NOT_NULLABLE)
                            ]
                        ),
                        classifications=[SimpleDC(SimpleDCTypes.PUB)]
                    )
                ]
            ),
            PythonCodeArtifact([], {}, "3.11")
            )
        )
    t.add(w)

    # IoT Monitoring Workspace
    w = Workspace(
        f"IoTMonitoring_{index}",
        ws_db,
        DatasetGroup(
            "IoTSensorData",
            platform_chooser=chooser,
            sinks=[
                DatasetSink(f"WWI_Data_{index}", "vehicle_temperatures"),
                DatasetSink(f"WWI_Data_{index}", "cold_room_temperatures")
            ]
        ))
    t.add(w)
    addDSGPlatformMappingForWorkspace(eco, w, w.dsgs["IoTSensorData"], dp)
