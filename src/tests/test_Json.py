"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

# These tests are used to test the JSON serialization and deserialization of the DSL objects.

import unittest
from datasurface.md import Ecosystem, GovernanceZone, Datastore, Team, Workspace
from datasurface.md.types import DataType, ArrayType, Boolean, MapType, StructType, SmallInt, IEEE64, Timestamp, Date, Decimal, VarChar, NVarChar

from tests.nwdb.eco import createEcosystem
from typing import Any
from collections import OrderedDict


class TestDataStoreJSON(unittest.TestCase):
    def test_DataStoreJSON(self):
        pass

    def test_DataStoreJSON_from_json(self):
        eco: Ecosystem = createEcosystem()
        gzUSA: GovernanceZone = eco.getZoneOrThrow("USA")
        nwTeam: Team = gzUSA.getTeamOrThrow("NorthWindTeam")
        nwData: Datastore = nwTeam.dataStores["NW_Data"]
        json: dict[str, Any] = nwData.to_json()
        # Check that the attributes are present
        self.assertTrue("_type" in json)
        self.assertTrue("name" in json)
        self.assertTrue("doc" in json)

        # Check that the datasets are present
        self.assertTrue("datasets" in json)
        datasets: dict[str, Any] = json["datasets"]
        self.assertTrue("us_states" in datasets)
        self.assertTrue("customers" in datasets)

        # Check us_states is valid
        us_states: dict[str, Any] = datasets["us_states"]
        self.assertTrue("originalSchema" in us_states)
        schema: dict[str, Any] = us_states["originalSchema"]
        self.assertTrue("columns" in schema)
        self.assertTrue("state_id" in schema["columns"])
        self.assertTrue("state_name" in schema["columns"])
        self.assertTrue("state_abbr" in schema["columns"])

        # Check customers is valid
        customers: dict[str, Any] = datasets["customers"]
        self.assertTrue("originalSchema" in customers)
        schema: dict[str, Any] = customers["originalSchema"]
        self.assertTrue("columns" in schema)
        self.assertTrue("customer_id" in schema["columns"])
        self.assertTrue("company_name" in schema["columns"])
        self.assertTrue("contact_name" in schema["columns"])
        self.assertTrue("contact_title" in schema["columns"])

        # Check each columns type is correct

        # customer_id is VarChar(5), not nullable, primary key
        customer_id: dict[str, Any] = schema["columns"]["customer_id"]
        # type is a dict with a "type" key
        customer_id_type: dict[str, Any] = customer_id["type"]
        self.assertEqual(customer_id_type["type"], "VarChar")
        self.assertEqual(customer_id_type["maxSize"], 5)
        self.assertEqual(customer_id["nullable"], False)
        self.assertEqual(customer_id["primaryKey"], True)

        # company_name is VarChar(40), NullableStatus.NOT_NULLABLE, Not primary key
        company_name: dict[str, Any] = schema["columns"]["company_name"]
        company_name_type: dict[str, Any] = company_name["type"]
        self.assertEqual(company_name_type["type"], "VarChar")
        self.assertEqual(company_name_type["maxSize"], 40)
        self.assertEqual(company_name["nullable"], False)
        self.assertEqual(company_name["primaryKey"], False)

        # contact_name is VarChar(30), NullableStatus.NULLABLE, Not primary key
        contact_name: dict[str, Any] = schema["columns"]["contact_name"]
        contact_name_type: dict[str, Any] = contact_name["type"]
        self.assertEqual(contact_name_type["type"], "VarChar")
        self.assertEqual(contact_name_type["maxSize"], 30)
        self.assertEqual(contact_name["nullable"], True)
        self.assertEqual(contact_name["primaryKey"], False)

        # contact_title is VarChar(30), NullableStatus.NULLABLE, Not primary key
        contact_title: dict[str, Any] = schema["columns"]["contact_title"]
        contact_title_type: dict[str, Any] = contact_title["type"]
        self.assertEqual(contact_title_type["type"], "VarChar")
        self.assertEqual(contact_title_type["maxSize"], 30)
        self.assertEqual(contact_title["nullable"], True)
        self.assertEqual(contact_title["primaryKey"], False)

        # address is VarChar(60), NullableStatus.NULLABLE, Not primary key
        address: dict[str, Any] = schema["columns"]["address"]
        address_type: dict[str, Any] = address["type"]
        self.assertEqual(address_type["type"], "VarChar")
        self.assertEqual(address_type["maxSize"], 60)
        self.assertEqual(address["nullable"], True)
        self.assertEqual(address["primaryKey"], False)

        # city is VarChar(15), NullableStatus.NULLABLE, Not primary key
        city: dict[str, Any] = schema["columns"]["city"]
        city_type: dict[str, Any] = city["type"]
        self.assertEqual(city_type["type"], "VarChar")
        self.assertEqual(city_type["maxSize"], 15)
        self.assertEqual(city["nullable"], True)
        self.assertEqual(city["primaryKey"], False)

        # region is VarChar(15), NullableStatus.NULLABLE, Not primary key
        region: dict[str, Any] = schema["columns"]["region"]
        region_type: dict[str, Any] = region["type"]
        self.assertEqual(region_type["type"], "VarChar")
        self.assertEqual(region_type["maxSize"], 15)
        self.assertEqual(region["nullable"], True)
        self.assertEqual(region["primaryKey"], False)

        # postal_code is VarChar(10), NullableStatus.NULLABLE, Not primary key
        postal_code: dict[str, Any] = schema["columns"]["postal_code"]
        postal_code_type: dict[str, Any] = postal_code["type"]
        self.assertEqual(postal_code_type["type"], "VarChar")
        self.assertEqual(postal_code_type["maxSize"], 10)
        self.assertEqual(postal_code["nullable"], True)
        self.assertEqual(postal_code["primaryKey"], False)

        # country is VarChar(15), NullableStatus.NULLABLE, Not primary key
        country: dict[str, Any] = schema["columns"]["country"]
        country_type: dict[str, Any] = country["type"]
        self.assertEqual(country_type["type"], "VarChar")
        self.assertEqual(country_type["maxSize"], 15)
        self.assertEqual(country["nullable"], True)
        self.assertEqual(country["primaryKey"], False)

        # phone is VarChar(24), NullableStatus.NULLABLE, Not primary key
        phone: dict[str, Any] = schema["columns"]["phone"]
        phone_type: dict[str, Any] = phone["type"]
        self.assertEqual(phone_type["type"], "VarChar")
        self.assertEqual(phone_type["maxSize"], 24)
        self.assertEqual(phone["nullable"], True)
        self.assertEqual(phone["primaryKey"], False)

        # fax is VarChar(24), NullableStatus.NULLABLE, Not primary key
        fax: dict[str, Any] = schema["columns"]["fax"]
        fax_type: dict[str, Any] = fax["type"]
        self.assertEqual(fax_type["type"], "VarChar")
        self.assertEqual(fax_type["maxSize"], 24)
        self.assertEqual(fax["nullable"], True)
        self.assertEqual(fax["primaryKey"], False)

    def test_WorkspaceJSON(self):
        eco: Ecosystem = createEcosystem()
        gzUSA: GovernanceZone = eco.getZoneOrThrow("USA")
        nwTeam: Team = gzUSA.getTeamOrThrow("NorthWindTeam")
        workspace: Workspace = nwTeam.workspaces["ProductLiveAdhocReporting"]
        workspaceJSON: dict[str, Any] = workspace.to_json()

        # Grab the Workspace ProductLiveAdhocReporting
        self.assertTrue("name" in workspaceJSON)
        self.assertEqual(workspaceJSON["name"], "ProductLiveAdhocReporting")

        # Check the DatasetGroup LiveProducts is present
        self.assertTrue("datasetGroups" in workspaceJSON)
        datasetGroups: dict[str, Any] = workspaceJSON["datasetGroups"]
        self.assertTrue("LiveProducts" in datasetGroups)
        liveProducts: dict[str, Any] = datasetGroups["LiveProducts"]
        self.assertTrue("name" in liveProducts)
        self.assertEqual(liveProducts["name"], "LiveProducts")

        # Check the DatasetSink NW_Data is present
        self.assertTrue("sinks" in liveProducts)
        sinks: dict[str, Any] = liveProducts["sinks"]

        # Check there is a sink for NW_Data#products, NW_Data#customers and NW_Data#suppliers
        products: dict[str, Any] = sinks["NW_Data:products"]
        self.assertTrue("storeName" in products)
        self.assertEqual(products["storeName"], "NW_Data")
        self.assertTrue("datasetName" in products)
        self.assertEqual(products["datasetName"], "products")
        self.assertTrue("deprecationsAllowed" in products)
        self.assertEqual(products["deprecationsAllowed"], "NEVER")

        # Check there is a sink for NW_Data#customers
        customers: dict[str, Any] = sinks["NW_Data:customers"]
        self.assertTrue("storeName" in customers)
        self.assertEqual(customers["storeName"], "NW_Data")
        self.assertTrue("datasetName" in customers)
        self.assertEqual(customers["datasetName"], "customers")
        self.assertTrue("deprecationsAllowed" in customers)
        self.assertEqual(customers["deprecationsAllowed"], "NEVER")

        # Check there is a sink for NW_Data#suppliers
        suppliers: dict[str, Any] = sinks["NW_Data:suppliers"]
        self.assertTrue("storeName" in suppliers)
        self.assertEqual(suppliers["storeName"], "NW_Data")
        self.assertTrue("datasetName" in suppliers)
        self.assertEqual(suppliers["datasetName"], "suppliers")
        self.assertTrue("deprecationsAllowed" in suppliers)
        self.assertEqual(suppliers["deprecationsAllowed"], "NEVER")

        # Check there is a fixed chooser for LegacyA
        self.assertTrue("platformMD" in liveProducts)
        chooser: dict[str, Any] = liveProducts["platformMD"]
        self.assertTrue("dataPlatformName" in chooser)
        self.assertEqual(chooser["dataPlatformName"], "LegacyA")
        self.assertEqual(chooser["_type"], "LegacyDataPlatformChooser")

    def test_EveryDataTypeHasJSON(self):
        # Check bounded ArrayType
        t: DataType = ArrayType(10, Boolean())
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "ArrayType")
        self.assertEqual(json["maxSize"], 10)
        childType: dict[str, Any] = json["elementType"]
        self.assertEqual(childType["type"], "Boolean")

        # Check Decimal
        t = Decimal(10, 2)
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "Decimal")
        self.assertEqual(json["maxSize"], 10)
        self.assertEqual(json["precision"], 2)

        # Check VarChar with no collation string
        t = VarChar(10)
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "VarChar")
        self.assertEqual(json["maxSize"], 10)
        self.assertTrue("collationString" not in json)

        # Check NVarChar with collation string
        t = NVarChar(10, "collation")
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "NVarChar")
        self.assertEqual(json["maxSize"], 10)
        self.assertEqual(json["collationString"], "collation")

        # Check SmallInt, TinyInt, Integer, BigInt, all are similar
        t = SmallInt()
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "SmallInt")
        self.assertEqual(json["sizeInBits"], 16)
        self.assertEqual(json["isSigned"], "SIGNED")

        # Check unbounded ArrayType
        t = ArrayType(None, SmallInt())
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "ArrayType")
        self.assertEqual(json["maxSize"], -1)
        childType: dict[str, Any] = json["elementType"]
        self.assertEqual(childType["type"], "SmallInt")

        # Check IEEE64 and all simple CustomFloats
        t = IEEE64()
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "IEEE64")
        self.assertEqual(json["maxExponent"], 1023)
        self.assertEqual(json["minExponent"], -1022)
        self.assertEqual(json["precision"], 53)
        self.assertEqual(json["sizeInBits"], 64)
        self.assertEqual(json["nonFiniteBehavior"], "IEEE754")
        self.assertEqual(json["nanEncoding"], "IEEE")

        # Check MapType
        t = MapType(SmallInt(), IEEE64())
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "MapType")
        keyType: dict[str, Any] = json["keyType"]
        self.assertEqual(keyType["type"], "SmallInt")
        valueType: dict[str, Any] = json["valueType"]
        self.assertEqual(valueType["type"], "IEEE64")

        # Check StructType
        t = StructType(
            OrderedDict(
                [
                    ("a", SmallInt()),
                    ("b", IEEE64()),
                ]
            )
        )
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "StructType")
        fields: list[dict[str, Any]] = json["fields"]
        self.assertEqual(fields[0]["name"], "a")
        fieldType: dict[str, Any] = fields[0]["type"]
        self.assertEqual(fieldType["type"], "SmallInt")
        self.assertEqual(fields[1]["name"], "b")
        fieldType: dict[str, Any] = fields[1]["type"]
        self.assertEqual(fieldType["type"], "IEEE64")

        # Check Boolean
        t = Boolean()
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "Boolean")

        # Check Timestamp
        t = Timestamp()
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "Timestamp")

        # Check Date
        t = Date()
        json: dict[str, Any] = t.to_json()
        self.assertEqual(json["type"], "Date")
