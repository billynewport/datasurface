"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest
from datasurface.md.repo import GitHubRepository
from datasurface.md import ValidationTree

from datasurface.md import is_valid_hostname_or_ip, is_valid_sql_identifier, validate_cron_string


class Test_Utils(unittest.TestCase):
    def test_Valid_IP_Address(self):
        self.assertTrue(is_valid_hostname_or_ip("12.12.12.12"))  # IP v4
        self.assertTrue(is_valid_hostname_or_ip("2001:0db8:85a3:0000:0000:8a2e:0370:7334"))  # IP v6

        self.assertTrue(is_valid_hostname_or_ip("www.google.com"))  # hostname
        self.assertTrue(is_valid_hostname_or_ip("pg-data.ns_kub_pg_test.svc.cluster.local"))  # Kubernetes DNS with underscore

        self.assertFalse(is_valid_hostname_or_ip("192.0.2.1.1"))
        self.assertFalse(is_valid_hostname_or_ip("2001:0db8:85a3:0000:0000:8a2e:0370:7334:"))

    def test_ANSI_SQL_Identifier(self):
        # Valid identifiers
        self.assertTrue(is_valid_sql_identifier("a"))
        self.assertTrue(is_valid_sql_identifier("A"))
        self.assertTrue(is_valid_sql_identifier("a1"))
        self.assertTrue(is_valid_sql_identifier("a_1"))
        self.assertTrue(is_valid_sql_identifier("A_1"))
        self.assertTrue(is_valid_sql_identifier("identifier"))
        self.assertTrue(is_valid_sql_identifier("IDENTIFIER"))
        self.assertTrue(is_valid_sql_identifier("Identifier"))
        self.assertTrue(is_valid_sql_identifier("identifier_1"))
        self.assertTrue(is_valid_sql_identifier("identifier_1_2_3"))

        # Invalid identifiers
        self.assertFalse(is_valid_sql_identifier(""))
        self.assertFalse(is_valid_sql_identifier("1a"))
        self.assertFalse(is_valid_sql_identifier("a-b"))
        self.assertFalse(is_valid_sql_identifier("a b"))
        self.assertFalse(is_valid_sql_identifier("select"))

    def test_GitHubUrl(self):
        # Valid SSH GitHub URLs
        valid_URLs: list[str] = [
            "billynewport/repo"
            ]

        for url in valid_URLs:
            r: GitHubRepository = GitHubRepository(url, "moduleName")
            tree: ValidationTree = ValidationTree(r)
            r.lint(tree)
            self.assertFalse(tree.hasErrors())

        # Valid HTTPS GitHub URLs


class TestCronStringValidator(unittest.TestCase):
    def test_valid_cron_strings(self):
        self.assertTrue(validate_cron_string('* * * * *'))
        self.assertTrue(validate_cron_string('0 0 1 1 *'))
        self.assertTrue(validate_cron_string('1,15,30 * * * *'))
        self.assertTrue(validate_cron_string('0-59 0-23 1-31 1-12 0-7'))
        self.assertTrue(validate_cron_string('*/5 * * * *'))  # Every 5 notation supported

    def test_invalid_cron_strings(self):
        self.assertFalse(validate_cron_string('invalid cron string'))
        self.assertFalse(validate_cron_string('* * * *'))  # Not enough fields
        self.assertFalse(validate_cron_string('* * * * * *'))  # Too many fields
        self.assertFalse(validate_cron_string('60 * * * *'))  # Minute out of range
        self.assertFalse(validate_cron_string('* 24 * * *'))  # Hour out of range
        self.assertFalse(validate_cron_string('* * 32 * *'))  # Day of month out of range
        self.assertFalse(validate_cron_string('* * * 13 *'))  # Month out of range
        self.assertFalse(validate_cron_string('* * * * 8'))  # Day of week out of range
        self.assertFalse(validate_cron_string('1-2-3 * * * *'))  # Invalid range
        self.assertFalse(validate_cron_string('1,2,3, * * * *'))  # Invalid list
        self.assertFalse(validate_cron_string('*/60 * * * *'))  # Minute out of range
