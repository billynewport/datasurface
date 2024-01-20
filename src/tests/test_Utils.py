import unittest
from datasurface.md.Governance import GitHubRepository
from datasurface.md.Lint import ValidationTree

from datasurface.md.utils import is_valid_hostname_or_ip, is_valid_sql_identifier

class Test_Utils(unittest.TestCase):
    def test_Valid_IP_Address(self):
        self.assertTrue(is_valid_hostname_or_ip("12.12.12.12"))

        self.assertTrue(is_valid_hostname_or_ip("www.google.com"))

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
        valid_URLs : list[str] = [ 
            "https://github.com/billynewport/datasurface.git",
            "git@github.com:billynewport/datasurface.git"
            ]

        for url in valid_URLs:
            r : GitHubRepository = GitHubRepository(url, "moduleName")
            tree : ValidationTree = ValidationTree(r)
            r.lint(tree)
            self.assertFalse(tree.hasErrors())
            
        # Valid HTTPS GitHub URLs        
