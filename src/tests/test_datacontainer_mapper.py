"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest

from datasurface.platforms.azure.Azure import SQLServerNamingMapper
from datasurface.md.Governance import DataContainerNamingMapper


class Test_DataContainerMapper(unittest.TestCase):
    def test_IdentifierTruncation(self):
        mapper: DataContainerNamingMapper = SQLServerNamingMapper()
        self.assertEqual(mapper.truncateIdentifier("this_is_a_test", 64), "this_is_a_test")
        self.assertEqual(mapper.truncateIdentifier("this_is_a_test", 8), "this_505")
        self.assertEqual(mapper.truncateIdentifier("this_is_a_different_test", 8), "this_3f7")
        self.assertEqual(mapper.truncateIdentifier("this_is_aanother_different_test", 8), "this_ed8")
