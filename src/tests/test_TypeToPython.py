"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest

from datasurface.md.types import Vector, Binary, Variant, Boolean, NChar, Char, String, VarChar
from datasurface.md.types import NVarChar, Interval, Date, Timestamp, Decimal, FP8_E4M3FNUZ


class Test_ColumnCodeGen(unittest.TestCase):

    def test_ColumnCodeGen(self):
        self.assertEqual(str(Vector(10)), "Vector(10)")
        self.assertEqual(str(Binary(10)), "Binary(10)")
        self.assertEqual(str(Variant(10)), "Variant(10)")
        self.assertEqual(str(Variant()), "Variant()")
        self.assertEqual(str(Boolean()), "Boolean()")

        self.assertEqual(str(NChar()), "NChar()")
        self.assertEqual(str(NChar(10)), "NChar(10)")
        self.assertEqual(str(NChar(10, "latin-1")), "NChar(10, 'latin-1')")

        self.assertEqual(str(Char()), "Char()")
        self.assertEqual(str(Char(10)), "Char(10)")
        self.assertEqual(str(Char(10, "latin-1")), "Char(10, 'latin-1')")

        self.assertEqual(str(String()), 'String()')
        self.assertEqual(str(String(10)), 'String(10)')
        self.assertEqual(str(String(10, 'latin-1')), "String(10, 'latin-1')")
        self.assertEqual(str(String(None, 'latin-1')), "String(collationString='latin-1')")

        self.assertEqual(str(VarChar()), 'VarChar()')
        self.assertEqual(str(VarChar(10)), 'VarChar(10)')
        self.assertEqual(str(VarChar(10, 'latin-1')), "VarChar(10, 'latin-1')")
        self.assertEqual(str(VarChar(None, 'latin-1')), "VarChar(collationString='latin-1')")

        self.assertEqual(str(NVarChar()), 'NVarChar()')
        self.assertEqual(str(NVarChar(10)), 'NVarChar(10)')
        self.assertEqual(str(NVarChar(10, 'latin-1')), "NVarChar(10, 'latin-1')")
        self.assertEqual(str(NVarChar(None, 'latin-1')), "NVarChar(collationString='latin-1')")

        self.assertEqual(str(Interval()), 'Interval()')
        self.assertEqual(str(Date()), 'Date()')
        self.assertEqual(str(Timestamp()), 'Timestamp()')

        self.assertEqual(str(Decimal(10, 2)), 'Decimal(10,2)')

        # The other floating types are similar to this
        self.assertEqual(str(FP8_E4M3FNUZ()), 'FP8_E4M3FNUZ()')
