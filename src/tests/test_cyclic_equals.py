"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


from typing import Any
import unittest

from datasurface.md import cyclic_safe_eq


class TestCyclicSafeEq(unittest.TestCase):
    def test_same_object(self):
        obj = {"key": "value"}
        self.assertTrue(cyclic_safe_eq(obj, obj, set()))

    def test_different_types(self):
        self.assertFalse(cyclic_safe_eq(1, "1", set()))

    def test_same_dict(self):
        self.assertTrue(cyclic_safe_eq({"key": "value"}, {"key": "value"}, set()))

    def test_different_dict(self):
        self.assertFalse(cyclic_safe_eq({"key": "value"}, {"key": "other value"}, set()))

    def test_cyclic_dict(self):
        a: dict[str, Any] = {}
        a["self"] = a
        b: dict[str, Any] = {}
        b["self"] = b
        self.assertTrue(cyclic_safe_eq(a, b, set()))

    def test_empty_dict(self):
        self.assertTrue(cyclic_safe_eq({}, {}, set()))

    def test_empty_set(self):
        self.assertTrue(cyclic_safe_eq({1, 2, 3}, {1, 2, 3}, set()))

    def test_different_set(self):
        self.assertFalse(cyclic_safe_eq({1, 2, 3}, {4, 5, 6}, set()))

    def test_nested_dict(self):
        self.assertTrue(cyclic_safe_eq({"key": {"nested_key": "value"}}, {"key": {"nested_key": "value"}}, set()))

    def test_nested_dict_different_value(self):
        self.assertFalse(cyclic_safe_eq({"key": {"nested_key": "value"}}, {"key": {"nested_key": "other value"}}, set()))


if __name__ == '__main__':
    unittest.main()
