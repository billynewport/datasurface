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
        a = {}
        a["self"] = a
        b = {}
        b["self"] = b
        self.assertTrue(cyclic_safe_eq(a, b, set()))

if __name__ == '__main__':
    unittest.main()