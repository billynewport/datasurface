import unittest
from datasurface.md.Documentation import PlainTextDocumentation

from datasurface.md.Policy import AllowDisallowPolicy


class Test_Policy(unittest.TestCase):
    def test_allow_disallow_policy(self):
        # Test case 1: Object is explicitly allowed
        allowed_values = {1, 2, 3}
        policy = AllowDisallowPolicy("Test Policy", PlainTextDocumentation("Test"), allowed=allowed_values)
        self.assertTrue(policy.isCompatible(2))

        # Test case 2: Object is explicitly forbidden
        not_allowed_values = {4, 5, 6}
        policy = AllowDisallowPolicy("Test Policy", PlainTextDocumentation("Test"), notAllowed=not_allowed_values)
        self.assertFalse(policy.isCompatible(4))

        # Test case 3: Object is neither explicitly allowed nor forbidden
        allowed_values = {1, 2, 3}
        not_allowed_values = {4, 5, 6}
        policy = AllowDisallowPolicy("Test Policy", PlainTextDocumentation("Test"), allowed=allowed_values, notAllowed=not_allowed_values)
        self.assertFalse(policy.isCompatible(7))

        # Test case 4: Object is both explicitly allowed and forbidden
        allowed_values = {1, 2, 3}
        not_allowed_values = {3, 4, 5}
        try:
            policy = AllowDisallowPolicy("Test Policy", PlainTextDocumentation("Test"), allowed=allowed_values, notAllowed=not_allowed_values)
            self.fail("Exception not thrown for overlapping allow/disallow sets")
        except Exception:
            # Test is succesful
            pass
