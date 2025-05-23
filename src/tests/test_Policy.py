"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.policy import AllowDisallowPolicy, Literal


class Test_Policy(unittest.TestCase):
    def test_allow_disallow_policy(self):
        # Test case 1: Object is explicitly allowed
        allowed_values = {Literal(1), Literal(2), Literal(3)}
        policy = AllowDisallowPolicy("Test Policy", PlainTextDocumentation("Test"), allowed=allowed_values)
        self.assertTrue(policy.isCompatible(Literal(2)))

        # Test case 2: Object is explicitly forbidden
        not_allowed_values = {Literal(4), Literal(5), Literal(6)}
        policy = AllowDisallowPolicy("Test Policy", PlainTextDocumentation("Test"), notAllowed=not_allowed_values)
        self.assertFalse(policy.isCompatible(Literal(4)))

        # Test case 3: Object is neither explicitly allowed nor forbidden
        allowed_values = {Literal(1), Literal(2), Literal(3)}
        not_allowed_values = {Literal(4), Literal(5), Literal(6)}
        policy = AllowDisallowPolicy("Test Policy", PlainTextDocumentation("Test"), allowed=allowed_values, notAllowed=not_allowed_values)
        self.assertFalse(policy.isCompatible(Literal(7)))

        # Test case 4: Object is both explicitly allowed and forbidden
        allowed_values = {Literal(1), Literal(2), Literal(3)}
        not_allowed_values = {Literal(3), Literal(4), Literal(5)}
        try:
            policy = AllowDisallowPolicy("Test Policy", PlainTextDocumentation("Test"), allowed=allowed_values, notAllowed=not_allowed_values)
            self.fail("Exception not thrown for overlapping allow/disallow sets")
        except Exception:
            # Test is succesful
            pass
