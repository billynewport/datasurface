"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""


import unittest
from datasurface.md.documentation import PlainTextDocumentation
from datasurface.md.policy import AllowDisallowPolicy, Literal


class Test_Policy(unittest.TestCase):
    def test_allow_disallow_policy(self) -> None:
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

    def test_allow_disallow_policy_edge_cases(self) -> None:
        """Test edge cases for isCompatible method"""
        doc = PlainTextDocumentation("Test")

        # Test case 1: Only notAllowed is set and object is NOT in notAllowed list
        not_allowed_values = {Literal(1), Literal(2)}
        policy = AllowDisallowPolicy("Test Policy", doc, notAllowed=not_allowed_values)
        self.assertTrue(policy.isCompatible(Literal(3)))  # Should return True

        # Test case 2: Both allowed and notAllowed are None (default behavior)
        policy: AllowDisallowPolicy[Literal[int]] = AllowDisallowPolicy("Test Policy", doc)
        self.assertTrue(policy.isCompatible(Literal(1)))  # Should return True

        # Test case 3: Empty sets for both allowed and notAllowed (same as None - no restrictions)
        empty_allowed_set: set[Literal[int]] = set()
        empty_not_allowed_set: set[Literal[int]] = set()
        policy = AllowDisallowPolicy("Test Policy", doc, allowed=empty_allowed_set, notAllowed=empty_not_allowed_set)
        self.assertTrue(policy.isCompatible(Literal(1)))  # Empty sets = no restrictions = allow

        # Test case 4: Empty allowed set with some notAllowed items (empty allowed = no restriction)
        empty_allowed: set[Literal[int]] = set()
        policy = AllowDisallowPolicy("Test Policy", doc, allowed=empty_allowed, notAllowed={Literal(1)})
        self.assertTrue(policy.isCompatible(Literal(2)))  # Empty allowed = no restriction, obj not in notAllowed
        self.assertFalse(policy.isCompatible(Literal(1)))  # Empty allowed = no restriction, but obj in notAllowed

        # Test case 5: Some allowed items with empty notAllowed set
        empty_not_allowed: set[Literal[int]] = set()
        policy = AllowDisallowPolicy("Test Policy", doc, allowed={Literal(1), Literal(2)}, notAllowed=empty_not_allowed)
        self.assertTrue(policy.isCompatible(Literal(1)))
        self.assertFalse(policy.isCompatible(Literal(3)))

        # Test case 6: Demonstrating the difference between empty set and None vs populated sets
        # With populated allowed set, only listed items are allowed
        policy_with_allowed = AllowDisallowPolicy("Test", doc, allowed={Literal(1), Literal(2)})
        self.assertTrue(policy_with_allowed.isCompatible(Literal(1)))   # In allowed list
        self.assertFalse(policy_with_allowed.isCompatible(Literal(3)))  # Not in allowed list

    def test_allow_disallow_policy_complex_scenarios(self) -> None:
        """Test more complex scenarios combining allowed and notAllowed lists"""
        doc = PlainTextDocumentation("Test")

        # Object is in allowed and notAllowed is also set (but obj not in notAllowed)
        allowed_values = {Literal(1), Literal(2)}
        not_allowed_values = {Literal(3), Literal(4)}
        policy = AllowDisallowPolicy("Test Policy", doc, allowed=allowed_values, notAllowed=not_allowed_values)
        self.assertTrue(policy.isCompatible(Literal(1)))  # In allowed, not in notAllowed
        self.assertFalse(policy.isCompatible(Literal(3)))  # Not in allowed, in notAllowed
        self.assertFalse(policy.isCompatible(Literal(5)))  # Not in allowed, not in notAllowed

        # Test with different data types
        allowed_strings = {Literal("a"), Literal("b")}
        not_allowed_strings = {Literal("c"), Literal("d")}
        string_policy = AllowDisallowPolicy("String Policy", doc, allowed=allowed_strings, notAllowed=not_allowed_strings)
        self.assertTrue(string_policy.isCompatible(Literal("a")))
        self.assertFalse(string_policy.isCompatible(Literal("c")))
        self.assertFalse(string_policy.isCompatible(Literal("z")))

    def test_allow_disallow_policy_hash(self) -> None:
        """Test __hash__ method"""
        doc = PlainTextDocumentation("Test")
        policy1 = AllowDisallowPolicy("Test Policy", doc, allowed={Literal(1)})
        policy2 = AllowDisallowPolicy("Test Policy", doc, allowed={Literal(2)})
        policy3 = AllowDisallowPolicy("Different Policy", doc, allowed={Literal(1)})

        # Policies with same name should have same hash
        self.assertEqual(hash(policy1), hash(policy2))
        # Policies with different names should have different hash
        self.assertNotEqual(hash(policy1), hash(policy3))

    def test_allow_disallow_policy_equality(self) -> None:
        """Test __eq__ method"""
        doc = PlainTextDocumentation("Test")
        allowed_values = {Literal(1), Literal(2)}
        not_allowed_values = {Literal(3), Literal(4)}

        policy1 = AllowDisallowPolicy("Test Policy", doc, allowed=allowed_values, notAllowed=not_allowed_values)

        # TODO I cannot explain why I though equality ignoring actual allow/disallow was correct. Disable for now.
        #       policy2 = AllowDisallowPolicy("Test Policy", doc, allowed={Literal(5)}, notAllowed={Literal(6)})
        #       policy3 = AllowDisallowPolicy("Different Policy", doc, allowed=allowed_values, notAllowed=not_allowed_values)

        # Policies with same name should be equal regardless of allowed/notAllowed sets
#        self.assertEqual(policy1, policy2)
        # Policies with different names should not be equal
#        self.assertNotEqual(policy1, policy3)

        # Test equality with non-AllowDisallowPolicy objects
        self.assertNotEqual(policy1, "not a policy")
        self.assertNotEqual(policy1, None)
        self.assertNotEqual(policy1, 42)

    def test_allow_disallow_policy_str(self) -> None:
        """Test __str__ method"""
        doc = PlainTextDocumentation("Test")
        allowed_values = {Literal(1), Literal(2)}
        not_allowed_values = {Literal(3), Literal(4)}

        policy = AllowDisallowPolicy("Test Policy", doc, allowed=allowed_values, notAllowed=not_allowed_values)
        str_repr = str(policy)
        self.assertIn("AllowDisallowPolicy", str_repr)
        self.assertIn("Test Policy", str_repr)

        # Test with None values
        policy_none: AllowDisallowPolicy[Literal[int]] = AllowDisallowPolicy("None Policy", doc)
        str_repr_none = str(policy_none)
        self.assertIn("AllowDisallowPolicy", str_repr_none)
        self.assertIn("None Policy", str_repr_none)

    def test_allow_disallow_policy_to_json(self) -> None:
        """Test to_json method"""
        doc = PlainTextDocumentation("Test")
        allowed_values = {Literal(1), Literal(2)}
        not_allowed_values = {Literal(3), Literal(4)}

        # Test with both allowed and notAllowed
        policy = AllowDisallowPolicy("Test Policy", doc, allowed=allowed_values, notAllowed=not_allowed_values)
        json_repr = policy.to_json()
        self.assertEqual(json_repr["_type"], "AllowDisallowPolicy")
        self.assertEqual(json_repr["name"], "Test Policy")
        self.assertIn("allowed", json_repr)
        self.assertIn("notAllowed", json_repr)
        self.assertEqual(len(json_repr["allowed"]), 2)
        self.assertEqual(len(json_repr["notAllowed"]), 2)

        # Test with only allowed
        policy_allowed_only = AllowDisallowPolicy("Allowed Only", doc, allowed=allowed_values)
        json_allowed_only = policy_allowed_only.to_json()
        self.assertIn("allowed", json_allowed_only)
        self.assertNotIn("notAllowed", json_allowed_only)

        # Test with only notAllowed
        policy_not_allowed_only = AllowDisallowPolicy("Not Allowed Only", doc, notAllowed=not_allowed_values)
        json_not_allowed_only = policy_not_allowed_only.to_json()
        self.assertNotIn("allowed", json_not_allowed_only)
        self.assertIn("notAllowed", json_not_allowed_only)

        # Test with neither (None values)
        policy_neither: AllowDisallowPolicy[Literal[int]] = AllowDisallowPolicy("Neither", doc)
        json_neither = policy_neither.to_json()
        self.assertNotIn("allowed", json_neither)
        self.assertNotIn("notAllowed", json_neither)
        self.assertEqual(json_neither["_type"], "AllowDisallowPolicy")
        self.assertEqual(json_neither["name"], "Neither")

    def test_allow_disallow_policy_documentation_variations(self) -> None:
        """Test policy creation with different documentation parameters"""
        # Test with None documentation
        policy_no_doc = AllowDisallowPolicy("No Doc Policy", None, allowed={Literal(1)})
        self.assertEqual(policy_no_doc.name, "No Doc Policy")
        self.assertTrue(policy_no_doc.isCompatible(Literal(1)))

        # Test with documentation
        doc = PlainTextDocumentation("Test documentation")
        policy_with_doc = AllowDisallowPolicy("With Doc Policy", doc, allowed={Literal(1)})
        self.assertEqual(policy_with_doc.name, "With Doc Policy")
        self.assertTrue(policy_with_doc.isCompatible(Literal(1)))

    def test_allow_disallow_policy_overlap_validation(self) -> None:
        """Test comprehensive overlap validation scenarios"""
        doc = PlainTextDocumentation("Test")

        # Test multiple overlapping values
        allowed_values = {Literal(1), Literal(2), Literal(3), Literal(4)}
        not_allowed_values = {Literal(3), Literal(4), Literal(5), Literal(6)}
        with self.assertRaises(Exception) as context:
            AllowDisallowPolicy("Overlap Policy", doc, allowed=allowed_values, notAllowed=not_allowed_values)
        self.assertIn("overlap", str(context.exception).lower())

        # Test single overlapping value
        allowed_single = {Literal(1)}
        not_allowed_single = {Literal(1)}
        with self.assertRaises(Exception):
            AllowDisallowPolicy("Single Overlap", doc, allowed=allowed_single, notAllowed=not_allowed_single)

        # Test no overlap (should succeed)
        allowed_no_overlap = {Literal(1), Literal(2)}
        not_allowed_no_overlap = {Literal(3), Literal(4)}
        policy = AllowDisallowPolicy("No Overlap", doc, allowed=allowed_no_overlap, notAllowed=not_allowed_no_overlap)
        self.assertIsNotNone(policy)
