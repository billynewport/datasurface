"""
Test utilities to ensure __eq__ methods cover all instance attributes.
This helps prevent bugs where new attributes are added but forgotten in equality checks.
"""

import inspect
import unittest
import textwrap
from typing import Any, Set, Type, get_type_hints, Optional, List
from dataclasses import fields, is_dataclass
import ast


def get_instance_attributes(cls: Type[Any]) -> Set[str]:
    """Extract all instance attributes from a class by examining __init__ method."""
    attributes: Set[str] = set()

    # Get attributes from __init__ method
    if hasattr(cls, '__init__'):
        try:
            init_source = inspect.getsource(cls.__init__)
            # Remove leading indentation to make it parseable
            init_source = textwrap.dedent(init_source)
            tree = ast.parse(init_source)

            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if (isinstance(target, ast.Attribute) and
                                isinstance(target.value, ast.Name) and
                                target.value.id == 'self'):
                            attributes.add(target.attr)
        except (OSError, TypeError, SyntaxError):
            # Could not get or parse source code, skip this method
            pass

    # Also check type hints
    try:
        hints = get_type_hints(cls)
        attributes.update(hints.keys())
    except (NameError, TypeError, AttributeError):
        # Type hints may reference undefined names or have other issues
        pass

    # For dataclasses, get field names
    if is_dataclass(cls):
        try:
            attributes.update(field.name for field in fields(cls))
        except (TypeError, AttributeError):
            pass

    return attributes


def get_eq_method_attributes(cls: Type[Any]) -> Set[str]:
    """Extract attributes referenced in the __eq__ method."""
    if not hasattr(cls, '__eq__'):
        return set()

    try:
        eq_source = inspect.getsource(cls.__eq__)
        # Remove leading indentation to make it parseable
        eq_source = textwrap.dedent(eq_source)
        tree = ast.parse(eq_source)
        attributes: Set[str] = set()

        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                if (isinstance(node.value, ast.Name) and
                        node.value.id in ('self', 'other')):
                    attributes.add(node.attr)

        return attributes
    except (OSError, TypeError, SyntaxError):
        # Could not get or parse source code
        return set()


def calls_super_eq(cls: Type[Any]) -> bool:
    """
    Check if the class's __eq__ method calls any base class __eq__ methods.
    This includes both super().__eq__() calls and explicit BaseClass.__eq__(self, other) calls.
    """
    called_base_classes = get_explicitly_called_base_classes(cls)
    return len(called_base_classes) > 0


def get_explicitly_called_base_classes(cls: Type[Any]) -> Set[Type[Any]]:
    """
    Get base classes whose __eq__ methods are explicitly called in the class's __eq__ method.
    This handles both super().__eq__() and explicit BaseClass.__eq__(self, other) patterns.
    """
    called_base_classes: Set[Type[Any]] = set()

    if not hasattr(cls, '__eq__'):
        return called_base_classes

    try:
        eq_source = inspect.getsource(cls.__eq__)
        eq_source = textwrap.dedent(eq_source)
        tree = ast.parse(eq_source)

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Check for super().__eq__() calls
                if (isinstance(node.func, ast.Attribute) and
                        node.func.attr == '__eq__' and
                        isinstance(node.func.value, ast.Call) and
                        isinstance(node.func.value.func, ast.Name) and
                        node.func.value.func.id == 'super'):
                    # For super() calls, add the next class in MRO that has __eq__
                    for base_cls in cls.__mro__[1:]:  # Skip self
                        if (hasattr(base_cls, '__eq__') and
                                base_cls.__eq__ is not object.__eq__ and
                                '__eq__' in base_cls.__dict__):
                            called_base_classes.add(base_cls)
                            break  # super() only calls the next one in MRO

                # Check for explicit BaseClass.__eq__(self, other) calls
                elif (isinstance(node.func, ast.Attribute) and
                        node.func.attr == '__eq__' and
                        isinstance(node.func.value, ast.Name)):
                    class_name = node.func.value.id
                    # Find the base class with this name
                    for base_cls in cls.__mro__[1:]:  # Skip self
                        if (base_cls.__name__ == class_name and
                                hasattr(base_cls, '__eq__') and
                                base_cls.__eq__ is not object.__eq__ and
                                '__eq__' in base_cls.__dict__):
                            called_base_classes.add(base_cls)
                            break

        return called_base_classes
    except (OSError, TypeError, SyntaxError):
        return set()


def get_all_inherited_eq_attributes(cls: Type[Any]) -> Set[str]:
    """
    Get all attributes handled by base classes whose __eq__ methods are actually called.
    This properly handles both super().__eq__() and explicit BaseClass.__eq__() patterns.
    """
    inherited_attributes: Set[str] = set()

    # Get the base classes whose __eq__ methods are explicitly called
    called_base_classes = get_explicitly_called_base_classes(cls)

    # For each called base class, get the attributes it handles in its __eq__ method
    for base_cls in called_base_classes:
        base_eq_attrs = get_eq_method_attributes(base_cls)
        inherited_attributes.update(base_eq_attrs)

    return inherited_attributes


def check_eq_completeness_with_inheritance(
        cls: Type[Any],
        excluded_attributes: Optional[Set[str]] = None,
        ignore_private: bool = True) -> tuple[bool, Set[str], Set[str], Set[str]]:
    """
    Check if __eq__ method covers all instance attributes, automatically handling inheritance.

    This function properly handles multiple levels of inheritance by:
    1. Detecting both super().__eq__() calls and explicit BaseClass.__eq__(self, other) calls
    2. Finding all attributes handled by the actually called base classes
    3. Excluding those attributes from the completeness check

    Args:
        cls: Class to check
        excluded_attributes: Additional attributes to exclude (e.g., cache fields)
        ignore_private: Whether to ignore attributes starting with underscore

    Returns:
        Tuple of (is_complete, missing_attributes, extra_attributes, auto_excluded_attributes)
    """
    excluded_attributes = excluded_attributes or set()

    instance_attrs = get_instance_attributes(cls)
    eq_attrs = get_eq_method_attributes(cls)
    auto_excluded: Set[str] = set()

    # Get attributes handled by explicitly called base classes
    inherited_attrs = get_all_inherited_eq_attributes(cls)
    auto_excluded = inherited_attrs
    # Remove inherited attributes from the comparison
    instance_attrs -= inherited_attrs

    # Filter out excluded and private attributes
    if ignore_private:
        instance_attrs = {attr for attr in instance_attrs if not attr.startswith('_')}
        eq_attrs = {attr for attr in eq_attrs if not attr.startswith('_')}

    instance_attrs -= excluded_attributes

    missing_in_eq = instance_attrs - eq_attrs
    extra_in_eq = eq_attrs - instance_attrs

    return len(missing_in_eq) == 0, missing_in_eq, extra_in_eq, auto_excluded


def check_eq_completeness(cls: Type[Any],
                          excluded_attributes: Optional[Set[str]] = None,
                          ignore_private: bool = True) -> tuple[bool, Set[str], Set[str]]:
    """
    Check if __eq__ method covers all instance attributes (backwards compatible version).

    Args:
        cls: Class to check
        excluded_attributes: Attributes to ignore (e.g., cache fields, computed properties)
        ignore_private: Whether to ignore attributes starting with underscore

    Returns:
        Tuple of (is_complete, missing_attributes, extra_attributes)
    """
    is_complete, missing, extra, _ = check_eq_completeness_with_inheritance(
        cls, excluded_attributes, ignore_private)
    return is_complete, missing, extra


def assert_eq_completeness(cls: Type[Any],
                           excluded_attributes: Optional[Set[str]] = None,
                           ignore_private: bool = True) -> None:
    """Assert that __eq__ method covers all instance attributes."""
    is_complete, missing, extra, auto_excluded = check_eq_completeness_with_inheritance(
        cls, excluded_attributes, ignore_private)

    if not is_complete:
        message = (f"Class {cls.__name__} __eq__ method is missing attributes: {missing}. "
                   f"Extra attributes in __eq__: {extra}")
        if auto_excluded:
            message += f". Auto-excluded (handled by super()): {auto_excluded}"
        raise AssertionError(message)


def detect_dangerous_super_with_multiple_inheritance(cls: Type[Any]) -> tuple[bool, Set[Type[Any]]]:
    """
    Detect if a class uses super().__eq__() when it has multiple base classes with __eq__ methods.
    This is dangerous because super() only calls the next class in MRO, not all base classes.

    Returns:
        Tuple of (is_dangerous, skipped_base_classes)
    """
    if not hasattr(cls, '__eq__'):
        return False, set()

    # Find base classes that have __eq__ methods
    base_classes_with_eq: List[Type[Any]] = []
    for base_cls in cls.__mro__[1:]:  # Skip self
        if (base_cls is not object and
                hasattr(base_cls, '__eq__') and
                base_cls.__eq__ is not object.__eq__ and
                '__eq__' in base_cls.__dict__):
            base_classes_with_eq.append(base_cls)

    # If there's only 0 or 1 base class with __eq__, super() is fine
    if len(base_classes_with_eq) <= 1:
        return False, set()

    # Check if this class uses super().__eq__()
    try:
        eq_source = inspect.getsource(cls.__eq__)
        eq_source = textwrap.dedent(eq_source)
        tree = ast.parse(eq_source)

        uses_super_eq = False
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and
                    isinstance(node.func, ast.Attribute) and
                    node.func.attr == '__eq__' and
                    isinstance(node.func.value, ast.Call) and
                    isinstance(node.func.value.func, ast.Name) and
                    node.func.value.func.id == 'super'):
                uses_super_eq = True
                break

        if uses_super_eq:
            # super() only calls the first base class in MRO, others are skipped
            skipped_base_classes: Set[Type[Any]] = set(base_classes_with_eq[1:])  # Rest are skipped
            return True, skipped_base_classes
        else:
            return False, set()

    except (OSError, TypeError, SyntaxError):
        return False, set()


class EqualityCompletenessTestMixin:
    """Mixin for test classes to add equality completeness checking."""

    def assert_eq_complete(self, cls: Type[Any],
                           excluded_attributes: Optional[Set[str]] = None,
                           ignore_private: bool = True) -> None:
        """Test helper to check equality completeness."""
        assert_eq_completeness(cls, excluded_attributes, ignore_private)

    def assert_eq_complete_with_details(self, cls: Type[Any],
                                        excluded_attributes: Optional[Set[str]] = None,
                                        ignore_private: bool = True) -> tuple[bool, Set[str], Set[str], Set[str]]:
        """Test helper that returns detailed information about the completeness check."""
        return check_eq_completeness_with_inheritance(cls, excluded_attributes, ignore_private)


class TestEqualityCompleteness(unittest.TestCase, EqualityCompletenessTestMixin):
    """Example test class showing how to use the equality completeness checker."""

    def test_multiple_inheritance_detection_works(self) -> None:
        """Test that multiple inheritance detection works correctly with explicit base class calls."""
        # Test Workspace which uses explicit BaseClass.__eq__ calls
        from datasurface.md.governance import Workspace

        # Check what base classes are actually called
        called_base_classes = get_explicitly_called_base_classes(Workspace)

        # Should detect both ANSI_SQL_NamedObject and Documentable
        base_class_names = {cls.__name__ for cls in called_base_classes}
        self.assertIn('ANSI_SQL_NamedObject', base_class_names, "Should detect ANSI_SQL_NamedObject.__eq__ call")
        self.assertIn('Documentable', base_class_names, "Should detect Documentable.__eq__ call")

        # Test the full completeness check
        is_complete, missing, _, auto_excluded = check_eq_completeness_with_inheritance(Workspace)

        # Should now be complete since we properly detect the base class calls
        self.assertTrue(is_complete, f"Workspace should be complete but missing: {missing}")
        self.assertGreater(len(auto_excluded), 0, "Workspace should have auto-excluded attributes from inheritance")

        # Should exclude attributes from both base classes
        self.assertIn('name', auto_excluded, "Should auto-exclude 'name' from ANSI_SQL_NamedObject")
        self.assertIn('documentation', auto_excluded, "Should auto-exclude 'documentation' from Documentable")

        print(f"Workspace called base classes: {base_class_names}")
        print(f"Workspace auto-excluded attributes: {auto_excluded}")

    def test_workspace_cache_entry_eq_completeness(self) -> None:
        """Test that WorkspaceCacheEntry __eq__ covers all attributes."""
        from datasurface.md.governance import WorkspaceCacheEntry
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(WorkspaceCacheEntry)
        self.assertTrue(is_complete)
        # Should have no auto-excluded attributes since it doesn't inherit from classes with __eq__
        self.assertEqual(len(auto_excluded), 0)

    def test_datastore_cache_entry_eq_completeness(self) -> None:
        """Test that DatastoreCacheEntry __eq__ covers all attributes."""
        from datasurface.md.governance import DatastoreCacheEntry
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(DatastoreCacheEntry)
        self.assertTrue(is_complete)
        # Should have no auto-excluded attributes since it doesn't inherit from classes with __eq__
        self.assertEqual(len(auto_excluded), 0)

    def test_governance_zone_eq_completeness(self) -> None:
        """Test that GovernanceZone __eq__ covers all attributes."""
        from datasurface.md.governance import GovernanceZone
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(GovernanceZone)
        self.assertTrue(is_complete)
        # Should have auto-excluded attributes since it inherits from GitControlledObject
        self.assertGreater(len(auto_excluded), 0)
        print(f"GovernanceZone auto-excluded attributes: {auto_excluded}")

    def test_dataset_eq_completeness(self) -> None:
        """Test that Dataset __eq__ covers all attributes with inheritance detection."""
        from datasurface.md.governance import Dataset
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(Dataset)
        self.assertTrue(is_complete)
        # Should have auto-excluded attributes since it inherits from ANSI_SQL_NamedObject and Documentable
        self.assertGreater(len(auto_excluded), 0)
        print(f"Dataset auto-excluded attributes: {auto_excluded}")

    def test_detect_dangerous_super_with_multiple_inheritance(self) -> None:
        """Test that we can detect dangerous super().__eq__() usage with multiple inheritance."""
        # This test would catch if someone incorrectly used super().__eq__() with multiple inheritance

        # Check some known classes that should use explicit base class calls
        from datasurface.md.governance import Workspace, Dataset, Datastore

        for cls in [Workspace, Dataset, Datastore]:
            is_dangerous, skipped_classes = detect_dangerous_super_with_multiple_inheritance(cls)
            if is_dangerous:
                skipped_names = {cls.__name__ for cls in skipped_classes}
                message = (f"Class {cls.__name__} uses super().__eq__() with multiple inheritance, "
                           f"which skips these base classes: {skipped_names}. "
                           f"Use explicit BaseClass.__eq__(self, other) calls instead.")
                self.fail(message)


if __name__ == '__main__':
    unittest.main()