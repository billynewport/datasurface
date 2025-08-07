"""
Test utilities to ensure __eq__ methods cover all instance attributes.
This helps prevent bugs where new attributes are added but forgotten in equality checks.
"""

import inspect
import unittest
import textwrap
import importlib
import pkgutil
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
                elif isinstance(node, ast.AnnAssign):
                    # Handle annotated assignments: self.attr: type = value
                    if (isinstance(node.target, ast.Attribute) and
                            isinstance(node.target.value, ast.Name) and
                            node.target.value.id == 'self'):
                        attributes.add(node.target.attr)
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
    Now handles multiple levels of inheritance recursively.
    """
    inherited_attributes: Set[str] = set()
    visited_classes: Set[Type[Any]] = set()  # Prevent infinite recursion

    def collect_inherited_attrs_recursive(current_cls: Type[Any]) -> None:
        """Recursively collect attributes from all called base classes."""
        if current_cls in visited_classes:
            return
        visited_classes.add(current_cls)

        # Get the base classes whose __eq__ methods are explicitly called by current_cls
        called_base_classes = get_explicitly_called_base_classes(current_cls)

        # For each called base class, get the attributes it handles and recurse
        for base_cls in called_base_classes:
            base_eq_attrs = get_eq_method_attributes(base_cls)
            inherited_attributes.update(base_eq_attrs)

            # Recursively check if this base class itself calls other base classes
            collect_inherited_attrs_recursive(base_cls)

    collect_inherited_attrs_recursive(cls)
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


# Manual exclusions for special cases (attributes to ignore in equality completeness checks)
MANUAL_EXCLUSIONS = {
    'Ecosystem': {'datastoreCache', 'workSpaceCache', 'teamCache', 'graph'},
    'AuthorizedObjectManager': {'factory'},
}


class TestEqualityCompleteness(unittest.TestCase, EqualityCompletenessTestMixin):
    """Example test class showing how to use the equality completeness checker."""

    def test_all_lintable_object_subclasses_dynamically(self) -> None:
        """
        Dynamically discover and test ALL LintableObject subclasses for equality completeness.
        This ensures no subclass is missed when new classes are added.
        """
        print("\n" + "="*80)
        print("DYNAMIC LINTABLEOBJECT SUBCLASS DISCOVERY AND TESTING")
        print("="*80)

        # Dynamically discover all LintableObject subclasses
        all_lintable_classes = get_all_lintable_object_subclasses()

        print(f"Total LintableObject subclasses discovered: {len(all_lintable_classes)}")

        # Categorize classes for reporting
        categories = categorize_lintable_classes(all_lintable_classes)

        # Print discovery summary
        for category, classes in categories.items():
            if classes:
                class_names = [cls.__name__ for cls in classes]
                print(f"{category.replace('_', ' ')}: {len(classes)} classes")
                print(f"  -> {class_names}")

        print("-" * 80)

        # Test all discovered classes
        failures: List[str] = []
        total_tested = 0
        total_auto_excluded = 0

        for cls in all_lintable_classes:
            with self.subTest(class_name=cls.__name__):
                try:
                    manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                    is_complete, missing, extra, auto_excluded = check_eq_completeness_with_inheritance(cls, manual_excluded_attrs)

                    total_tested += 1
                    total_auto_excluded += len(auto_excluded)

                    if not is_complete:
                        failure_info = (f"{cls.__module__}.{cls.__name__}: "
                                        f"Missing: {missing}")
                        if extra:
                            failure_info += f", Extra: {extra}"
                        if auto_excluded:
                            failure_info += f", Auto-excluded: {auto_excluded}"
                        failures.append(failure_info)

                        # Print detailed failure info
                        print(f"❌ FAILED: {cls.__name__}")
                        print(f"   Missing attributes: {missing}")
                        if extra:
                            print(f"   Extra attributes: {extra}")
                        if auto_excluded:
                            print(f"   Auto-excluded: {auto_excluded}")
                    else:
                        # Success - optionally print inheritance info for complex classes
                        if auto_excluded:
                            print(f"✅ {cls.__name__} (auto-excluded: {auto_excluded})")

                except Exception as e:
                    failures.append(f"{cls.__module__}.{cls.__name__}: Analysis failed: {e}")
                    print(f"❌ ERROR analyzing {cls.__name__}: {e}")

        # Print final summary
        print("-" * 80)
        print(f"RESULTS: {total_tested} classes tested")
        print(f"Inheritance attributes auto-excluded: {total_auto_excluded}")
        print(f"Classes with inheritance detection: {sum(1 for cls in all_lintable_classes if len(get_all_inherited_eq_attributes(cls)) > 0)}")

        if failures:
            print(f"\n❌ FAILURES: {len(failures)} classes failed")
            for failure in failures:
                print(f"  - {failure}")
            print("\nTo fix failures:")
            print("1. Add missing attributes to __eq__ methods")
            print("2. Ensure super().__eq__() or BaseClass.__eq__() calls are present for inheritance")
            print("3. Add manual exclusions for cache/computed fields if needed")

            self.fail(f"{len(failures)} LintableObject subclasses have incomplete __eq__ methods")
        else:
            print("✅ SUCCESS: All LintableObject subclasses have complete __eq__ methods!")

        print("="*80)

    def test_dynamic_discovery_finds_known_classes(self) -> None:
        """Verify that dynamic discovery finds key classes we know should exist."""
        all_classes = get_all_lintable_object_subclasses()
        class_names = {cls.__name__ for cls in all_classes}

        # Verify some key LintableObject subclasses are found
        # Note: Cache classes like WorkspaceCacheEntry are NOT LintableObject subclasses
        expected_classes = {
            'Dataset', 'Datastore', 'Workspace', 'DataType', 'Documentation',
            'ANSI_SQL_NamedObject'
        }

        missing_classes = expected_classes - class_names
        if missing_classes:
            self.fail(f"Dynamic discovery missed these known classes: {missing_classes}")

        print(f"✅ Dynamic discovery successfully found {len(expected_classes)} key LintableObject subclasses")
        print(f"Total LintableObject subclasses discovered: {len(class_names)}")

    def test_cache_classes_are_not_lintable_objects(self) -> None:
        """Verify that cache classes are correctly NOT included as LintableObject subclasses."""
        from datasurface.md.governance import WorkspaceCacheEntry, DatastoreCacheEntry, TeamCacheEntry
        from datasurface.md.lint import UserDSLObject, InternalLintableObject

        cache_classes = [WorkspaceCacheEntry, DatastoreCacheEntry, TeamCacheEntry]

        for cls in cache_classes:
            # These should not inherit from LintableObject base classes
            self.assertFalse(issubclass(cls, UserDSLObject),
                             f"{cls.__name__} should not inherit from UserDSLObject")
            self.assertFalse(issubclass(cls, InternalLintableObject),
                             f"{cls.__name__} should not inherit from InternalLintableObject")

            # But they should have their own __eq__ methods
            self.assertTrue(hasattr(cls, '__eq__'), f"{cls.__name__} should have __eq__ method")
            self.assertTrue('__eq__' in cls.__dict__, f"{cls.__name__} should define its own __eq__ method")

        # Verify they're not included in dynamic discovery
        all_lintable_classes = get_all_lintable_object_subclasses()
        lintable_names = {cls.__name__ for cls in all_lintable_classes}

        for cls in cache_classes:
            self.assertNotIn(cls.__name__, lintable_names,
                             f"{cls.__name__} should not be included in LintableObject discovery")

        print("✅ Cache classes are correctly excluded from LintableObject testing")

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

    def test_schema_subclasses_eq_completeness(self) -> None:
        """Test schema-related classes and their subclasses using dynamic discovery."""
        # Schema classes are typically in the 'Other_UserDSLObject' category
        all_classes = get_all_lintable_object_subclasses()
        schema_classes = [cls for cls in all_classes if 'schema' in cls.__module__.lower() or
                          cls.__name__ in ['AttributeList', 'Schema']]

        print(f"\n=== Testing {len(schema_classes)} Schema-related classes (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in schema_classes:
            with self.subTest(class_name=cls.__name__):
                manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(cls, manual_excluded_attrs)
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)
                print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")

        print(f"Schema classes tested: {tested_classes}")

    def test_datatype_subclasses_eq_completeness(self) -> None:
        """Test all DataType subclasses for equality completeness using dynamic discovery."""
        all_classes = get_all_lintable_object_subclasses()
        categories = categorize_lintable_classes(all_classes)
        datatype_subclasses = categories['DataType_subclasses']

        print(f"\n=== Testing {len(datatype_subclasses)} DataType subclasses (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in datatype_subclasses:
            with self.subTest(class_name=cls.__name__):
                manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls, manual_excluded_attrs)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)

        print(f"DataType subclasses tested: {tested_classes}")

    def test_ansi_sql_named_object_subclasses_eq_completeness(self) -> None:
        """Test ANSI_SQL_NamedObject subclasses using dynamic discovery."""
        all_classes = get_all_lintable_object_subclasses()
        categories = categorize_lintable_classes(all_classes)
        ansi_sql_subclasses = categories['ANSI_SQL_NamedObject_subclasses']

        print(f"\n=== Testing {len(ansi_sql_subclasses)} ANSI_SQL_NamedObject subclasses (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in ansi_sql_subclasses:
            with self.subTest(class_name=cls.__name__):
                manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls, manual_excluded_attrs)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)
                print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")

        print(f"ANSI_SQL_NamedObject subclasses tested: {tested_classes}")

    def test_documentable_subclasses_eq_completeness(self) -> None:
        """Test Documentable subclasses using dynamic discovery."""
        all_classes = get_all_lintable_object_subclasses()
        categories = categorize_lintable_classes(all_classes)
        documentable_subclasses = categories['Documentable_subclasses']

        print(f"\n=== Testing {len(documentable_subclasses)} Documentable subclasses (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in documentable_subclasses:
            with self.subTest(class_name=cls.__name__):
                manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls, manual_excluded_attrs)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)
                print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")

        print(f"Documentable subclasses tested: {tested_classes}")

    def test_data_container_subclasses_eq_completeness(self) -> None:
        """Test DataContainer subclasses using dynamic discovery."""
        all_classes = get_all_lintable_object_subclasses()
        categories = categorize_lintable_classes(all_classes)
        datacontainer_subclasses = categories['DataContainer_subclasses']

        print(f"\n=== Testing {len(datacontainer_subclasses)} DataContainer subclasses (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in datacontainer_subclasses:
            with self.subTest(class_name=cls.__name__):
                manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls, manual_excluded_attrs)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)
                print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")

        print(f"DataContainer subclasses tested: {tested_classes}")

    def test_capture_metadata_subclasses_eq_completeness(self) -> None:
        """Test CaptureMetaData subclasses using dynamic discovery."""
        all_classes = get_all_lintable_object_subclasses()
        categories = categorize_lintable_classes(all_classes)
        capture_metadata_subclasses = categories['CaptureMetaData_subclasses']

        print(f"\n=== Testing {len(capture_metadata_subclasses)} CaptureMetaData subclasses (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in capture_metadata_subclasses:
            with self.subTest(class_name=cls.__name__):
                manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls, manual_excluded_attrs)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)
                print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")

        print(f"CaptureMetaData subclasses tested: {tested_classes}")

    def test_internal_lintable_object_subclasses_eq_completeness(self) -> None:
        """Test InternalLintableObject subclasses using dynamic discovery."""
        all_classes = get_all_lintable_object_subclasses()
        categories = categorize_lintable_classes(all_classes)
        internal_subclasses = categories['InternalLintableObject']

        print(f"\n=== Testing {len(internal_subclasses)} InternalLintableObject subclasses (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in internal_subclasses:
            with self.subTest(class_name=cls.__name__):
                manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls, manual_excluded_attrs)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)
                print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")

        print(f"InternalLintableObject subclasses tested: {tested_classes}")

    def test_other_userdsl_object_subclasses_eq_completeness(self) -> None:
        """Test other UserDSLObject subclasses using dynamic discovery."""
        all_classes = get_all_lintable_object_subclasses()
        categories = categorize_lintable_classes(all_classes)
        other_subclasses = categories['Other_UserDSLObject']

        print(f"\n=== Testing {len(other_subclasses)} Other UserDSLObject subclasses (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in other_subclasses:
            with self.subTest(class_name=cls.__name__):
                manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls, manual_excluded_attrs)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)
                print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")

        print(f"Other UserDSLObject subclasses tested: {tested_classes}")

    def test_all_policy_subclasses_dynamically(self) -> None:
        """
        Dynamically discover and test ALL Policy subclasses for equality completeness.
        This ensures no Policy subclass is missed when new classes are added.
        """
        print("\n" + "="*80)
        print("DYNAMIC POLICY SUBCLASS DISCOVERY AND TESTING")
        print("="*80)

        # Dynamically discover all Policy subclasses
        all_policy_classes = get_all_policy_subclasses()

        print(f"Total Policy subclasses discovered: {len(all_policy_classes)}")

        # Categorize classes for reporting
        categories = categorize_policy_classes(all_policy_classes)

        # Print discovery summary
        for category, classes in categories.items():
            if classes:
                class_names = [cls.__name__ for cls in classes]
                print(f"{category.replace('_', ' ')}: {len(classes)} classes")
                print(f"  -> {class_names}")

        print("-" * 80)

        # Test all discovered classes
        failures: List[str] = []
        total_tested = 0
        total_auto_excluded = 0

        for cls in all_policy_classes:
            with self.subTest(class_name=cls.__name__):
                try:
                    is_complete, missing, extra, auto_excluded = check_eq_completeness_with_inheritance(cls)

                    total_tested += 1
                    total_auto_excluded += len(auto_excluded)

                    if not is_complete:
                        failure_info = (f"{cls.__module__}.{cls.__name__}: "
                                        f"Missing: {missing}")
                        if extra:
                            failure_info += f", Extra: {extra}"
                        if auto_excluded:
                            failure_info += f", Auto-excluded: {auto_excluded}"
                        failures.append(failure_info)

                        # Print detailed failure info
                        print(f"❌ FAILED: {cls.__name__}")
                        print(f"   Missing attributes: {missing}")
                        if extra:
                            print(f"   Extra attributes: {extra}")
                        if auto_excluded:
                            print(f"   Auto-excluded: {auto_excluded}")
                    else:
                        # Success - optionally print inheritance info for complex classes
                        if auto_excluded:
                            print(f"✅ {cls.__name__} (auto-excluded: {auto_excluded})")

                except Exception as e:
                    failures.append(f"{cls.__module__}.{cls.__name__}: Analysis failed: {e}")
                    print(f"❌ ERROR analyzing {cls.__name__}: {e}")

        # Print final summary
        print("-" * 80)
        print(f"RESULTS: {total_tested} Policy classes tested")
        print(f"Inheritance attributes auto-excluded: {total_auto_excluded}")
        print(f"Classes with inheritance detection: {sum(1 for cls in all_policy_classes if len(get_all_inherited_eq_attributes(cls)) > 0)}")

        if failures:
            print(f"\n❌ FAILURES: {len(failures)} Policy classes failed")
            for failure in failures:
                print(f"  - {failure}")
            print("\nTo fix failures:")
            print("1. Add missing attributes to __eq__ methods")
            print("2. Ensure super().__eq__() or BaseClass.__eq__() calls are present for inheritance")
            print("3. Add manual exclusions for cache/computed fields if needed")

            self.fail(f"{len(failures)} Policy subclasses have incomplete __eq__ methods")
        else:
            print("✅ SUCCESS: All Policy subclasses have complete __eq__ methods!")

        print("="*80)

    def test_policy_subclasses_eq_completeness(self) -> None:
        """Test Policy subclasses using dynamic discovery."""
        all_classes = get_all_policy_subclasses()
        categories = categorize_policy_classes(all_classes)
        policy_subclasses = categories['Policy_subclasses']

        print(f"\n=== Testing {len(policy_subclasses)} Policy subclasses (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in policy_subclasses:
            with self.subTest(class_name=cls.__name__):
                manual_excluded_attrs = MANUAL_EXCLUSIONS.get(cls.__name__, set())
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls, manual_excluded_attrs)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)
                print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")

        print(f"Policy subclasses tested: {tested_classes}")

    def test_allow_disallow_policy_subclasses_eq_completeness(self) -> None:
        """Test AllowDisallowPolicy subclasses using dynamic discovery."""
        all_classes = get_all_policy_subclasses()
        categories = categorize_policy_classes(all_classes)
        allow_disallow_subclasses = categories['AllowDisallowPolicy_subclasses']

        print(f"\n=== Testing {len(allow_disallow_subclasses)} AllowDisallowPolicy subclasses (dynamically discovered) ===")
        tested_classes: List[str] = []

        for cls in allow_disallow_subclasses:
            with self.subTest(class_name=cls.__name__):
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                tested_classes.append(cls.__name__)
                print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")

        print(f"AllowDisallowPolicy subclasses tested: {tested_classes}")

    def test_policy_discovery_finds_known_classes(self) -> None:
        """Verify that dynamic discovery finds key Policy classes we know should exist."""
        all_classes = get_all_policy_subclasses()
        class_names = {cls.__name__ for cls in all_classes}

        # Verify some key Policy subclasses are found
        expected_classes = {
            'StoragePolicy', 'DataClassificationPolicy', 'DataPlatformPolicy',
            'InfraStructureLocationPolicy', 'InfraStructureVendorPolicy'
        }

        missing_classes = expected_classes - class_names
        if missing_classes:
            self.fail(f"Dynamic discovery missed these known Policy classes: {missing_classes}")

        print(f"✅ Dynamic discovery successfully found {len(expected_classes)} key Policy subclasses")
        print(f"Total Policy subclasses discovered: {len(class_names)}")
        print(f"All discovered Policy classes: {sorted(class_names)}")


def get_all_lintable_object_subclasses() -> List[Type[Any]]:
    """Dynamically discover all LintableObject subclasses in the codebase."""
    def get_all_classes_from_package(package_name: str) -> List[Type[Any]]:
        """Find all classes in a package and its subpackages."""
        classes: List[Type[Any]] = []

        try:
            package = importlib.import_module(package_name)

            # Get classes from the main package
            for _, obj in inspect.getmembers(package, inspect.isclass):
                if obj.__module__.startswith(package_name):
                    classes.append(obj)

            # Recursively get classes from subpackages
            if hasattr(package, '__path__'):
                for _, modname, _ in pkgutil.iter_modules(package.__path__, package_name + "."):
                    try:
                        submodule = importlib.import_module(modname)
                        for _, obj in inspect.getmembers(submodule, inspect.isclass):
                            if obj.__module__.startswith(package_name):
                                classes.append(obj)
                    except (ImportError, AttributeError):
                        # Skip modules that can't be imported
                        continue

        except ImportError:
            # Package doesn't exist or can't be imported
            pass

        return classes

    # Get all classes from datasurface package
    all_classes = get_all_classes_from_package('datasurface')

    # Import the LintableObject base classes
    from datasurface.md.lint import UserDSLObject, InternalLintableObject

    # Find all classes that inherit from LintableObject (UserDSLObject or InternalLintableObject)
    lintable_subclasses: List[Type[Any]] = []

    for cls in all_classes:
        try:
            # Check if class inherits from UserDSLObject or InternalLintableObject
            if (issubclass(cls, UserDSLObject) or issubclass(cls, InternalLintableObject)) and \
               cls not in (UserDSLObject, InternalLintableObject):
                # Only include classes that have their own __eq__ method
                cls_typed: Type[Any] = cls  # type: ignore
                if hasattr(cls_typed, '__eq__') and '__eq__' in cls_typed.__dict__:  # type: ignore
                    lintable_subclasses.append(cls_typed)  # type: ignore
        except TypeError:
            # Some objects might not be classes or might cause TypeError in issubclass
            continue

    return lintable_subclasses


def categorize_lintable_classes(classes: List[Type[Any]]) -> dict[str, List[Type[Any]]]:
    """Categorize LintableObject subclasses by their inheritance hierarchy for reporting."""
    from datasurface.md.lint import UserDSLObject, InternalLintableObject
    from datasurface.md.types import DataType
    from datasurface.md.lint import ANSI_SQL_NamedObject
    from datasurface.md.documentation import Documentable
    from datasurface.md.governance import DataContainer, CaptureMetaData

    categories: dict[str, List[Type[Any]]] = {
        'InternalLintableObject': [],
        'DataType_subclasses': [],
        'ANSI_SQL_NamedObject_subclasses': [],
        'Documentable_subclasses': [],
        'DataContainer_subclasses': [],
        'CaptureMetaData_subclasses': [],
        'Other_UserDSLObject': []
    }

    for cls in classes:
        try:
            if issubclass(cls, InternalLintableObject):
                categories['InternalLintableObject'].append(cls)
            elif issubclass(cls, DataType) and cls != DataType:
                categories['DataType_subclasses'].append(cls)
            elif issubclass(cls, ANSI_SQL_NamedObject) and cls != ANSI_SQL_NamedObject:
                categories['ANSI_SQL_NamedObject_subclasses'].append(cls)
            elif issubclass(cls, Documentable) and cls != Documentable:
                categories['Documentable_subclasses'].append(cls)
            elif issubclass(cls, DataContainer) and cls != DataContainer:
                categories['DataContainer_subclasses'].append(cls)
            elif issubclass(cls, CaptureMetaData) and cls != CaptureMetaData:
                categories['CaptureMetaData_subclasses'].append(cls)
            elif issubclass(cls, UserDSLObject):
                categories['Other_UserDSLObject'].append(cls)
        except TypeError:
            # Fallback to Other_UserDSLObject if we can't determine the category
            categories['Other_UserDSLObject'].append(cls)

    return categories


def get_all_policy_subclasses() -> List[Type[Any]]:
    """Dynamically discover all Policy subclasses in the codebase."""
    def get_all_classes_from_package(package_name: str) -> List[Type[Any]]:
        """Find all classes in a package and its subpackages."""
        classes: List[Type[Any]] = []

        try:
            package = importlib.import_module(package_name)

            # Get classes from the main package
            for _, obj in inspect.getmembers(package, inspect.isclass):
                if obj.__module__.startswith(package_name):
                    classes.append(obj)

            # Recursively get classes from subpackages
            if hasattr(package, '__path__'):
                for _, modname, _ in pkgutil.iter_modules(package.__path__, package_name + "."):
                    try:
                        submodule = importlib.import_module(modname)
                        for _, obj in inspect.getmembers(submodule, inspect.isclass):
                            if obj.__module__.startswith(package_name):
                                classes.append(obj)
                    except (ImportError, AttributeError):
                        # Skip modules that can't be imported
                        continue

        except ImportError:
            # Package doesn't exist or can't be imported
            pass

        return classes

    # Get all classes from datasurface package
    all_classes = get_all_classes_from_package('datasurface')

    # Import the Policy base classes
    from datasurface.md.policy import Policy, AllowDisallowPolicy

    # Find all classes that inherit from Policy or AllowDisallowPolicy
    policy_subclasses: List[Type[Any]] = []

    for cls in all_classes:
        try:
            # Check if class inherits from Policy or AllowDisallowPolicy
            if (issubclass(cls, (Policy, AllowDisallowPolicy))) and \
               cls not in (Policy, AllowDisallowPolicy):
                # Only include classes that have their own __eq__ method
                cls_typed: Type[Any] = cls  # type: ignore
                if hasattr(cls_typed, '__eq__') and '__eq__' in cls_typed.__dict__:  # type: ignore
                    policy_subclasses.append(cls_typed)  # type: ignore
        except TypeError:
            # Some objects might not be classes or might cause TypeError in issubclass
            continue

    return policy_subclasses


def categorize_policy_classes(classes: List[Type[Any]]) -> dict[str, List[Type[Any]]]:
    """Categorize Policy subclasses by their inheritance hierarchy for reporting."""
    from datasurface.md.policy import Policy, AllowDisallowPolicy

    categories: dict[str, List[Type[Any]]] = {
        'Policy_subclasses': [],
        'AllowDisallowPolicy_subclasses': [],
        'Other_Policy': []
    }

    for cls in classes:
        try:
            if issubclass(cls, AllowDisallowPolicy) and cls != AllowDisallowPolicy:
                categories['AllowDisallowPolicy_subclasses'].append(cls)  # type: ignore
            elif issubclass(cls, Policy) and cls != Policy:
                categories['Policy_subclasses'].append(cls)  # type: ignore
            else:
                categories['Other_Policy'].append(cls)  # type: ignore
        except TypeError:
            # Fallback to Other_Policy if we can't determine the category
            categories['Other_Policy'].append(cls)  # type: ignore

    return categories


if __name__ == '__main__':
    unittest.main()
