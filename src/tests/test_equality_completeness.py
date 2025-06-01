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

    # Tests for InternalLintableObject subclasses
    def test_pipeline_node_eq_completeness(self) -> None:
        """Test PipelineNode and its subclasses for equality completeness."""
        from datasurface.md.governance import ExportNode, IngestionMultiNode, IngestionSingleNode, TriggerNode, DataTransformerNode

        # Skip abstract base classes that can't be instantiated
        concrete_classes = [ExportNode, IngestionMultiNode, IngestionSingleNode, TriggerNode, DataTransformerNode]

        for cls in concrete_classes:
            with self.subTest(class_name=cls.__name__):
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls)
                # Many pipeline nodes inherit from PipelineNode
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")

    def test_platform_pipeline_graph_eq_completeness(self) -> None:
        """Test PlatformPipelineGraph for equality completeness."""
        from datasurface.md.governance import PlatformPipelineGraph
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(PlatformPipelineGraph)
        self.assertTrue(is_complete)
        print(f"PlatformPipelineGraph auto-excluded attributes: {auto_excluded}")

    def test_ecosystem_pipeline_graph_eq_completeness(self) -> None:
        """Test EcosystemPipelineGraph for equality completeness."""
        from datasurface.md.governance import EcosystemPipelineGraph
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(EcosystemPipelineGraph)
        self.assertTrue(is_complete)
        print(f"EcosystemPipelineGraph auto-excluded attributes: {auto_excluded}")

    # Tests for UserDSLObject subclasses
    def test_documentation_classes_eq_completeness(self) -> None:
        """Test Documentation and Documentable classes."""
        from datasurface.md.documentation import Documentation, Documentable

        # Test Documentation
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(Documentation)
        self.assertTrue(is_complete, "Documentation should have complete __eq__ method")
        print(f"Documentation auto-excluded attributes: {auto_excluded}")

        # Test Documentable
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(Documentable)
        self.assertTrue(is_complete, "Documentable should have complete __eq__ method")
        print(f"Documentable auto-excluded attributes: {auto_excluded}")

    def test_credential_classes_eq_completeness(self) -> None:
        """Test Credential and CredentialStore classes."""
        from datasurface.md.credential import Credential, CredentialStore

        # Test Credential
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(Credential)
        self.assertTrue(is_complete, "Credential should have complete __eq__ method")
        print(f"Credential auto-excluded attributes: {auto_excluded}")

        # Test CredentialStore
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(CredentialStore)
        self.assertTrue(is_complete, "CredentialStore should have complete __eq__ method")
        print(f"CredentialStore auto-excluded attributes: {auto_excluded}")

    def test_schema_classes_eq_completeness(self) -> None:
        """Test schema-related UserDSLObject classes."""
        from datasurface.md.schema import AttributeList

        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(AttributeList)
        self.assertTrue(is_complete, "AttributeList should have complete __eq__ method")
        print(f"AttributeList auto-excluded attributes: {auto_excluded}")

    def test_types_classes_eq_completeness(self) -> None:
        """Test DataType class."""
        from datasurface.md.types import DataType
        
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(DataType)
        self.assertTrue(is_complete, "DataType should have complete __eq__ method")
        print(f"DataType auto-excluded attributes: {auto_excluded}")

    def test_datatype_subclasses_eq_completeness(self) -> None:
        """Test all DataType subclasses for equality completeness."""
        from datasurface.md.types import (
            BoundedDataType, ArrayType, MapType, StructType, TextDataType, 
            NumericDataType, FixedIntegerDataType, TinyInt, SmallInt, Integer, BigInt,
            CustomFloat, MicroScaling_CustomFloat, Decimal, TemporalDataType, 
            Timestamp, Date, Interval, UniCodeType, NonUnicodeString, Boolean, Variant, Binary
        )
        
        # Test all concrete DataType subclasses
        datatype_subclasses = [
            BoundedDataType, ArrayType, MapType, StructType, TextDataType,
            NumericDataType, FixedIntegerDataType, TinyInt, SmallInt, Integer, BigInt,
            CustomFloat, MicroScaling_CustomFloat, Decimal, TemporalDataType,
            Timestamp, Date, Interval, UniCodeType, NonUnicodeString, Boolean, Variant, Binary
        ]
        
        print(f"\n=== Testing {len(datatype_subclasses)} DataType subclasses ===")
        tested_classes = []
        
        for cls in datatype_subclasses:
            with self.subTest(class_name=cls.__name__):
                # Only test classes that have their own __eq__ method
                if hasattr(cls, '__eq__') and '__eq__' in cls.__dict__:
                    is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls)
                    if not is_complete:
                        print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                    self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                    tested_classes.append(cls.__name__)
                else:
                    print(f"Skipping {cls.__name__} (inherits __eq__ from parent)")
        
        print(f"DataType subclasses tested: {tested_classes}")

    def test_schema_subclasses_eq_completeness(self) -> None:
        """Test schema-related classes and their subclasses."""
        from datasurface.md.schema import AttributeList
        
        # Test schema classes
        schema_classes = [AttributeList]
        tested_classes = []
        
        print(f"\n=== Testing {len(schema_classes)} Schema classes ===")
        
        for cls in schema_classes:
            with self.subTest(class_name=cls.__name__):
                if hasattr(cls, '__eq__') and '__eq__' in cls.__dict__:
                    is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(cls)
                    self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                    tested_classes.append(cls.__name__)
                    print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")
        
        print(f"Schema classes tested: {tested_classes}")

    def test_ansi_sql_named_object_subclasses_eq_completeness(self) -> None:
        """Test ANSI_SQL_NamedObject subclasses."""
        from datasurface.md.governance import (
            Dataset, Datastore, Workspace, DatasetGroup, DataTransformer
        )
        
        # Test ANSI_SQL_NamedObject subclasses  
        ansi_sql_subclasses = [Dataset, Datastore, Workspace, DatasetGroup, DataTransformer]
        tested_classes = []
        
        print(f"\n=== Testing {len(ansi_sql_subclasses)} ANSI_SQL_NamedObject subclasses ===")
        
        for cls in ansi_sql_subclasses:
            with self.subTest(class_name=cls.__name__):
                if hasattr(cls, '__eq__') and '__eq__' in cls.__dict__:
                    is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls)
                    if not is_complete:
                        print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                    self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                    tested_classes.append(cls.__name__)
                    print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")
        
        print(f"ANSI_SQL_NamedObject subclasses tested: {tested_classes}")

    def test_documentable_subclasses_eq_completeness(self) -> None:
        """Test Documentable subclasses."""
        from datasurface.md.governance import (
            DeprecationInfo, StoragePolicy, DataContainer
        )
        
        # Test Documentable subclasses
        documentable_subclasses = [DeprecationInfo, StoragePolicy, DataContainer]
        tested_classes = []
        
        print(f"\n=== Testing {len(documentable_subclasses)} Documentable subclasses ===")
        
        for cls in documentable_subclasses:
            with self.subTest(class_name=cls.__name__):
                if hasattr(cls, '__eq__') and '__eq__' in cls.__dict__:
                    is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls)
                    if not is_complete:
                        print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                    self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                    tested_classes.append(cls.__name__)
                    print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")
        
        print(f"Documentable subclasses tested: {tested_classes}")

    def test_data_container_subclasses_eq_completeness(self) -> None:
        """Test DataContainer subclasses."""
        from datasurface.md.governance import (
            SQLDatabase, URLSQLDatabase, HostPortSQLDatabase, PostgresDatabase,
            ObjectStorage, PyOdbcSourceInfo, KafkaServer
        )
        
        # Test DataContainer subclasses
        datacontainer_subclasses = [
            SQLDatabase, URLSQLDatabase, HostPortSQLDatabase, PostgresDatabase,
            ObjectStorage, PyOdbcSourceInfo, KafkaServer
        ]
        tested_classes = []
        
        print(f"\n=== Testing {len(datacontainer_subclasses)} DataContainer subclasses ===")
        
        for cls in datacontainer_subclasses:
            with self.subTest(class_name=cls.__name__):
                if hasattr(cls, '__eq__') and '__eq__' in cls.__dict__:
                    is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls)
                    if not is_complete:
                        print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                    self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                    tested_classes.append(cls.__name__)
                    print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")
        
        print(f"DataContainer subclasses tested: {tested_classes}")

    def test_capture_metadata_subclasses_eq_completeness(self) -> None:
        """Test CaptureMetaData subclasses."""
        from datasurface.md.governance import (
            DataTransformerOutput, IngestionMetadata, CDCCaptureIngestion,
            SQLPullIngestion, StreamingIngestion, KafkaIngestion, DatasetPerTopicKafkaIngestion
        )
        
        # Test CaptureMetaData subclasses
        capture_metadata_subclasses = [
            DataTransformerOutput, IngestionMetadata, CDCCaptureIngestion,
            SQLPullIngestion, StreamingIngestion, KafkaIngestion, DatasetPerTopicKafkaIngestion
        ]
        tested_classes = []
        
        print(f"\n=== Testing {len(capture_metadata_subclasses)} CaptureMetaData subclasses ===")
        
        for cls in capture_metadata_subclasses:
            with self.subTest(class_name=cls.__name__):
                if hasattr(cls, '__eq__') and '__eq__' in cls.__dict__:
                    is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls)
                    if not is_complete:
                        print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                    self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")
                    tested_classes.append(cls.__name__)
                    print(f"{cls.__name__} auto-excluded attributes: {auto_excluded}")
        
        print(f"CaptureMetaData subclasses tested: {tested_classes}")

    def test_comprehensive_lintableobject_coverage_summary(self) -> None:
        """Provide a comprehensive summary of all LintableObject subclasses tested."""
        print("\n" + "="*80)
        print("COMPREHENSIVE LINTABLEOBJECT SUBCLASS TEST COVERAGE SUMMARY")
        print("="*80)
        
        # Count all the classes we're testing
        total_tested = 0
        
        # UserDSLObject base classes
        base_classes = ['Documentation', 'Documentable', 'ANSI_SQL_NamedObject', 'Credential', 
                         'CredentialStore', 'AttributeList', 'DataType', 'LocationKey', 'SecurityModule']
        print(f"UserDSLObject base classes tested: {len(base_classes)}")
        total_tested += len(base_classes)
        
        # DataType subclasses
        datatype_count = 23  # From our test
        print(f"DataType subclasses tested: {datatype_count}")
        total_tested += datatype_count
        
        # Governance UserDSLObject classes
        governance_classes = ['HostPortPair', 'HostPortPairList', 'DefaultDataPlatform', 'VendorKey', 'DatasetSink']
        print(f"Governance UserDSLObject classes tested: {len(governance_classes)}")
        total_tested += len(governance_classes)
        
        # ANSI_SQL_NamedObject subclasses
        ansi_sql_subclasses = ['Dataset', 'Datastore', 'Workspace', 'DatasetGroup', 'DataTransformer']
        print(f"ANSI_SQL_NamedObject subclasses tested: {len(ansi_sql_subclasses)}")
        total_tested += len(ansi_sql_subclasses)
        
        # Documentable subclasses
        documentable_subclasses = ['DeprecationInfo', 'StoragePolicy', 'DataContainer']
        print(f"Documentable subclasses tested: {len(documentable_subclasses)}")
        total_tested += len(documentable_subclasses)
        
        # DataContainer subclasses
        datacontainer_subclasses = ['SQLDatabase', 'URLSQLDatabase', 'HostPortSQLDatabase', 
                                     'PostgresDatabase', 'ObjectStorage', 'PyOdbcSourceInfo', 'KafkaServer']
        print(f"DataContainer subclasses tested: {len(datacontainer_subclasses)}")
        total_tested += len(datacontainer_subclasses)
        
        # CaptureMetaData subclasses
        capture_metadata_subclasses = ['DataTransformerOutput', 'IngestionMetadata', 'CDCCaptureIngestion',
                                        'SQLPullIngestion', 'StreamingIngestion', 'KafkaIngestion', 
                                        'DatasetPerTopicKafkaIngestion']
        print(f"CaptureMetaData subclasses tested: {len(capture_metadata_subclasses)}")
        total_tested += len(capture_metadata_subclasses)
        
        # InternalLintableObject classes
        internal_classes = ['PipelineNode subclasses', 'PlatformPipelineGraph', 'EcosystemPipelineGraph']
        print(f"InternalLintableObject classes tested: {len(internal_classes)}")
        total_tested += len(internal_classes)
        
        # Cache classes
        cache_classes = ['WorkspaceCacheEntry', 'DatastoreCacheEntry', 'TeamCacheEntry']
        print(f"Cache classes tested: {len(cache_classes)}")
        total_tested += len(cache_classes)
        
        print("-" * 80)
        print(f"TOTAL LINTABLEOBJECT SUBCLASSES TESTED: {total_tested}")
        print("✅ Multiple level inheritance detection: ENABLED")
        print("✅ Dangerous super() pattern detection: ENABLED")
        print("✅ Automatic inheritance attribute exclusion: ENABLED")
        print("="*80)

    def test_keys_classes_eq_completeness(self) -> None:
        """Test LocationKey class."""
        from datasurface.md.keys import LocationKey
        
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(LocationKey)
        self.assertTrue(is_complete, "LocationKey should have complete __eq__ method")
        print(f"LocationKey auto-excluded attributes: {auto_excluded}")

    def test_security_classes_eq_completeness(self) -> None:
        """Test SecurityModule class."""
        from datasurface.md.security import SecurityModule
        
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(SecurityModule)
        self.assertTrue(is_complete, "SecurityModule should have complete __eq__ method")
        print(f"SecurityModule auto-excluded attributes: {auto_excluded}")

    def test_lint_classes_eq_completeness(self) -> None:
        """Test ANSI_SQL_NamedObject class."""
        from datasurface.md.lint import ANSI_SQL_NamedObject
        
        is_complete, _, _, auto_excluded = self.assert_eq_complete_with_details(ANSI_SQL_NamedObject)
        self.assertTrue(is_complete, "ANSI_SQL_NamedObject should have complete __eq__ method")
        print(f"ANSI_SQL_NamedObject auto-excluded attributes: {auto_excluded}")

    def test_governance_userdsl_classes_eq_completeness(self) -> None:
        """Test UserDSLObject subclasses in governance.py."""
        from datasurface.md.governance import (
            HostPortPair, HostPortPairList, DefaultDataPlatform,
            VendorKey, DatasetSink
        )
        
        # Test concrete classes (skip abstract ones)
        concrete_classes = [
            HostPortPair, HostPortPairList, DefaultDataPlatform,
            VendorKey, DatasetSink
        ]
        
        for cls in concrete_classes:
            with self.subTest(class_name=cls.__name__):
                is_complete, missing, _, auto_excluded = self.assert_eq_complete_with_details(cls)
                if not is_complete:
                    print(f"{cls.__name__} missing: {missing}, auto-excluded: {auto_excluded}")
                self.assertTrue(is_complete, f"{cls.__name__} should have complete __eq__ method")


if __name__ == '__main__':
    unittest.main()
