"""
Comprehensive test suite that validates ALL __eq__ methods in the codebase.
This catches any classes where new attributes were added but __eq__ wasn't updated.
"""

import unittest
import importlib
import inspect
import pkgutil
from typing import Type, Any, Set, Dict, List
from tests.test_equality_completeness import EqualityCompletenessTestMixin, check_eq_completeness_with_inheritance


class TestAllEqualityMethods(unittest.TestCase, EqualityCompletenessTestMixin):
    """
    Test that automatically finds all classes with __eq__ methods and validates completeness.
    Uses automatic inheritance detection to handle parent class attributes.
    """

    # Classes that should be excluded from automatic checking
    EXCLUDED_CLASSES: Set[str] = {
        'ABCMeta',  # Abstract base classes
        'type',     # Built-in types
        'object',   # Base object
        # Add any specific classes that have intentionally incomplete __eq__ methods
    }

    # Manual exclusions for special cases (now rarely needed due to inheritance detection)
    # These are for attributes that can't be automatically detected (e.g., computed properties, caches)
    MANUAL_EXCLUSIONS: Dict[str, Set[str]] = {
        'Ecosystem': {'datastoreCache', 'workSpaceCache', 'teamCache', 'graph'},  # Runtime caches
        'AuthorizedObjectManager': {'factory'},  # Runtime caches
        # Add more specific exclusions only when needed for cache/computed fields
    }

    def get_all_classes_from_package(self, package_name: str) -> List[Type[Any]]:
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

    def get_classes_with_eq(self) -> List[Type[Any]]:
        """Find all classes in the datasurface package that have custom __eq__ methods."""
        all_classes = self.get_all_classes_from_package('datasurface')

        classes_with_eq: List[Type[Any]] = []

        for cls in all_classes:
            # Skip excluded classes
            if cls.__name__ in self.EXCLUDED_CLASSES:
                continue

            # Check if class has a custom __eq__ method (not inherited from object)
            if (hasattr(cls, '__eq__') and
                    cls.__eq__ is not object.__eq__ and
                    '__eq__' in cls.__dict__):
                classes_with_eq.append(cls)

        return classes_with_eq

    def test_all_eq_methods_complete_with_inheritance(self) -> None:
        """Test that all __eq__ methods in the codebase cover all instance attributes using inheritance detection."""
        classes_with_eq = self.get_classes_with_eq()

        failures: List[str] = []
        inheritance_summary: Dict[str, int] = {}

        for cls in classes_with_eq:
            try:
                # Get manual exclusions for this specific class
                manual_excluded_attrs = self.MANUAL_EXCLUSIONS.get(cls.__name__, set())

                # Check completeness with inheritance detection
                is_complete, missing, extra, auto_excluded = check_eq_completeness_with_inheritance(
                    cls, manual_excluded_attrs)

                # Track inheritance detection statistics
                inheritance_summary[cls.__name__] = len(auto_excluded)

                if not is_complete:
                    failure_info = (f"{cls.__module__}.{cls.__name__}: "
                                    f"Missing: {missing}, Extra: {extra}")
                    if auto_excluded:
                        failure_info += f", Auto-excluded (inheritance): {auto_excluded}"
                    if manual_excluded_attrs:
                        failure_info += f", Manual exclusions: {manual_excluded_attrs}"
                    failures.append(failure_info)

            except Exception as e:
                # If we can't analyze the class, note it as a failure
                failures.append(f"{cls.__module__}.{cls.__name__}: Analysis failed: {e}")

        # Print summary of inheritance detection
        total_classes = len(classes_with_eq)
        classes_with_inheritance = sum(1 for count in inheritance_summary.values() if count > 0)
        total_auto_excluded = sum(inheritance_summary.values())

        print("\n=== Inheritance Detection Summary ===")
        print(f"Total classes checked: {total_classes}")
        print(f"Classes using inheritance: {classes_with_inheritance}")
        print(f"Total attributes auto-excluded: {total_auto_excluded}")
        print("Classes with most inherited attributes:")

        # Show top 5 classes with most inherited attributes
        sorted_classes = sorted(inheritance_summary.items(), key=lambda x: x[1], reverse=True)[:5]
        for class_name, count in sorted_classes:
            if count > 0:
                print(f"  - {class_name}: {count} attributes")

        if failures:
            failure_message = "The following classes have incomplete __eq__ methods:\n"
            failure_message += "\n".join(f"  - {failure}" for failure in failures)
            failure_message += "\n\nTo fix these issues:"
            failure_message += "\n1. Add missing attributes to the __eq__ method"
            failure_message += "\n2. Or add them to MANUAL_EXCLUSIONS if they're cache/computed fields"
            failure_message += "\n3. Ensure super().__eq__() is called if using inheritance"

            self.fail(failure_message)

    def test_specific_critical_classes(self) -> None:
        """Test specific classes that are critical for the git action handler."""
        critical_classes = [
            'datasurface.md.governance.Dataset',
            'datasurface.md.governance.Datastore',
            'datasurface.md.governance.Workspace',
            'datasurface.md.governance.GovernanceZone',
            'datasurface.md.governance.Team',
            'datasurface.md.governance.Ecosystem',
        ]

        for class_path in critical_classes:
            try:
                module_path, class_name = class_path.rsplit('.', 1)
                module = importlib.import_module(module_path)
                cls = getattr(module, class_name)

                # Get manual exclusions (inheritance is handled automatically)
                manual_excluded_attrs = self.MANUAL_EXCLUSIONS.get(class_name, set())

                with self.subTest(class_name=class_name):
                    is_complete, missing, _, auto_excluded = check_eq_completeness_with_inheritance(
                        cls, manual_excluded_attrs)

                    if not is_complete:
                        failure_msg = (f"Class {class_name} __eq__ method is missing attributes: {missing}.")
                        if auto_excluded:
                            failure_msg += f" Auto-excluded (inheritance): {auto_excluded}"
                        self.fail(failure_msg)

                    # Print inheritance info for critical classes
                    if auto_excluded:
                        print(f"{class_name} auto-excluded attributes: {auto_excluded}")

            except (ImportError, AttributeError) as e:
                self.fail(f"Could not import critical class {class_path}: {e}")

    def test_inheritance_detection_works(self) -> None:
        """Test that inheritance detection is working correctly for known inheritance patterns."""
        # Test Dataset which inherits from ANSI_SQL_NamedObject and Documentable
        from datasurface.md.governance import Dataset
        is_complete, missing, _, auto_excluded = check_eq_completeness_with_inheritance(Dataset)

        self.assertTrue(is_complete, f"Dataset should be complete but missing: {missing}")
        self.assertGreater(len(auto_excluded), 0, "Dataset should have auto-excluded attributes from inheritance")
        self.assertIn('name', auto_excluded, "Dataset should auto-exclude 'name' from ANSI_SQL_NamedObject")
        self.assertIn('documentation', auto_excluded, "Dataset should auto-exclude 'documentation' from Documentable")

        # Test simple cache classes that don't use inheritance
        from datasurface.md.governance import WorkspaceCacheEntry
        is_complete, missing, _, auto_excluded = check_eq_completeness_with_inheritance(WorkspaceCacheEntry)

        self.assertTrue(is_complete, f"WorkspaceCacheEntry should be complete but missing: {missing}")
        self.assertEqual(len(auto_excluded), 0, "WorkspaceCacheEntry should have no auto-excluded attributes")

    def test_no_missing_critical_attributes(self) -> None:
        """Test that commonly forgotten attributes are not missing from __eq__ methods."""
        # These are attributes that are commonly forgotten in __eq__ methods
        commonly_forgotten = {
            'key', 'name', 'documentation', 'deprecationStatus',
            'productionStatus', 'policies', 'classifications'
        }

        classes_with_eq = self.get_classes_with_eq()

        for cls in classes_with_eq:
            try:
                from tests.test_equality_completeness import get_instance_attributes, get_eq_method_attributes

                instance_attrs = get_instance_attributes(cls)
                eq_attrs = get_eq_method_attributes(cls)

                # Check if any commonly forgotten attributes are missing
                forgotten_attrs = (instance_attrs & commonly_forgotten) - eq_attrs

                if forgotten_attrs:
                    # Get both manual and auto exclusions
                    manual_excluded_attrs = self.MANUAL_EXCLUSIONS.get(cls.__name__, set())
                    _, _, _, auto_excluded = check_eq_completeness_with_inheritance(cls, manual_excluded_attrs)

                    # Only report if not in any exclusions
                    forgotten_attrs -= manual_excluded_attrs
                    forgotten_attrs -= auto_excluded

                    if forgotten_attrs:
                        with self.subTest(class_name=cls.__name__):
                            self.fail(
                                f"Class {cls.__name__} is missing commonly forgotten "
                                f"attributes in __eq__: {forgotten_attrs}"
                            )

            except Exception:
                # Skip classes we can't analyze
                continue


if __name__ == '__main__':
    unittest.main()
