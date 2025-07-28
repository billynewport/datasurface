"""
Decorators and utilities to enforce equality method completeness.
"""

from typing import Any, Type, Set, Optional, Callable, Dict, Tuple


def eq_complete(*,
                exclude: Optional[Set[str]] = None,
                ignore_private: bool = True,
                allow_extra: bool = False) -> Callable[[Type[Any]], Type[Any]]:
    """
    Decorator to enforce that __eq__ method covers all instance attributes.

    Args:
        exclude: Set of attribute names to exclude from checking
        ignore_private: Whether to ignore attributes starting with underscore
        allow_extra: Whether to allow extra attributes in __eq__ that aren't instance vars

    Usage:
        @eq_complete(exclude={'cache_field', 'computed_property'})
        class MyClass:
            def __init__(self):
                self.name = "test"
                self.value = 42
                self.cache_field = None  # excluded

            def __eq__(self, other):
                return (isinstance(other, MyClass) and
                        self.name == other.name and
                        self.value == other.value)
    """
    def decorator(cls: Type[Any]) -> Type[Any]:
        from tests.test_equality_completeness import check_eq_completeness

        # Check completeness when class is defined
        excluded_attrs = exclude or set()
        is_complete, missing, extra = check_eq_completeness(cls, excluded_attrs, ignore_private)

        if not is_complete:
            raise TypeError(
                f"Class {cls.__name__} __eq__ method is missing attributes: {missing}"
            )

        if extra and not allow_extra:
            raise TypeError(
                f"Class {cls.__name__} __eq__ method has extra attributes: {extra}. "
                f"Use allow_extra=True if this is intentional."
            )

        return cls

    return decorator


def eq_attrs(*attrs: str) -> Callable[[Type[Any]], Type[Any]]:
    """
    Decorator to explicitly specify which attributes should be in __eq__.
    This is useful when you want to be explicit about equality semantics.

    Usage:
        @eq_attrs('name', 'value')
        class MyClass:
            def __init__(self):
                self.name = "test"
                self.value = 42
                self.internal_cache = {}  # not in equality

            def __eq__(self, other):
                return (isinstance(other, MyClass) and
                        self.name == other.name and
                        self.value == other.value)
    """
    def decorator(cls: Type[Any]) -> Type[Any]:
        from tests.test_equality_completeness import get_eq_method_attributes

        expected_attrs = set(attrs)
        eq_attrs_found = get_eq_method_attributes(cls)
        # Remove class check attributes
        eq_attrs_found.discard('__class__')

        missing = expected_attrs - eq_attrs_found
        extra = eq_attrs_found - expected_attrs

        if missing:
            raise TypeError(
                f"Class {cls.__name__} __eq__ method is missing required attributes: {missing}"
            )

        if extra:
            raise TypeError(
                f"Class {cls.__name__} __eq__ method has unexpected attributes: {extra}"
            )

        # Store the expected attributes on the class for introspection
        cls._eq_attrs = expected_attrs  # type: ignore

        return cls

    return decorator


class EqualityMeta(type):
    """
    Metaclass that automatically validates __eq__ completeness.

    Usage:
        class MyClass(metaclass=EqualityMeta):
            _eq_exclude = {'cache_field'}  # Optional: attributes to exclude

            def __init__(self):
                self.name = "test"
                self.value = 42
                self.cache_field = None

            def __eq__(self, other):
                return (isinstance(other, MyClass) and
                        self.name == other.name and
                        self.value == other.value)
    """

    def __new__(mcs, name: str, bases: Tuple[Type[Any], ...], namespace: Dict[str, Any]) -> Type[Any]:
        cls = super().__new__(mcs, name, bases, namespace)

        # Skip validation for abstract base classes or if __eq__ is inherited
        if hasattr(cls, '__eq__') and '__eq__' in namespace:
            from tests.test_equality_completeness import check_eq_completeness

            exclude_set: Set[str] = set()
            if hasattr(cls, '_eq_exclude'):
                exclude_set = getattr(cls, '_eq_exclude')
            is_complete, missing, _ = check_eq_completeness(cls, exclude_set)

            if not is_complete:
                raise TypeError(
                    f"Class {name} __eq__ method is missing attributes: {missing}. "
                    f"Add them to __eq__ or to _eq_exclude class attribute."
                )

        return cls


def eq_complete_check(cls: Type[Any],
                      exclude: Optional[Set[str]] = None,
                      ignore_private: bool = True) -> None:
    """
    Function to manually check equality completeness without decoration.
    Useful for testing or validation in specific contexts.

    Args:
        cls: Class to check
        exclude: Set of attribute names to exclude from checking
        ignore_private: Whether to ignore attributes starting with underscore

    Raises:
        TypeError: If __eq__ method is incomplete

    Usage:
        # In a test or validation function
        eq_complete_check(MyClass, exclude={'cache_field'})
    """
    from tests.test_equality_completeness import check_eq_completeness

    excluded_attrs = exclude or set()
    is_complete, missing, extra = check_eq_completeness(cls, excluded_attrs, ignore_private)

    if not is_complete:
        raise TypeError(
            f"Class {cls.__name__} __eq__ method is missing attributes: {missing}. "
            f"Extra attributes in __eq__: {extra}"
        )
