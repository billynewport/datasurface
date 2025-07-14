"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from abc import abstractmethod
from typing import Any, Optional, OrderedDict
from datasurface.md.lint import UserDSLObject, ValidationTree


class Documentation(UserDSLObject):
    """This is the base class for all documentation objects. There are subclasses for different ways to express documentation such as plain text \
    or markdown and so on."""
    def __init__(self, description: str, tags: Optional[OrderedDict[str, str]] = None) -> None:
        UserDSLObject.__init__(self)
        if not description:
            raise ValueError("Description cannot be empty")
        if tags is not None:
            for key, value in tags.items():
                if not key or not value:
                    raise ValueError("Tag keys and values cannot be empty")
        self.description: str = description
        self.tags: Optional[OrderedDict[str, str]] = tags

    def __eq__(self, other: object) -> bool:
        if other is None or not isinstance(other, Documentation):
            return False
        return (self.description == other.description and
                (self.tags == other.tags if self.tags and other.tags else self.tags is other.tags))

    def __str__(self) -> str:
        tags_str = f", tags={self.tags}" if self.tags else ""
        return f"Documentation(description='{self.description}'{tags_str})"

    @abstractmethod
    def lint(self, tree: ValidationTree):
        pass

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {"_type": self.__class__.__name__, "description": self.description}
        if (self.tags is not None):
            rc["tags"] = {key: value for key, value in self.tags.items()}
        return rc


class Documentable(UserDSLObject):
    """This is the base class for all objects which can have documentation."""
    def __init__(self, documentation: Optional[Documentation]) -> None:
        UserDSLObject.__init__(self)
        self.documentation: Optional[Documentation] = documentation

    def __eq__(self, other: object):
        if (not isinstance(other, Documentable)):
            return False
        return self.documentation == other.documentation

    def __str__(self) -> str:
        return f"Documentable({self.documentation})"

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {"_type": self.__class__.__name__}
        if self.documentation is not None:
            rc.update({"documentation": self.documentation.to_json()})
        return rc


class PlainTextDocumentation(Documentation):
    def __init__(self, description: str, tags: Optional[OrderedDict[str, str]] = None) -> None:
        super().__init__(description, tags)

    def __eq__(self, other: object):
        if (not isinstance(other, PlainTextDocumentation)):
            return False
        return super().__eq__(other)

    def lint(self, tree: ValidationTree):
        pass

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__})
        return rc

    def __hash__(self) -> int:
        return hash(self.description)


class MarkdownDocumentation(Documentation):
    def __init__(self, description: str, markdown: str, tags: Optional[OrderedDict[str, str]] = None) -> None:
        super().__init__(description, tags)
        self.markdown: str = markdown

    def __eq__(self, other: object):
        if (not isinstance(other, MarkdownDocumentation)):
            return False
        return super().__eq__(other) and self.markdown == other.markdown

    def lint(self, tree: ValidationTree):
        pass

    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = super().to_json()
        rc.update({"_type": self.__class__.__name__, "markdown": self.markdown})
        return rc

    def __hash__(self) -> int:
        return hash(self.description)
