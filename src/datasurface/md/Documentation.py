"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Optional

from datasurface.md.Lint import ValidationTree


class Documentation(ABC):
    def __init__(self, description: str, tags: Optional[OrderedDict[str, str]] = None) -> None:
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


class Documentable:
    def __init__(self, documentation: Optional[Documentation]) -> None:
        self.documentation: Optional[Documentation] = documentation

    def __eq__(self, __value: object):
        if (not isinstance(__value, Documentable)):
            return False
        return self.documentation == __value.documentation

    def __str__(self) -> str:
        return f"Documentable({self.documentation})"


class PlainTextDocumentation(Documentation):
    def __init__(self, description: str, tags: Optional[OrderedDict[str, str]] = None) -> None:
        super().__init__(description, tags)

    def __eq__(self, other: object):
        if (not isinstance(other, PlainTextDocumentation)):
            return False
        return super().__eq__(other)

    def lint(self, tree: ValidationTree):
        pass


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
