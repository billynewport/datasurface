"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1
"""

from abc import ABC, abstractmethod
from typing import Any


class JSONable(ABC):
    """This is a base class for all objects which can be converted to a JSON object"""
    def __init__(self) -> None:
        ABC.__init__(self)

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        rc: dict[str, Any] = {"_type": self.__class__.__name__}
        return rc
