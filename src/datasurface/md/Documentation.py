from collections import OrderedDict
from typing import Optional


class Documentation:
    def __init__(self, description : str, tags : Optional[OrderedDict[str, str]] = None) -> None:
        self.description : str = description
        self.tags : Optional[OrderedDict[str, str]] = tags

class MarkdownDocumentation(Documentation):
    def __init__(self, description : str, markdown : str, tags : Optional[OrderedDict[str, str]] = None) -> None:
        super().__init__(description, tags)
        self.markdown : str = markdown

