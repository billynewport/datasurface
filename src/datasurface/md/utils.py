import re

from datasurface.md.Exceptions import NameMustBeANSISQLIdentifierException
from datasurface.md.Lint import ValidationTree
from typing import Callable, TypeVar, Tuple, Dict, Generic


sql_reserved_words: list[str] = [
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "INSERT", "UPDATE", "DELETE",
    "CREATE", "ALTER", "DROP", "TABLE", "DATABASE", "INDEX", "VIEW", "TRIGGER",
    "PROCEDURE", "FUNCTION", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "ON",
    "GROUP", "BY", "ORDER", "HAVING", "UNION", "EXCEPT", "INTERSECT", "CASE",
    "WHEN", "THEN", "ELSE", "END", "AS", "DISTINCT", "NULL", "IS", "BETWEEN",
    "LIKE", "IN", "EXISTS", "ALL", "ANY", "SOME", "CAST", "CONVERT", "COALESCE",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "TOP", "LIMIT", "FETCH", "OFFSET",
    "ROW", "ROWS", "ONLY", "FIRST", "NEXT", "VALUE", "VALUES", "INTO", "SET",
    "OUTPUT", "DECLARE", "CURSOR", "FOR", "WHILE", "LOOP", "REPEAT", "IF",
    "ELSEIF", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "TRANSACTION", "TRY",
    "CATCH", "THROW", "USE", "USING", "COLLATE", "PLAN", "EXECUTE", "PREPARE",
    "DEALLOCATE", "ASC", "DESC"]

sql_reserved_words_as_set: set[str] = set(sql_reserved_words)


def is_valid_sql_identifier(identifier: str) -> bool:
    """This checks if the string is a valid SQL identifier"""
    # Check for reserved words
    if (identifier.upper() in sql_reserved_words_as_set):
        return False
    # Regular expression for a valid SQL identifier
    pattern = r'^[a-zA-Z][a-zA-Z0-9_]{0,127}$'
    return (re.match(pattern, identifier)) is not None


def is_valid_azure_key_vault_name(name: str) -> bool:
    # Regular expression for a valid Azure Key Vault name
    pattern = r'^[a-z0-9]{3,24}$'
    return (re.match(pattern, name)) is not None


def is_valid_hostname_or_ip(s: str) -> bool:
    """This checks if the string is a valid hostname or IP address"""
    # Check if it's a valid IPv4 address
    pattern_ipv4 = r"^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"
    if re.fullmatch(pattern_ipv4, s) is not None:
        return True

    # Check if it's a valid IPv6 address
    pattern_ipv6 = (
        r"^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|"
        r"([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}"
        r"(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|"
        r"([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|"
        r"fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|"
        r"1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|"
        r"1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$"
    )

    if re.fullmatch(pattern_ipv6, s) is not None:
        return True

    # Check hostname
    pattern_hostname = r"^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$"
    if re.fullmatch(pattern_hostname, s) is not None and len(s) <= 253:
        return True

    return False


class ANSI_SQL_NamedObject:
    """This is the base class for objects in the model which must have an SQL identifier compatible name. These
    objects may have names which are using in creating database artifacts such as Tables, views, columns"""
    def __init__(self, name: str) -> None:
        self.name: str = name
        """The name of the object"""
        if not is_valid_sql_identifier(self.name):
            raise NameMustBeANSISQLIdentifierException(self.name)

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, ANSI_SQL_NamedObject) and self.name == __value.name

    def isBackwardsCompatibleWith(self, other: object, vTree: ValidationTree) -> bool:
        if (not isinstance(other, ANSI_SQL_NamedObject)):
            vTree.addProblem(f"Object {other} is not an ANSI_SQL_NamedObject")
            return False

        """Returns true if this column is backwards compatible with the other column"""
        # TODO Add support to changing the column data type to a compatible type
        if (self.name != other.name):
            vTree.addProblem(f"Column name changed from {self.name} to {other.name}")
        return True

    def nameLint(self, tree: ValidationTree) -> None:
        if not is_valid_sql_identifier(self.name):
            tree.addProblem(f"Name {self.name} is not a valid ANSI SQL identifier")

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"


def validate_cron_string(cron_string: str):
    # Split the cron string into fields
    fields: list[str] = cron_string.split()

    # Check that there are exactly 5 fields
    if len(fields) != 5:
        return False

    # Define the valid ranges for each field
    ranges = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]

    # Check each field
    for field, (min_value, max_value) in zip(fields, ranges):
        # If the field is a '*', it's valid
        if field == '*':
            continue

        # If the field contains a ',', it's a list of values
        if ',' in field:
            values: list[str] = field.split(',')
        else:
            values: list[str] = [field]

        # Check each value
        for value in values:
            if '/' in value:
                dashList: list[str] = value.split('/')
                if (len(dashList) != 2):
                    return False
                start: str = dashList[0]
                if (start != '*'):
                    return False
                end: str = dashList[1]
                if not end.isdigit() or not end.isdigit() or not (min_value <= int(end) <= max_value):
                    return False
            # If the value contains a '-', it's a range
            elif '-' in value:
                dashList: list[str] = value.split('-')
                if (len(dashList) != 2):
                    return False
                start: str = dashList[0]
                end: str = dashList[1]
                if not start.isdigit() or not end.isdigit() or not (min_value <= int(start) <= int(end) <= max_value):
                    return False
            else:
                # The value should be a single number
                if not value.isdigit() or not (min_value <= int(value) <= max_value):
                    return False

    # If we've made it this far, the cron string is valid
    return True


R = TypeVar('R')
A = TypeVar('A')


class Memoize(Generic[A, R]):
    """Decorator to cache previous calls to a method"""
    def __init__(self, func: Callable[[A], R]) -> None:
        self.func = func
        self.cache: Dict[Tuple[A, ...], R] = {}

    def __call__(self, *args: A) -> R:
        if args in self.cache:
            return self.cache[args]

        result = self.func(*args)
        self.cache[args] = result
        return result


def memoize(func: Callable[[A], R]) -> Memoize[A, R]:
    return Memoize(func)
