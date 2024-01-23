from abc import ABC, abstractmethod
import re
import socket
import ipaddress
from typing import Generic, TypeVar

from datasurface.md.Exceptions import NameMustBeANSISQLIdentifierException
from datasurface.md.Lint import ValidationTree

sql_reserved_words : list[str] = [
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

sql_reserved_words_as_set : set[str] = set(sql_reserved_words)

def is_valid_sql_identifier(identifier: str) -> bool:
    """This checks if the string is a valid SQL identifier"""
    # Check for reserved words
    if(identifier.upper() in sql_reserved_words_as_set):
        return False
    # Regular expression for a valid SQL identifier
    pattern = r'^[a-zA-Z][a-zA-Z0-9_]{0,127}$'
    return bool(re.match(pattern, identifier))

def is_valid_azure_key_vault_name(name: str) -> bool:
    # Regular expression for a valid Azure Key Vault name
    pattern = r'^[a-z0-9]{3,24}$'
    return bool(re.match(pattern, name))

def is_valid_hostname_or_ip(s : str) -> bool:
    """This checks if the string is a valid hostname or IP address"""
    try:
        # Check if it's a valid IP address
        ipaddress.ip_address(s)
        return True
    except ValueError:
        pass

    try:
        # Check if it's a valid hostname
        socket.gethostbyname(s)
        return True
    except socket.gaierror:
        pass

    return False    
        
class ANSI_SQL_NamedObject:
    def __init__(self, name : str) -> None:
        self.name : str = name
        """The name of the object"""
        if not is_valid_sql_identifier(self.name):
            raise NameMustBeANSISQLIdentifierException(self.name)

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, ANSI_SQL_NamedObject) and self.name == __value.name

    def isBackwardsCompatibleWith(self, other : object, vTree : ValidationTree) -> bool:
        if(not isinstance(other, ANSI_SQL_NamedObject)):
            vTree.addProblem(f"Object {other} is not an ANSI_SQL_NamedObject")
            return False
        
        """Returns true if this column is backwards compatible with the other column"""
        # TODO Add support to changing the column data type to a compatible type
        if(self.name != other.name):
            vTree.addProblem(f"Column name changed from {self.name} to {other.name}")
        return True
    
    def nameLint(self, tree : ValidationTree) -> None:
        if not is_valid_sql_identifier(self.name):
            tree.addProblem(f"Name {self.name} is not a valid ANSI SQL identifier")

T = TypeVar('T')

class Policy(ABC, Generic[T]):
    """Base class for all policies"""
    
    @abstractmethod
    def isCompatible(self, obj : T) -> bool:
        """Check if obj meets the policy"""
        raise NotImplementedError()

