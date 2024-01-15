import re
import socket
import ipaddress

def is_valid_sql_identifier(identifier: str) -> bool:
    """This checks if the string is a valid SQL identifier"""
    # Regular expression for a valid SQL identifier
    pattern = r'^[a-zA-Z][a-zA-Z0-9_]{0,127}$'
    return bool(re.match(pattern, identifier))

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
        if s == socket.gethostbyname(s):
            return True
    except socket.gaierror:
        pass

    return False    
        
