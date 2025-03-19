from typing import List, TypeVar, Any

T = TypeVar('T')

def swap_elements(lst: List[T], index1: int, index2: int) -> List[T]:
    """
    Swap two elements in a list by their indices.
    
    Args:
        lst: The input list
        index1: Index of the first element to swap
        index2: Index of the second element to swap
        
    Returns:
        The list with swapped elements
        
    Raises:
        IndexError: If either index is out of range
    """
    if not isinstance(lst, list):
        raise TypeError("Input must be a list")
    
    if index1 < 0 or index1 >= len(lst):
        raise IndexError(f"Index {index1} is out of range for list of length {len(lst)}")
    
    if index2 < 0 or index2 >= len(lst):
        raise IndexError(f"Index {index2} is out of range for list of length {len(lst)}")
    
    # Create a copy of the list to avoid modifying the original
    result = lst.copy()
    
    # Swap the elements
    result[index1], result[index2] = result[index2], result[index1]
    
    return result


def swap_elements_inplace(lst: List[Any], index1: int, index2: int) -> None:
    """
    Swap two elements in a list by their indices, modifying the original list.
    
    Args:
        lst: The input list to be modified
        index1: Index of the first element to swap
        index2: Index of the second element to swap
        
    Raises:
        IndexError: If either index is out of range
    """
    if not isinstance(lst, list):
        raise TypeError("Input must be a list")
    
    if index1 < 0 or index1 >= len(lst):
        raise IndexError(f"Index {index1} is out of range for list of length {len(lst)}")
    
    if index2 < 0 or index2 >= len(lst):
        raise IndexError(f"Index {index2} is out of range for list of length {len(lst)}")
    
    # Swap the elements in place
    lst[index1], lst[index2] = lst[index2], lst[index1]


# Example usage
if __name__ == "__main__":
    # Example with the non-modifying version
    my_list = [1, 2, 3, 4, 5]
    new_list = swap_elements(my_list, 0, 4)
    print(f"Original list: {my_list}")
    print(f"New list with swapped elements: {new_list}")
    
    # Example with the in-place version
    test_list = [10, 20, 30, 40, 50]
    print(f"Before swap: {test_list}")
    swap_elements_inplace(test_list, 1, 3)
    print(f"After swap: {test_list}") 