def col_is_uuid(col_type: str | None) -> bool:
    """
    Check whether a column type definition represents a UUID field.

    Supported MYSQL UUID representations:
    - CHAR 36
    - BINARY 16

    Args:
        col_type: Column type definition as a string.

    Returns:
        True if the column type matches a supported UUID format,
        otherwise False.
    """
    
    if col_type is not None:
        base, size = col_type.lower().split()
        base, size = base.strip(), size.strip()
        return (base == "char" and size == "36") or (
        base == "binary" and size == "16"
        )

    return False
    

def is_decimal(value: str) -> bool:
    """
    Validate whether a string can be converted to a decimal number.

    Examples:
        "12.5" -> True
        "12,5" -> True
        "abc" -> False

    Args:
        value: Input string value.

    Returns:
        True if the value is numeric, otherwise False.
    """
    normalized_value = value.replace(",", ".")

    try:
        float(normalized_value)
        return True
    except ValueError:
        return False
    