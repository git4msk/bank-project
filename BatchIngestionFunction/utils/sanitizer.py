# utils/sanitizer.py
def strip(value):
    """
    Strip whitespace safely.
    Returns trimmed string or original value.
    """
    if isinstance(value, str):
        return value.strip()
    return value


def to_float(value):
    """
    Convert to float safely.
    Returns float OR None if not possible.
    """
    if value is None:
        return None

    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return None


def to_bool(value):
    """
    Convert values like 'yes', 'no', 'true', 'false', '1', '0' to bool.
    Returns None if ambiguous.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    s = str(value).strip().lower()
    if s in ["yes", "true", "1", "y"]:
        return True
    if s in ["no", "false", "0", "n"]:
        return False

    return None
