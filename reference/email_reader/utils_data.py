import re
import unicodedata

def sanitize_filename(name: str, replacement: str = "_", max_length: int = 100, extension: str =  "xlsx") -> str:
    """
    Convert any string (e.g. email subject) into a safe filename.
    """
    # 1. Normalize unicode (e.g. "é" → "e", smart quotes → normal quotes)
    name = unicodedata.normalize("NFKC", name)

    # 2. Remove or replace invalid characters
    #    - Windows forbids: < > : " | ? * \ / and control chars 0-31
    #    - Also remove leading/trailing spaces and dots (Windows issue)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', replacement, name)

    # 3. Collapse multiple replacements or spaces into single one
    name = re.sub(f"{replacement}+", replacement, name)

    # 4. Strip leading/trailing replacement chars, spaces, and dots
    name = name.strip(f" .{replacement}")

    # 5. Enforce max length (leave room for extension)
    if len(name) > max_length:
        name = name[:max_length].rstrip(f" .{replacement}")

    # 6. Fallback if everything was stripped
    if not name:
        name = "email_content_file"

    return f"{name}.{extension}"
