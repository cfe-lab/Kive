"""
portal.models

Kive data models relating to general front-end functionality.
"""

FILE_SIZE_MULTIPLIERS = dict(k=1 << 10,
                             m=1 << 20,
                             g=1 << 30,
                             t=1 << 40)


def parse_file_size(text):
    """ Parse a file size in bytes from a string. Converts from KB, MB, etc. """
    lower_text = text.strip().lower()
    suffix = lower_text and lower_text[-1]
    if suffix == 'b':
        lower_text = lower_text[:-1]
        suffix = lower_text and lower_text[-1]
    multiplier = FILE_SIZE_MULTIPLIERS.get(suffix)
    if multiplier:
        lower_text = lower_text[:-1]
    else:
        multiplier = 1
    try:
        raw_value = float(lower_text)
    except ValueError:
        raise ValueError('Invalid file size: {!r}'.format(text))
    return int(raw_value * multiplier + 0.5)
