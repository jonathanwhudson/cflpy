import more_itertools as mit


def chunk(original_list: list, parts: int):
    return [list(c) for c in mit.divide(parts, original_list)]


def flatten(original_list: list):
    return [item for sublist in original_list for item in sublist]
