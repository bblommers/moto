def extract_duplicates(input_list: list[str]) -> list[str]:
    return [item for item in input_list if input_list.count(item) > 1]
