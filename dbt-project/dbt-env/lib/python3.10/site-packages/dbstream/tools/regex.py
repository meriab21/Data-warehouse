import re

regex = "(?:(FROM|JOIN|INTO|USING)\s+)(\w+)[.](\w+)"


def replace_query_details(query, schema_suffix_to_add):
    a = re.finditer(regex, query, flags=re.I)
    details = []
    template_detail = 'Found in position %s: %s.%s --> %s replaced with %s'
    for i in a:
        pos = i.span()
        group = i.groups()
        t = template_detail % (pos, group[1], group[2], group[1], group[1] + '_' + schema_suffix_to_add)
        details.append(t)
    return re.sub(regex, r'\1 \2' + '_' + schema_suffix_to_add + r'.\3', query, flags=re.I | re.VERBOSE), details
