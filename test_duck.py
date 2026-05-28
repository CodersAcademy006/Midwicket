def has_cross_product(node):
    if isinstance(node, dict):
        if node.get("name") == "CROSS_PRODUCT":
            return True
        for v in node.values():
            if has_cross_product(v):
                return True
    elif isinstance(node, list):
        for v in node:
            if has_cross_product(v):
                return True
    return False

import json
plan = json.loads('[\n    {\n        "name": "CROSS_PRODUCT",\n        "children": []\n    }\n]')
print(has_cross_product(plan))
