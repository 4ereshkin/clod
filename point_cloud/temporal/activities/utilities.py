import json
from typing import List, Tuple


Vec3 = Tuple[float, float, float]

class SelectOptions:
    template: str
    title: str = ''
    filter_name: str = ''
    extensions_str: str = ''

    def read_options(self):
        with open(self.template, 'r', encoding='utf-8') as f:
            options = json.load(f)
        self.title = options['title']
        self.filter_name = options['file_types']['filter_name']
        self.extensions_str = ' '.join(options['file_types']['types'])

    def filetypes(self) -> List[Tuple[str, str]]:
        return [(self.filter_name, self.extensions_str)]