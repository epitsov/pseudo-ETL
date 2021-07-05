from .simulator import Simulation
import json

class Creator:
    """Use this function to create write data to the data file. It overwrites the data in the file, doesn't add new one"""

    def write(self, entries: int):
        array_with_json = []
        for i in range(entries):
            s = Simulation()
            array_with_json.append(s.return_to_json())
        with open('data.json', 'w') as f:
            json.dump(array_with_json, f, ensure_ascii=False, indent=4)


