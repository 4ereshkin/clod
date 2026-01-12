import json
import pdal
import pprint

js = [
    {"type": "readers.las",
     "filename": r"D:\1_prod\data\user_data\НПС Крутое\1\t100pro_2025-04-28-08-36-08_filter_map.laz"},
    {"type": "filters.info"},
    {"type": "filters.stats"},
    {"type": "filters.hexbin",
     "density": r"D:\1_prod\test_ideas\hex.geojson"}
]


pipe = pdal.Pipeline(json.dumps(js))
pipe.execute()
pprint.pprint(pipe.metadata)
