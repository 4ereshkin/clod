import json
import pdal
import pprint

js = [
    {"type": "readers.las",
     "filename": r"C:\Users\ceres\Downloads\merged (5).laz"},
]


pipe = pdal.Pipeline(json.dumps(js))
pipe.execute()
pprint.pprint(pipe.metadata)
