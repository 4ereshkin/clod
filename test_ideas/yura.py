import json
import pdal
import pprint


pipe = [
    {
        "type": "readers.las",
        "filename": r"D:\1_prod\test_ideas\npskrytoe.las"
    },
    {
        "type": "filters.delaunay"
    },
    {
        "type": "writers.ply",
        "filename": "intermediate.ply",
        'faces': 'true'
    }
]

pipeline = pdal.Pipeline(json.dumps(pipe))
count = pipeline.execute()
pprint.pprint(pipeline.metadata)