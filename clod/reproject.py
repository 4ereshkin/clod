import json
import os
import pdal
import yaml
from pathlib import Path
from typing import Optional

with open('point_cloud/temporal/workflows/config.yaml', 'r') as f:
    VERSION = yaml.safe_load(f)['VERSION_INFO']['WORKFLOW_VERSION']


class SRS:
    def __init__(self, cloud_path: str, in_srs: str, out_srs: str):
        self.cloud_path = cloud_path
        self.in_srs = in_srs
        self.out_srs = out_srs

    def _get_source_name(self) -> str:
        return Path(self.cloud_path).stem

    def reproject(self) -> Optional[str]:

        input_file = self.cloud_path
        output_file = f"result_cloud/reprojected_clouds/{self._get_source_name()}_{VERSION}_reproj.laz"
        os.makedirs("result_cloud/reprojected_clouds", exist_ok=True)

        pipeline_json = {
            "pipeline": [
                {
                    "type": "readers.las",
                    "filename": input_file
                },
                {
                    "type": "filters.reprojection",
                    "in_srs": str(self.in_srs),
                    "out_srs": str(self.out_srs)
                },
                {
                    "type": "writers.las",
                    "filename": output_file
                }
            ]
        }

        try:
            pipeline = pdal.Pipeline(json.dumps(pipeline_json))
            pipeline.execute()

            if os.path.exists(output_file):
                print(f"Successfully reprojected: {output_file}")
                return output_file
            else:
                print(f"Output file was not created: {output_file}")
                return None
        except Exception as e:
            print(f'PDAL reproj failed for {input_file}: {e}')
            return None

    def run(self) -> Optional[str]:
        return self.reproject()
