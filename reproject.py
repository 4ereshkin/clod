# reproject.py
from __future__ import annotations

import subprocess
from pathlib import Path

class SRS:
    def __init__(self, cloud_path: str, in_srs: str, out_srs: str):
        self.cloud_path = cloud_path
        self.in_srs = in_srs
        self.out_srs = out_srs

    def run(self) -> str:
        inp = Path(self.cloud_path)
        out = inp.with_name(inp.stem + f"__{self.out_srs.replace(':','_')}" + inp.suffix)

        # Вариант A: pdal translate + reprojection
        cmd = [
            "pdal",
            "translate",
            str(inp),
            str(out),
            "reprojection",
            f"--filters.reprojection.in_srs={self.in_srs}",
            f"--filters.reprojection.out_srs={self.out_srs}",
        ]

        try:
            p = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                "PDAL reprojection failed.\n"
                f"CMD: {' '.join(cmd)}\n"
                f"returncode: {e.returncode}\n"
                f"stdout:\n{e.stdout}\n"
                f"stderr:\n{e.stderr}\n"
            ) from e

        if not out.exists():
            raise RuntimeError(
                "PDAL finished without output file.\n"
                f"CMD: {' '.join(cmd)}\n"
                f"stdout:\n{p.stdout}\n"
                f"stderr:\n{p.stderr}\n"
            )

        return str(out)
