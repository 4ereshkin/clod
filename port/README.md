# port package

Infrastructure-facing helpers and adapters.

## Structure

- `port.connectors` — reusable connector classes/config models (S3, env config).
- `port.tools` — local utility scripts that are not connectors.
- `port.samples` — sample input/output data for local experiments.

## Notes

`port.connectors.rbmq` is deprecated and kept as a compatibility shim. Use
`port.tools.reproject_paths` for path-file reprojection utility behavior.
