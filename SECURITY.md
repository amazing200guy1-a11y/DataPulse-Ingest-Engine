# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.x     | Yes       |

## Reporting a Vulnerability

Contact: security@mehd.ai — Subject: `[SECURITY] DataPulse-Ingest-Engine`

Do NOT open a public GitHub issue for security disclosures.

## Notes

- No credentials are stored in this repository.
- All file paths are sanitised through `pathlib.Path` before use.
- Anomaly detection threshold is hard-coded and cannot be soft-overridden at runtime.