# Microsoft Fabric Ingestion Accelerator

A reusable Git template for onboarding client data sources into OneLake. One metadata-driven engine selects the connector, plans the batch, writes Landing data, and commits the checkpoint. The synthetic POC demonstrates incremental pickup, replay, and a batch-scoped quality gate.

| Start here | Purpose |
|---|---|
| [Live API walkthrough](docs/public-api-demo.md) | Run USGS and World Bank from Landing through Gold |
| [Fabric POC walkthrough](docs/fabric-poc.md) | Set up DEV and run the client demo with synthetic orders |
| [GitHub and workspace sync](docs/github.md) | Publish a private template and connect your Fabric DEV workspace |
| [Architecture infographic](docs/architecture.html) | Open locally for the interactive source and retry walkthrough |
| [Engine reference](docs/engine.md) | Metadata keys, connector modes, authentication, checkpoints, and recovery |
| [Notebooks](notebooks/) | Importable bootstrap, landing, POC setup, and POC quality notebooks |
| [Metadata](metadata/) | Control tables, example jobs, Variable Library settings, and quality rules |

The engine implements REST API and Workday RaaS full and watermark loads, SQL Server full/watermark/CDC, and ADLS full/incremental file copy. The demo quality notebook evaluates published CSV batches and records rule outcomes. The public API demo includes purpose-built Bronze, Silver, and Gold transforms. Production orchestration, generalized transforms, and client-specific authentication and networking are integration work. Run the Fabric POC in DEV before presenting results.

Edit `src/` and `metadata/`, then run `python build_notebooks.py` and `python scripts/validate.py`. The included GitHub Actions workflow checks notebook regeneration, syntax, and metadata keys.
