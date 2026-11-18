# GitHub setup and Fabric workspace sync

Suggested private repository name is `fabric-ingestion-accelerator`. Suggested description is “Metadata-driven Microsoft Fabric ingestion to OneLake, with incremental loads, SQL CDC, replay, and a quality-gate POC.”

## Publish the starter

From an authenticated GitHub CLI session, in the extracted repo folder:

```bash
git init -b main
git add .
git commit -m "Add Microsoft Fabric metadata-driven ingestion POC"
gh repo create fabric-ingestion-accelerator --private --description "Metadata-driven Fabric ingestion with OneLake Landing, incremental loads, CDC, and a quality-gate POC" --source=. --remote=origin --push
```

For an existing repository, use a new branch and open a pull request. Set **Template repository** in GitHub Settings if you want to create a separate repository for each client.

## Connect Fabric DEV

1. Follow [the Fabric POC walkthrough](fabric-poc.md) to import the notebooks and create the DEV Lakehouse and Variable Library.
2. Create a GitHub branch for the POC. In Fabric DEV, open **Workspace settings → Git integration**, select GitHub, authenticate, and choose the branch and a dedicated folder such as `/fabric-workspace`.
3. Commit the imported items from Fabric's Source control panel into that folder. Use Fabric's generated definitions for workspace synchronization. The source `.ipynb` files under `notebooks/` are for import. After a change to the generated source notebooks, refresh the corresponding Fabric items and commit their updated definitions through Fabric.
4. Keep raw payloads, Key Vault values, and client data out of Git. Configure workspace-specific settings and recheck bindings and permissions in each Fabric workspace.

Fabric Git integration requires appropriate capacity, tenant settings, and access to the GitHub repository and Fabric workspace. If the Git integration is unavailable, the manual notebook-import walkthrough still works.

[Microsoft Learn — Fabric Git integration setup](https://learn.microsoft.com/fabric/cicd/git-integration/git-get-started)
