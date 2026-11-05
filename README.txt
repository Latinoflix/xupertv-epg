XuperTV EPG - Cloudflare Pages + GitHub Actions

1. Create a NEW GitHub repository. Public or private is fine.
2. Upload all files/folders from this package to the repository root.
3. In GitHub: Settings > Actions > General > Workflow permissions:
   select "Read and write permissions", then Save.
4. Open Actions > Build and publish XuperTV EPG > Run workflow.
5. Wait until the workflow is green. It should create docs/epg.json.
6. In Cloudflare Dashboard > Workers & Pages > Create > Pages > Connect to Git.
   Select this repository.
7. Build settings:
   Framework preset: None
   Build command: leave EMPTY
   Build output directory: docs
   Deploy.
8. After deployment, test:
   https://YOUR-PROJECT.pages.dev/epg.json

IMPORTANT:
- GitHub Actions processes XMLTV; Cloudflare Pages only serves the generated JSON.
- Schedule runs every 12 hours. GitHub scheduled runs may be delayed.
- This starter uses the existing epg_server.py parser and XuperTV's expected JSON format.
- Do not upload private credentials or admin tokens.
