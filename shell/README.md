# Multi-profile shell (source)

Templates and assets for **Feature Store Mini** landing pages. **Do not edit generated folders by hand** — change `shell/` and re-run Node.

## Render

From the **repository root**:

```bash
node shell/render-shell.mjs --project shell/projects/feature-store.json --body shell/body/feature-store.html --out layout-shell --profile recruiter

node shell/render-shell.mjs --project shell/projects/feature-store.json --body shell/body/feature-store.html --out layout-shell-commercial --profile commercial
```

Outputs include `index.html`, `shell.css`, `demo-content.css`, `shell.js`, `favicon.svg`, and `profile.json`. Asset URLs use `/layout-shell/` so they match `FastAPI` static mount + `GET /` serving `layout-shell/index.html`.

Deploy **recruiter** build to **feature-store.vahdetkaratas.com** and **commercial** build to **feature-store.vahdetlabs.com** (or your chosen hostnames). Canonical live API/UI entry: **https://features.vahdetkaratas.com/**.
