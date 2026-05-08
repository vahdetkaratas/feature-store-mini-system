# Multi-profile shell (source)

Templates and assets for **Feature Store Mini** landing pages. **Do not edit generated folders by hand** — change `shell/` and re-run Node.

## Render

From the **repository root**, recruiter uses **`shell/body/feature-store.html`**; commercial uses **`shell/body/feature-store-commercial.html`**:

```bash
node shell/render-shell.mjs --project shell/projects/feature-store.json --body shell/body/feature-store.html --out layout-shell --profile recruiter

node shell/render-shell.mjs --project shell/projects/feature-store.json --body shell/body/feature-store-commercial.html --out layout-shell-commercial --profile commercial
```

Outputs include `index.html`, `shell.css`, `demo-content.css`, `shell.js`, `favicon.svg`, and `profile.json`. Recruiter build uses `/layout-shell/` as `<base href>` (matches FastAPI `StaticFiles` on **`/layout-shell`**). **Commercial** profile overrides `assetPrefix` to **`/`** so static deploys at **`feature-store.vahdetlabs.com`** root resolve `shell.css` and `favicon.svg` correctly.

**Commit policy:** `layout-shell/` (recruiter) and `layout-shell-commercial/` are tracked so clones match CI and deploys without running Node first. Re-render and commit after any change under `shell/`.

**Deploy:** static **recruiter** → **feature-store.vahdetkaratas.com**; static **commercial** → **feature-store.vahdetlabs.com**. API hosts **features.vahdetkaratas.com** / **features.vahdetlabs.com** — omit `layout-shell/` beside the process so **`GET /`** redirects to **`/docs`** (see `src/api/main.py`).

Commercial hero/review CTAs use **features.vahdetlabs.com** and **vahdetlabs.com**. The left sidebar “ML systems” block also lists **Portfolio** at **vahdetkaratas.com/portfolio** for cross-navigation.
