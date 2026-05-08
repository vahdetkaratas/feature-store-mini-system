# Multi-profile shell (source)

Templates and assets for **Feature Store Mini** landing pages. **Do not edit generated folders by hand** — change `shell/` and re-run Node.

**Profiles:** Sidebar identity comes from **`shell/profiles/<name>.json`** (`--profile recruiter` → `recruiter.json`, etc.). The build writes that object to **`profile.json`** in the output folder for use by the static pages.

## Render

From the **repository root**, recruiter uses **`shell/body/feature-store.html`**; commercial uses **`shell/body/feature-store-commercial.html`**:

```bash
node shell/render-shell.mjs --project shell/projects/feature-store.json --body shell/body/feature-store.html --out layout-shell --profile recruiter --demo-body shell/body/interactive-demo.html

node shell/render-shell.mjs --project shell/projects/feature-store.json --body shell/body/feature-store-commercial.html --out layout-shell-commercial --profile commercial --demo-body shell/body/interactive-demo.html
```

Outputs include `index.html`, **`portfolio-demo.html`** (interactive CSV demo + **`fs-portfolio-demo.css`**), `shell.css`, `demo-content.css`, `shell.js`, `favicon.svg`, and `profile.json`. **No `<base href>`** — CSS/JS/favicon use paths **relative to the HTML file**, so the same build works for **static root deploys** (e.g. `feature-store.*`) and for **`/layout-shell/...`** on the API host.

**API (`features.*`):** with `layout-shell/` beside the app, **`GET /`** redirects to **`/layout-shell/portfolio-demo.html`** when present (otherwise **`/layout-shell/index.html`**) so relative assets resolve under **`/layout-shell/`** (see `src/api/main.py`).

**Commit policy:** `layout-shell/` (recruiter) and `layout-shell-commercial/` are tracked so clones match CI and deploys without running Node first. Re-render and commit after any change under `shell/`.

**Deploy:** static **recruiter** → **feature-store.vahdetkaratas.com**; static **commercial** → **feature-store.vahdetlabs.com**. API hosts **features.vahdetkaratas.com** / **features.vahdetlabs.com** — omit `layout-shell/` beside the process so **`GET /`** redirects to **`/docs`**; with the UI folder present, **`GET /`** prefers **`/layout-shell/portfolio-demo.html`** (else **`/layout-shell/index.html`**) (see `src/api/main.py`).

Commercial hero/review CTAs use **features.vahdetlabs.com** and **vahdetlabs.com**. Sidebar footer may still include **Portfolio** and **GitHub** (`shell/profiles/commercial.json`); rail “home” / avatar use **`portfolioUrl`** there. **`index.html`** / **`portfolio-demo.html`** are siblings in the same output folder.

**Demo “back” link:** `portfolio-demo.html` uses **← Project overview** → **`index.html`** in that same build (recruiter vs commercial), not a shared URL across domains.
