import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_PROFILE = "recruiter";

const GH_ICON = `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/></svg>`;

/** Main column: marketing hero + limitations (placeholders filled with the rest of the template). */
const MAIN_TOP_FULL = `                    <section class="hero">
                      <span class="hero-badge">{{PROJECT_EYEBROW}}</span>
                      <h1>{{PROJECT_HERO_TITLE}}</h1>
                      <p class="hero-lead">{{PROJECT_SUMMARY}}</p>
                      {{HERO_NOTE_HTML}}
                      <div class="metrics" aria-label="Project facts">
                        {{METRICS_ITEMS}}
                      </div>
                    </section>

                    {{LIMITATIONS_SECTION_HTML}}
`;

const MAIN_TOP_DEMO = "";

function usage() {
  console.error(
    "Usage: node shell/render-shell.mjs --project <project.json> --body <body.html> --out <output-dir> [--profile <name>] [--demo-body <interactive.html>] [--demo-out <filename>]"
  );
  process.exit(1);
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    if (!key || !key.startsWith("--")) usage();
    const name = key.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) {
      args[name] = true;
    } else {
      args[name] = next;
      i += 1;
    }
  }
  if (!args.project || !args.body || !args.out) usage();
  return args;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function listItems(items) {
  return items.map((item) => `<li>${escapeHtml(item)}</li>`).join("\n");
}

function linkItems(items) {
  return items
    .map(
      (item) =>
        `<li><a href="${escapeHtml(item.href)}"${item.external ? ' target="_blank" rel="noopener noreferrer"' : ""}>${escapeHtml(item.label)}</a></li>`
    )
    .join("\n");
}

function socialItems(items) {
  return items
    .map(
      (item) =>
        `<a href="${escapeHtml(item.href)}" target="_blank" rel="noopener noreferrer" title="${escapeHtml(item.label)}" aria-label="${escapeHtml(item.label)}"><i class="${escapeHtml(item.iconClass)}" aria-hidden="true"></i></a>`
    )
    .join("\n");
}

function ctaButtons(items) {
  if (!items?.length) return "";
  return items
    .map((item, index) => {
      const primary = index === 0;
      const cls = primary ? "btn btn-primary" : "btn btn-ghost";
      const ext = item.external ? ' target="_blank" rel="noopener noreferrer"' : "";
      const gh =
        primary &&
        typeof item.href === "string" &&
        item.href.includes("github.com");
      const label = escapeHtml(item.label);
      const inner = gh ? `${GH_ICON}${label}` : label;
      return `<a class="${cls}" href="${escapeHtml(item.href)}"${ext}>${inner}</a>`;
    })
    .join("\n");
}

function metricsFromStack(stack) {
  if (!stack?.length) return "";
  return stack
    .slice(0, 4)
    .map((s) => `<div><span>${escapeHtml(s)}</span></div>`)
    .join("\n");
}

function limitationsSection(items) {
  if (!items?.length) return "";
  const lis = items.map((t) => `<li>${escapeHtml(t)}</li>`).join("\n");
  return `<section id="scope" aria-labelledby="h-scope"><h2 id="h-scope">Limitations &amp; scope</h2><div class="pitch"><ul>${lis}</ul></div></section>`;
}

function heroNoteHtml(note) {
  const n = note && String(note).trim();
  if (!n) return "";
  return `<p class="hero-note">${escapeHtml(n)}</p>`;
}

function identitySubHtml(profile) {
  const sub = profile.identitySub && String(profile.identitySub).trim();
  if (!sub) return "";
  return `<div class="art-sm-text art-muted mb-10">${escapeHtml(sub)}</div>`;
}

function relatedGroupsHtml(project) {
  const ml = project.relatedMlSystems || [];
  const dt = project.relatedDataTools || [];
  const legacy = project.relatedSystems || [];
  if (!ml.length && !dt.length && legacy.length) {
    return `<div class="art-knowledge-block p-30-15"><h6 class="mb-15">Related</h6><ul class="art-knowledge-list p-15-0">${linkItems(legacy)}</ul></div>`;
  }
  let html = "";
  if (ml.length) {
    html += `<div class="art-knowledge-block p-30-15"><h6 class="mb-15">ML systems</h6><ul class="art-knowledge-list p-15-0">${linkItems(ml)}</ul></div>`;
  }
  if (dt.length) {
    html += `<div class="art-knowledge-block p-30-15"><h6 class="mb-15">Data tools</h6><ul class="art-knowledge-list p-15-0">${linkItems(dt)}</ul></div>`;
  }
  return html;
}

function mergeProject(baseProject, profileName) {
  const variant = baseProject.profiles?.[profileName] || {};
  return {
    ...baseProject,
    ...variant,
  };
}

function applyFeaturesUrls(bodyHtml, featuresPublicUrl, featuresDocsUrl) {
  const base = String(featuresPublicUrl).replace(/\/+$/, "");
  const pub = `${base}/`;
  const docs = String(featuresDocsUrl);
  const health = `${base}/health`;
  const catalog = `${base}/features`;
  return bodyHtml
    .replaceAll("{{FEATURES_PUBLIC_URL}}", escapeHtml(pub))
    .replaceAll("{{FEATURES_DOCS_URL}}", escapeHtml(docs))
    .replaceAll("{{FEATURES_HEALTH_URL}}", escapeHtml(health))
    .replaceAll("{{FEATURES_CATALOG_URL}}", escapeHtml(catalog));
}

function applyInteractivePlaceholders(bodyHtml, project) {
  const eyebrow =
    project.interactiveDemoEyebrow ||
    project.eyebrow ||
    "Interactive demo";
  const lead =
    project.interactiveDemoLead ||
    project.summary ||
    "";
  const scope =
    project.interactiveDemoScopeNote ||
    "Small scope: consistent offline batch features — not Feast, Tecton, or a hosted product.";
  return bodyHtml
    .replaceAll("{{INTERACTIVE_DEMO_EYEBROW}}", escapeHtml(eyebrow))
    .replaceAll("{{INTERACTIVE_DEMO_LEAD}}", escapeHtml(lead))
    .replaceAll("{{INTERACTIVE_SCOPE_NOTE}}", escapeHtml(scope));
}

function renderHtml(
  template,
  {
    bodyHtml,
    mainTopHtml,
    headExtraHtml,
    headerNavExtraHtml,
    navLinkItems,
    pageTitle,
    metaDescription,
    themeColor,
    project,
    profile,
    logo1,
    logo2,
    heroTitle,
    footerTitle,
    footerSub,
    railHomeTitle,
    railAsideLabel,
  }
) {
  const headerNav = navLinkItems ?? project.projectLinks ?? [];
  return template
    .replaceAll("{{HEAD_EXTRA_HTML}}", headExtraHtml)
    .replaceAll("{{HEADER_NAV_EXTRA_HTML}}", headerNavExtraHtml || "")
    .replaceAll("{{MAIN_TOP_HTML}}", mainTopHtml)
    .replaceAll("{{PAGE_TITLE}}", escapeHtml(pageTitle))
    .replaceAll("{{META_DESCRIPTION}}", escapeHtml(metaDescription))
    .replaceAll("{{THEME_COLOR}}", escapeHtml(themeColor))
    .replaceAll("{{PORTFOLIO_URL}}", escapeHtml(profile.portfolioUrl))
    .replaceAll("{{RAIL_HOME_TITLE}}", escapeHtml(railHomeTitle))
    .replaceAll("{{RAIL_ASIDE_LABEL}}", escapeHtml(railAsideLabel))
    .replaceAll("{{AVATAR_URL}}", escapeHtml(profile.avatarUrl))
    .replaceAll("{{PROFILE_NAME}}", escapeHtml(profile.name))
    .replaceAll("{{PROFILE_IDENTITY}}", escapeHtml(profile.identity))
    .replaceAll("{{PROFILE_IDENTITY_SUB_HTML}}", identitySubHtml(profile))
    .replaceAll("{{PROFILE_LOCATION}}", escapeHtml(profile.location))
    .replaceAll(
      "{{TECHNICAL_FOCUS_ITEMS}}",
      listItems(profile.technicalFocus || [])
    )
    .replaceAll(
      "{{REVIEW_SECTION_TITLE}}",
      escapeHtml(profile.reviewSectionTitle || "Review")
    )
    .replaceAll("{{REVIEW_LINK_ITEMS}}", linkItems(project.reviewLinks || []))
    .replaceAll("{{RELATED_GROUPS_HTML}}", relatedGroupsHtml(project))
    .replaceAll(
      "{{SOCIAL_LINK_ITEMS}}",
      socialItems(profile.socialLinks || [])
    )
    .replaceAll("{{LOGO_LINE_1}}", escapeHtml(logo1))
    .replaceAll("{{LOGO_LINE_2}}", escapeHtml(logo2))
    .replaceAll("{{NAV_BUTTONS}}", ctaButtons(headerNav))
    .replaceAll(
      "{{PROJECT_EYEBROW}}",
      escapeHtml(project.eyebrow || "Portfolio artifact")
    )
    .replaceAll("{{PROJECT_HERO_TITLE}}", escapeHtml(heroTitle))
    .replaceAll("{{PROJECT_SUMMARY}}", escapeHtml(project.summary))
    .replaceAll("{{HERO_NOTE_HTML}}", heroNoteHtml(project.heroNote))
    .replaceAll("{{METRICS_ITEMS}}", metricsFromStack(project.stack || []))
    .replaceAll(
      "{{LIMITATIONS_SECTION_HTML}}",
      limitationsSection(project.limitations || [])
    )
    .replaceAll("{{CONTENT_HTML}}", bodyHtml)
    .replaceAll("{{PROJECT_FOOTER_TITLE}}", escapeHtml(footerTitle))
    .replaceAll("{{PROJECT_FOOTER_SUB}}", escapeHtml(footerSub))
    .replaceAll(
      "{{FOOTER_TAGLINE}}",
      escapeHtml(
        profile.footerTagline || "Applied ML / data systems and APIs."
      )
    );
}

async function readJson(filePath) {
  return JSON.parse(await fs.readFile(filePath, "utf8"));
}

async function readProfile(profileName) {
  const profilePath = path.join(ROOT, "profiles", `${profileName}.json`);
  return readJson(profilePath);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const projectPath = path.resolve(args.project);
  const bodyPath = path.resolve(args.body);
  const outDir = path.resolve(args.out);
  const profileName = args.profile || DEFAULT_PROFILE;
  const demoBodyArg = args["demo-body"];
  const demoOutName = args["demo-out"] || "portfolio-demo.html";

  const profile = await readProfile(profileName);
  const projectBase = await readJson(projectPath);
  const project = mergeProject(projectBase, profileName);
  const template = await fs.readFile(path.join(ROOT, "index.html"), "utf8");
  let bodyHtml = await fs.readFile(bodyPath, "utf8");

  const fp = project.featuresPublicUrl;
  const featuresPublicUrl =
    fp != null && String(fp).trim() !== ""
      ? String(fp).replace(/\/?$/, "/")
      : "https://features.vahdetkaratas.com/";
  const fd = project.featuresDocsUrl;
  const featuresDocsUrl =
    fd != null && String(fd).trim() !== ""
      ? String(fd)
      : `${String(featuresPublicUrl).replace(/\/$/, "")}/docs`;
  bodyHtml = applyFeaturesUrls(bodyHtml, featuresPublicUrl, featuresDocsUrl);

  const pageTitle =
    project.pageTitle || `${project.title} - ${profile.name}`;
  const metaDescription = project.metaDescription || project.summary;
  const themeColor = project.themeColor || "#1a1a1f";
  const logo1 = project.logoLine1 || "Feature";
  const logo2 = project.logoLine2 || "store mini";
  const heroTitle = project.heroTitle || project.title;
  const footerTitle = project.footerProjectName || "Feature Store Mini";
  const footerSub =
    project.footerProjectSub ||
    "Batch pipeline · definitions · validation · FastAPI";
  const railHomeTitle = profile.railHomeTitle || "Portfolio home";
  const railAsideLabel = profile.railAsideLabel || "Home";

  const shared = {
    themeColor,
    project,
    profile,
    logo1,
    logo2,
    heroTitle,
    footerTitle,
    footerSub,
    railHomeTitle,
    railAsideLabel,
  };

  const indexRendered = renderHtml(template, {
    ...shared,
    bodyHtml,
    mainTopHtml: MAIN_TOP_FULL,
    headExtraHtml: "",
    headerNavExtraHtml: "",
    pageTitle,
    metaDescription,
  });

  await fs.rm(outDir, { recursive: true, force: true });
  await fs.mkdir(outDir, { recursive: true });
  await fs.writeFile(path.join(outDir, "index.html"), indexRendered, "utf8");

  if (demoBodyArg) {
    let demoBody = await fs.readFile(path.resolve(demoBodyArg), "utf8");
    demoBody = applyFeaturesUrls(
      demoBody,
      featuresPublicUrl,
      featuresDocsUrl
    );
    demoBody = applyInteractivePlaceholders(demoBody, project);
    const demoOriginRaw = project.demoFileApiOrigin;
    const demoOriginDefault = String(featuresPublicUrl).replace(/\/$/, "");
    const demoOrigin =
      demoOriginRaw != null && String(demoOriginRaw).trim() !== ""
        ? String(demoOriginRaw).trim().replace(/\/$/, "")
        : demoOriginDefault;
    const headExtraDemo = `<meta name="fs-api-origin" content="${escapeHtml(demoOrigin)}">\n  <link rel="stylesheet" href="fs-portfolio-demo.css">`;
    const demoPageTitle =
      project.demoPageTitle || `${project.title} — interactive demo`;
    const demoMeta =
      project.demoMetaDescription ||
      project.metaDescription ||
      project.summary ||
      "";
    const demoRendered = renderHtml(template, {
      ...shared,
      bodyHtml: demoBody,
      mainTopHtml: MAIN_TOP_DEMO,
      headExtraHtml: headExtraDemo,
      headerNavExtraHtml: "",
      navLinkItems: project.demoProjectLinks || project.projectLinks || [],
      pageTitle: demoPageTitle,
      metaDescription: demoMeta,
    });
    await fs.writeFile(
      path.join(outDir, demoOutName),
      demoRendered,
      "utf8"
    );
    console.log(`Also wrote ${demoOutName} (interactive demo).`);
  }

  for (const file of [
    "shell.css",
    "demo-content.css",
    "shell.js",
    "favicon.svg",
    "fs-portfolio-demo.css",
  ]) {
    const src = path.join(ROOT, file);
    try {
      await fs.copyFile(src, path.join(outDir, file));
    } catch (e) {
      if (file !== "favicon.svg") throw e;
    }
  }

  await fs.writeFile(
    path.join(outDir, "profile.json"),
    `${JSON.stringify(profile, null, 2)}\n`,
    "utf8"
  );

  console.log(`Rendered shell to ${outDir} using profile '${profileName}'`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
