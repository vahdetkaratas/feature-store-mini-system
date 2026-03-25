# layout-shell

**This repo (Feature Store Mini):** FastAPI serves this folder at **`/layout-shell`**. The live demo is **`index.html`** (shell + CSV upload UI in `#projectContent`); main-column-only tweaks live in **`demo-content.css`**. **`GET /`** on the API redirects to **`/layout-shell/index.html`**.

**Other projects:** `vercel_demo/static/layout-shell.css` (kept in sync with `styles.css` here) + page-specific CSS for churn demos, etc.

**Desktop (≥921px) — uzun içerik:** Sol sütun + sağ rail, ana sütunla **aynı yüksekliğe** uzar; kaydırma **pencerede** (iç kolon scroll’u kapalı). Kısa sayfalarda `min-height: calc(100vh - 30px)` korunur.

## Left column (birebir kaynak)

- **DOM + class’lar:** `src/app/_layouts/info-bar/Index.jsx` ile aynı (`art-info-bar`, `art-header`, `art-avatar`, `art-table`, `art-knowledge-block`, `art-knowledge-list`, `art-ls-social`, …).
- **CSS:** `_info-bar.scss`, `_common.scss` (`.art-table`, `.art-ls-divider`, `.art-sm-text`, padding/margin utilities), `_markup.scss` (info bar boyutu / scroll frame), `_enhancements.scss` (header border, avatar ring, divider opacity, social hover).
- **Scroll:** `#scrollbar2.art-scroll-frame` — `padding: 235px 0 55px` (üstte sabit `art-header` 235px, altta `art-ls-social` 50px için boşluk), `overflow-y: auto`.
- **Liste işaretleri:** Sarı nokta değil; tema ile aynı **Font Awesome 5 Free** `\f00c` (check) — bu yüzden **Font Awesome 5.15.4** CDN kullanılıyor (`fas` / `fab` ana site ile uyumlu).
- **Sosyal şerit:** `padding: 0 35px`, `height: 50px`, `justify-content: space-between`, `background` gradient (`art-ls-social`).
- **Mobil (≤920px):** `_markup.scss` gibi info bar `absolute` + `translateX`; **üç nokta** butonu ile aç/kapa (script `index.html` içinde).

## Sağ şerit

- Menü yok; sadece **ev ikonu** (portfolio URL).

## Footer

- `LayoutDefault.jsx` + `_content.scss` `footer` bloğu.

## Not

Ana projede ikonlar `font-awesome.min.css` (yerel) ile yüklenir; shell tek dosya olduğu için **aynı sürüm** FA5 CDN kullanıldı. Projeyi kopyalarken `../src/app/_styles/css/plugins/font-awesome.min.css` yoluna da çevirebilirsiniz.

## Son kontrol (sol sütun)

- Yatay boşluk **sadece** `.art-info-bar-frame { padding: 0 15px }` — `.art-info-bar` kökünde padding yok (çift gutter yok). `.art-header` / `.art-ls-social` **`left: 0; width: 100%`** (frame içinde; taşma yok). `calc(100% + 30px)` denemesi alandan taşırdığı için kaldırıldı.
- Metin aralığı: `.art-name` için `margin: 0` `.mb-10`’u eziyordu → **`margin: 0 0 10px 0`**. `.art-info-bar` içinde **`line-height: 1.5`**, **`font-size: 13px` / `letter-spacing: 0.5px`** (_common body). Başlık **`line-height: 1.25`**, küçük metin **`1.5`**, sosyal linkler **`13px`** (SCSS’te ayrı font-size yok).
- `#scrollbar2` iç boşluk: **`240px 0 50px`** — `_markup.scss` `.scroll-content` ile aynı.
- Avatar perdesi: flex + `align-self: center` ikon, hover `scale(1.07)` — `_info-bar.scss` (translate yok).
- `.art-knowledge-block > h6` **`margin: 0 0 15px 0`** — `.mb-15` utility’nin ezilmesi giderildi.
- Mobil: `shell-mobile-curtain` + tıklanınca kapanma — `.art-content` + perde davranışına yakın; açık sidebar **`z-index: 1000`**.
