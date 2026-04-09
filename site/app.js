const MAY_2026 = "2026-05-01";

const formatPercent = (value) => (value == null ? "—" : `${Math.round(value)}%`);

const formatTemp = (value) => (value == null ? "—" : `${Math.round(value)} °C`);

const formatDateTime = (value) =>
  new Intl.DateTimeFormat("de-DE", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));

const formatDateLabel = (value) =>
  new Intl.DateTimeFormat("de-DE", {
    weekday: "long",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(new Date(`${value}T12:00:00`));

const daypartLabels = {
  morning: "Morgen",
  afternoon: "Nachmittag",
  evening: "Abend",
};

const delayClass = ["delay-1", "delay-2", "delay-3"];

function escapeAttr(text) {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;");
}

function escapeHtml(text) {
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/"/g, "&quot;");
}

/** @param {string | null | undefined} condition @param {number | null | undefined} pct */
function classifyWeather(condition, pct) {
  const raw = (condition || "").toLowerCase();
  const c = raw.normalize("NFD").replace(/\p{M}/gu, "");
  const p = pct == null ? null : Math.min(100, Math.max(0, Number(pct)));

  if (/regen|schauer|niesel|gewitter/.test(c)) return "rain";
  if (/sonne|klar|heiter|licht/.test(c)) {
    if (p == null || p < 45) return "sun";
    if (p < 70) return "cloud";
    return "rain";
  }
  if (/bewolk|wolk|nebel|bedeckt|trub|dunst/.test(c)) return "cloud";
  if (p != null) {
    if (p >= 60) return "rain";
    if (p <= 22) return "sun";
  }
  return "cloud";
}

function renderSourceLine(latest) {
  const line = document.querySelector("#source-line");
  line.textContent = "";
  line.hidden = true;

  const sel = latest.selected_source;
  if (!sel || typeof sel.source_url !== "string" || typeof sel.source_name !== "string") return;
  if (!/^https?:\/\//i.test(sel.source_url)) return;

  try {
    new URL(sel.source_url);
  } catch {
    return;
  }

  line.hidden = false;
  const intro = document.createTextNode("Vorhersage von ");
  const link = document.createElement("a");
  link.href = sel.source_url;
  link.target = "_blank";
  link.rel = "noopener noreferrer";
  link.className = "source-link";
  link.textContent = sel.source_name;
  const outro = document.createTextNode(" — optimistischste Quelle (Regen ↓, Sonne ↑, Temperatur ↑).");
  line.appendChild(intro);
  line.appendChild(link);
  line.appendChild(outro);
}

function weatherSceneMarkup(weather) {
  return `
    <div class="weather-scene weather-scene--${weather}" aria-hidden="true">
      <div class="ws-sun">
        <span class="ws-sun-body"></span>
        <span class="ws-sun-glow"></span>
        <span class="ws-sun-rays"></span>
      </div>
      <div class="ws-cloud ws-cloud--a"></div>
      <div class="ws-cloud ws-cloud--b"></div>
      <div class="ws-rain">
        <span class="ws-rain-strip ws-rain-strip--1"></span>
        <span class="ws-rain-strip ws-rain-strip--2"></span>
        <span class="ws-rain-strip ws-rain-strip--3"></span>
      </div>
    </div>
  `;
}

function render(latest) {
  const locationName = latest.location?.name ?? "Haltern am See";
  document.querySelector("#location-line").textContent = locationName;

  document.title = "Haltern · 1. Mai";
  document.querySelector("#primary-title").textContent = "1. Mai 2026";

  const fallbackEl = document.querySelector("#fallback-note");
  if (latest.target_date === MAY_2026) {
    fallbackEl.hidden = true;
    fallbackEl.textContent = "";
  } else {
    fallbackEl.hidden = false;
    const actual = formatDateLabel(latest.target_date);
    fallbackEl.textContent = `Die Werte gelten für ${actual}, weil für den 1. Mai 2026 noch nicht genügend Quellen einen vollständigen Tag mit Temperatur liefern.`;
  }

  const dayparts = latest.best_forecast?.dayparts;
  const container = document.querySelector("#dayparts-container");
  if (!dayparts) {
    container.innerHTML = "";
    renderSourceLine(latest);
    return;
  }

  container.innerHTML = Object.entries(daypartLabels)
    .map(([key, label], i) => {
      const values = dayparts[key] ?? {};
      const condition = values.condition_summary || "—";
      const pct = values.precip_probability_pct;
      const temp = values.temperature_celsius;
      const rainLabel = `Regen ${formatPercent(pct)}`;
      const tempLabel = formatTemp(temp);
      const width = pct == null ? 0 : Math.min(100, Math.max(0, pct));
      const weather = classifyWeather(condition, pct);
      const aria = escapeAttr(`${label}: ${condition}, ${rainLabel}, ${tempLabel}`);
      const safeCondition = escapeHtml(condition);
      return `
        <article class="daypart tile animate-in ${delayClass[i]}" data-weather="${weather}" style="--rain-pct: ${width}%" aria-label="${aria}">
          ${weatherSceneMarkup(weather)}
          <div class="daypart-inner">
            <span class="daypart-label">${label}</span>
            <p class="condition">${safeCondition}</p>
            <div class="rain-track" role="presentation" aria-hidden="true">
              <span class="rain-fill"></span>
            </div>
            <span class="rain-value">${rainLabel}</span>
            <span class="temp-value">${tempLabel}</span>
          </div>
        </article>
      `;
    })
    .join("");

  document.querySelector("#generated-at-footer").textContent =
    `Aktualisiert ${formatDateTime(latest.generated_at)}`;

  renderSourceLine(latest);
}

async function main() {
  const response = await fetch("./data/latest.json");
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const latest = await response.json();
  render(latest);
}

main().catch((error) => {
  console.error(error);
  const msg = document.querySelector("#status-message");
  msg.hidden = false;
  msg.textContent = "Die Daten konnten nicht geladen werden.";
});
