const dataUrls = {
  latest: "./data/latest.json",
  history: "./data/history.json",
};

const formatTemp = (value) => (value == null ? "—" : `${Math.round(value)}°`);
const formatPercent = (value) => (value == null ? "—" : `${Math.round(value)}%`);
const formatWind = (value) => (value == null ? "—" : `${Math.round(value)} km/h`);
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

function renderCountdown(targetDate) {
  const target = new Date(`${targetDate}T12:00:00+02:00`);
  const now = new Date();
  const diffMs = target - now;
  const days = Math.max(0, Math.ceil(diffMs / (1000 * 60 * 60 * 24)));
  document.querySelector("#countdown-days").textContent = String(days);
}

function renderSummary(latest) {
  const forecast = latest.best_forecast;
  const targetLabel = formatDateLabel(latest.target_date);
  document.title = `Haltern am See • Wetter für ${targetLabel}`;
  document.querySelector("#hero-date").textContent = `Haltern am See · ${targetLabel}`;
  document.querySelector("#temp-min").textContent = formatTemp(forecast.temp_min_c);
  document.querySelector("#temp-max").textContent = formatTemp(forecast.temp_max_c);
  document.querySelector("#condition-summary").textContent = forecast.condition_summary || forecast.note;
  document.querySelector("#precip-probability").textContent = formatPercent(forecast.precip_probability_pct);
  document.querySelector("#wind-speed").textContent = formatWind(forecast.wind_kph);
  document.querySelector("#confidence-value").textContent = `${Math.round((latest.confidence || 0) * 100)}%`;
  document.querySelector("#consensus-note").textContent = forecast.note;
  document.querySelector("#coverage-value").textContent =
    `${latest.coverage.available_sources} / ${latest.coverage.total_sources}`;
  document.querySelector("#generated-at").textContent = formatDateTime(latest.generated_at);
  document.querySelector("#hero-note").textContent =
    forecast.status === "available"
      ? "Mehrere Dienste liefern bereits Werte für den Zieltag, der aktuell am nächsten am 1. Mai liegt. Die Seite verdichtet sie zu einer transparenten Gesamtsicht."
      : "Der 1. Mai ist noch nicht sauber belegt. Die Seite zeigt deshalb den nächstliegenden verfügbaren Termin statt einer erfundenen Prognose.";

  const width = `${Math.round((latest.confidence || 0) * 100)}%`;
  document.querySelector("#confidence-fill").style.width = width;
}

function renderSpread(forecast) {
  const spreadGrid = document.querySelector("#spread-grid");
  const cards = [
    ["Spannweite Tmax", forecast.spread.temp_max_c == null ? "—" : `${forecast.spread.temp_max_c}°`],
    ["Spannweite Tmin", forecast.spread.temp_min_c == null ? "—" : `${forecast.spread.temp_min_c}°`],
    [
      "Spannweite Regen",
      forecast.spread.precip_probability_pct == null ? "—" : `${forecast.spread.precip_probability_pct}%`,
    ],
    ["Spannweite Wind", forecast.spread.wind_kph == null ? "—" : `${forecast.spread.wind_kph} km/h`],
  ];

  spreadGrid.innerHTML = cards
    .map(
      ([label, value]) => `
        <article class="spread-card">
          <span class="card-label">${label}</span>
          <strong>${value}</strong>
        </article>
      `,
    )
    .join("");
}

function sourceBadge(status) {
  if (status === "available") return { label: "Live", className: "" };
  if (status === "partial") return { label: "Teilweise", className: "" };
  if (status === "error") return { label: "Fehler", className: "error" };
  return { label: "Noch nicht", className: "pending" };
}

function renderSources(latest) {
  const grid = document.querySelector("#sources-grid");
  const template = document.querySelector("#source-card-template");
  grid.innerHTML = "";

  latest.sources.forEach((source) => {
    const fragment = template.content.cloneNode(true);
    const article = fragment.querySelector(".source-card");
    const badge = fragment.querySelector(".badge");
    const title = fragment.querySelector("h3");
    const link = fragment.querySelector("a");
    const note = fragment.querySelector(".source-note");
    const stats = fragment.querySelector(".source-stats");
    const badgeMeta = sourceBadge(source.status);

    badge.textContent = badgeMeta.label;
    badge.classList.add(...badgeMeta.className.split(" ").filter(Boolean));
    title.textContent = source.source_name;
    link.href = source.source_url;
    note.textContent = source.note || "Werte wurden erfolgreich eingelesen.";

    const statRows = [
      ["Tmin", formatTemp(source.temp_min_c)],
      ["Tmax", formatTemp(source.temp_max_c)],
      ["Regenchance", formatPercent(source.precip_probability_pct)],
      ["Wind", formatWind(source.wind_kph)],
    ];
    stats.innerHTML = statRows.map(([dt, dd]) => `<div><dt>${dt}</dt><dd>${dd}</dd></div>`).join("");
    article.dataset.status = source.status;
    grid.appendChild(fragment);
  });
}

function renderHistory(history) {
  const svg = document.querySelector("#history-chart");
  const legend = document.querySelector("#history-legend");
  const snapshots = history.snapshots || [];

  if (!snapshots.length) {
    svg.innerHTML = `<text x="16" y="90" fill="#526262" font-size="16">Noch keine Historie vorhanden.</text>`;
    legend.textContent = "Der erste Verlaufspunkt entsteht nach dem ersten erfolgreichen History-Run.";
    return;
  }

  const values = snapshots.map((item) => item.best_forecast.temp_max_c).filter((value) => value != null);
  if (!values.length) {
    svg.innerHTML = `<text x="16" y="90" fill="#526262" font-size="16">Bisher keine Temperaturwerte im Archiv.</text>`;
    legend.textContent = `${snapshots.length} Snapshot(s) gespeichert, aber noch ohne veröffentlichte Zieltagswerte.`;
    return;
  }

  const max = Math.max(...values);
  const min = Math.min(...values);
  const range = Math.max(1, max - min);
  const stepX = 600 / Math.max(1, values.length - 1);
  const points = snapshots
    .filter((item) => item.best_forecast.temp_max_c != null)
    .map((item, index) => {
      const x = stepX * index;
      const y = 160 - (((item.best_forecast.temp_max_c - min) / range) * 120 + 20);
      return `${x},${y}`;
    })
    .join(" ");

  svg.innerHTML = `
    <defs>
      <linearGradient id="lineGradient" x1="0" x2="1">
        <stop offset="0%" stop-color="#be8457" />
        <stop offset="100%" stop-color="#2f6f79" />
      </linearGradient>
    </defs>
    <polyline points="${points}" fill="none" stroke="url(#lineGradient)" stroke-width="4" stroke-linecap="round" />
  `;

  const first = snapshots[0].fetched_at;
  const last = snapshots[snapshots.length - 1].fetched_at;
  legend.innerHTML = `<span>Start: ${formatDateTime(first)}</span><span>Ende: ${formatDateTime(last)}</span>`;
}

async function main() {
  const [latestResponse, historyResponse] = await Promise.all([
    fetch(dataUrls.latest),
    fetch(dataUrls.history),
  ]);

  const latest = await latestResponse.json();
  const history = await historyResponse.json();
  renderCountdown(latest.target_date);
  renderSummary(latest);
  renderSpread(latest.best_forecast);
  renderSources(latest);
  renderHistory(history);
}

main().catch((error) => {
  console.error(error);
  document.querySelector("#hero-note").textContent = "Die Daten konnten lokal nicht geladen werden.";
});
