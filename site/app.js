const dataUrls = {
  latest: "./data/latest.json",
  history: "./data/history.json",
};

const formatPercent = (value) => (value == null ? "—" : `${Math.round(value)}%`);
const formatHours = (value) => (value == null ? "—" : `${Math.round(value * 10) / 10} h`);
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

function renderCountdown(targetDate) {
  const target = new Date(`${targetDate}T12:00:00+02:00`);
  const now = new Date();
  const diffMs = target - now;
  const days = Math.max(0, Math.ceil(diffMs / (1000 * 60 * 60 * 24)));
  document.querySelector("#countdown-days").textContent = String(days);
}

function renderDaypartCards(container, dayparts, withSpread = false) {
  container.innerHTML = Object.entries(daypartLabels)
    .map(([key, label]) => {
      const values = dayparts[key];
      const weather = values.condition_summary || "—";
      const rain = formatPercent(values.precip_probability_pct);
      const sun = formatHours(values.sunshine_hours);
      const title = withSpread ? `${label} Spannweite` : label;
      return `
        <article class="spread-card">
          <span class="card-label">${title}</span>
          <strong>${weather}</strong>
          <span>Regen ${rain}</span>
          <span>Sonne ${sun}</span>
        </article>
      `;
    })
    .join("");
}

function renderSummary(latest) {
  const forecast = latest.best_forecast;
  const targetLabel = formatDateLabel(latest.target_date);
  document.title = `Haltern am See • Tagesabschnitte für ${targetLabel}`;
  document.querySelector("#hero-date").textContent = `Haltern am See · ${targetLabel}`;
  document.querySelector("#condition-summary").textContent = forecast.note;
  document.querySelector("#confidence-value").textContent = `${Math.round((latest.confidence || 0) * 100)}%`;
  document.querySelector("#consensus-note").textContent = forecast.note;
  document.querySelector("#coverage-value").textContent =
    `${latest.coverage.available_sources} / ${latest.coverage.total_sources}`;
  document.querySelector("#generated-at").textContent = formatDateTime(latest.generated_at);
  document.querySelector("#hero-note").textContent =
    "Die Seite zeigt den naechsten vollstaendigen Tag, fuer den zehn Quellen Wetter, Regenchance und Sonnenstunden in Tagesabschnitten liefern.";
  document.querySelector("#confidence-fill").style.width = `${Math.round((latest.confidence || 0) * 100)}%`;
  renderDaypartCards(document.querySelector("#daypart-summary-grid"), forecast.dayparts);
}

function renderSpread(forecast) {
  renderDaypartCards(document.querySelector("#spread-grid"), forecast.spread, true);
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
    note.textContent = source.note || "Tagesabschnitte erfolgreich eingelesen.";

    stats.innerHTML = Object.entries(daypartLabels)
      .map(([key, label]) => {
        const values = source.dayparts[key];
        return `
          <div>
            <dt>${label}</dt>
            <dd>${values.condition_summary || "—"} · ${formatPercent(values.precip_probability_pct)} · ${formatHours(values.sunshine_hours)}</dd>
          </div>
        `;
      })
      .join("");
    article.dataset.status = source.status;
    grid.appendChild(fragment);
  });
}

function renderHistory(history) {
  const legend = document.querySelector("#history-legend");
  const snapshots = history.snapshots || [];
  if (!snapshots.length) {
    legend.textContent = "Noch keine Historie vorhanden.";
    return;
  }
  legend.innerHTML = snapshots
    .map(
      (item) =>
        `<p>${formatDateTime(item.fetched_at)} · ${item.source_count} vollstaendige Quellen</p>`,
    )
    .join("");
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
