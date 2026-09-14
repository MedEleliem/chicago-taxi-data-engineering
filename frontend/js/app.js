"use strict";
const API_BASE = "/api";
const $ = id => document.getElementById(id);
const state = { page: "overview", health: null, offset: 0, sort: "trip_start_timestamp", descending: true,
  map: null, mapReady: null, mode: "density", geo: null, zones: [], selectedTrip: null, request: 0 };
const palette = { primary: "#6257d6", green: "#21987d", gray: "#a3aabc", red: "#c96c6b", amber: "#c39241" };
const titles = { overview: ["Chicago Taxi Intelligence", "Operational & Revenue Analytics"], operations: ["Operations", "Trip activity, operators & payment patterns"],
  geography: ["Geography", "The geography of Chicago mobility"], quality: ["Data Quality", "From source records to trusted analytics"] };
const fmt = (value, digits = 0) => value == null ? "—" : Number(value).toLocaleString("en-US", { maximumFractionDigits: digits });
const money = value => value == null ? "—" : Number(value).toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
const moneyExact = value => value == null ? "—" : Number(value).toLocaleString("en-US", { style: "currency", currency: "USD" });
const percent = value => value == null ? "—" : `${fmt(value * 100, 1)}%`;
const escapeHTML = value => String(value ?? "—").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
const icons = () => window.lucide?.createIcons();

function selectedParams(quality = false) {
  const params = new URLSearchParams();
  if ($("start-date").value) params.set("start_date", $("start-date").value);
  if ($("end-date").value) params.set("end_date", $("end-date").value);
  if (!quality && $("company").value) params.set("company", $("company").value);
  if (!quality && $("payment").value) params.set("payment_type", $("payment").value);
  return params;
}
async function api(path, params = selectedParams()) {
  const response = await fetch(`${API_BASE}${path}?${params}`, { cache: "no-store" });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(typeof body.detail === "string" ? body.detail : `Request failed (${response.status}).`);
  }
  return response.json();
}
const fetchKPIs = () => api("/kpis");
const fetchDaily = () => api("/daily");
const fetchHourly = () => api("/hourly");
const fetchZones = () => api("/zones");
const fetchDQ = () => Promise.all(["summary", "errors", "warnings"].map(kind => api(`/data-quality/${kind}`, selectedParams(true))));
async function fetchTrips() {
  const params = selectedParams();
  params.set("limit", "12"); params.set("offset", state.offset); params.set("sort", state.sort);
  params.set("descending", state.descending); params.set("search", $("trip-search").value);
  return api("/trips", params);
}
function feedback(message, neutral = false) {
  $("feedback").textContent = message;
  $("feedback").classList.toggle("neutral", neutral);
  $("feedback").hidden = !message;
}
function kpis(target, items) {
  $(target).innerHTML = items.map(([label, value, unit, icon]) => `<article class="kpi"><div class="kpi-label"><span>${label}</span><i data-lucide="${icon}"></i></div><div class="kpi-value">${value}</div><div class="kpi-unit">${unit}</div></article>`).join("");
  icons();
}
function chart(id, traces, options = {}) {
  if (!window.Plotly) { $(id).textContent = "Charts unavailable. Check your connection and refresh."; return; }
  const empty = !traces.length || traces.every(trace => !(trace.x?.length || trace.z?.length));
  const layout = { margin: { l: 44, r: 12, t: 26, b: 36 }, paper_bgcolor: "transparent", plot_bgcolor: "transparent",
    font: { family: "Inter, sans-serif", size: 10, color: "#667085" }, hoverlabel: { bgcolor: "#fff", font: { size: 12 }, bordercolor: "#e5e9f0" },
    showlegend: false, hovermode: "closest", xaxis: { showgrid: false, zeroline: false, fixedrange: true, automargin: true },
    yaxis: { gridcolor: "#e9edf3", zeroline: false, fixedrange: true, automargin: true },
    annotations: empty ? [{ text: "No data for this selection", showarrow: false, xref: "paper", yref: "paper", x: .5, y: .5 }] : [], ...options,
    width: $(id).clientWidth, autosize: true };
  layout.margin.l = Math.min(layout.margin.l, Math.round($(id).clientWidth * .46));
  return Plotly.react(id, traces, layout, { responsive: true, displayModeBar: false, displaylogo: false });
}
function hours(rows) {
  const totals = Array(24).fill(0);
  rows.forEach(row => { totals[row.trip_hour] += row.trip_count; });
  return totals;
}
function bar(x, y, color = palette.primary, horizontal = false) {
  return { type: "bar", x, y, orientation: horizontal ? "h" : "v", marker: { color, line: { width: 0 } },
    hovertemplate: horizontal ? "%{y}<br>%{x:,.0f}<extra></extra>" : "%{x}<br>%{y:,.0f}<extra></extra>" };
}
function drawOverview(kpi, daily, hourly) {
  kpis("overview-kpis", [["Total trips", fmt(kpi.total_trips), "Validated trips", "car-front"], ["Revenue", money(kpi.total_revenue), "Total trip revenue", "wallet"],
    ["Avg trip value", moneyExact(kpi.avg_trip_total), "Per validated trip", "badge-dollar-sign"], ["Avg distance", fmt(kpi.avg_trip_miles, 1), "Miles per trip", "route"],
    ["Avg duration", fmt(kpi.avg_trip_duration_minutes, 1), "Minutes per trip", "clock-3"], ["Tip rate", percent(kpi.tipped_trip_rate), "Trips with a tip", "heart-handshake"]]);
  chart("daily-trips", [{ type: "scatter", mode: "lines+markers", x: daily.map(r => r.trip_date), y: daily.map(r => r.trip_count),
    line: { color: palette.primary, width: 2.5, shape: "linear" }, marker: { size: 6, color: palette.primary, line: { color: "white", width: 2 } },
    fill: "tozeroy", fillcolor: "rgba(98,87,214,.065)", hovertemplate: "%{x|%b %d}<br>%{y:,.0f} trips<extra></extra>" }], { xaxis: { type: "date", tickformat: "%b %d", showgrid: false, fixedrange: true } });
  chart("daily-revenue", [bar(daily.map(r => r.trip_date), daily.map(r => r.total_revenue), palette.green)], { bargap: .55, yaxis: { tickprefix: "$", gridcolor: "#e9edf3", fixedrange: true }, xaxis: { type: "date", tickformat: "%b %d", showgrid: false, fixedrange: true } });
  chart("overview-hourly", [bar(Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2, "0")}:00`), hours(hourly))], { bargap: .4 });
}
function drawOperations(kpi, hourly, companies, payments) {
  kpis("operations-kpis", [["Average duration", `${fmt(kpi.avg_trip_duration_minutes, 1)} min`, "Per validated trip", "clock-3"],
    ["Average distance", `${fmt(kpi.avg_trip_miles, 1)} mi`, "Per validated trip", "route"], ["Active taxis", fmt(kpi.unique_taxis), "Distinct taxi identifiers", "car-front"]]);
  const z = Array.from({ length: 7 }, () => Array(24).fill(0));
  hourly.forEach(row => { const day = (new Date(`${String(row.trip_date).slice(0, 10)}T12:00:00Z`).getUTCDay() + 6) % 7; z[day][row.trip_hour] += row.trip_count; });
  chart("activity-heatmap", [{ type: "heatmap", x: Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2, "0")}:00`),
    y: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], z, xgap: 3, ygap: 3,
    colorscale: [[0, "#f1f0fb"], [.3, "#d2cef0"], [.65, "#9489df"], [1, "#5546c1"]], showscale: false,
    hovertemplate: "%{y}, %{x}<br>%{z} trips<extra></extra>" }], { yaxis: { autorange: "reversed", showgrid: false, fixedrange: true } });
  const companyRevenue = [...companies].sort((a, b) => a.total_revenue - b.total_revenue).slice(-8);
  chart("company-revenue", [bar(companyRevenue.map(r => r.total_revenue), companyRevenue.map(r => r.company), palette.green, true)], { margin: { l: 115, r: 20, t: 25, b: 35 } });
  const ranked = [...companies].sort((a, b) => a.trip_count - b.trip_count).slice(-8);
  chart("company-ranking", [bar(ranked.map(r => r.trip_count), ranked.map(r => r.company), palette.primary, true)], { margin: { l: 115, r: 20, t: 25, b: 35 } });
  chart("payment-mix", [bar(payments.map(r => r.payment_type), payments.map(r => r.trip_count), "#8793a8")]);
}
function drawTrips(result) {
  $("trip-rows").innerHTML = result.items.map(row => `<tr><td><button class="trip-link" data-trip="${escapeHTML(row.trip_id)}">${escapeHTML(row.trip_id.slice(0, 16))}</button></td><td>${escapeHTML(row.trip_start_timestamp?.replace("T", " ").slice(0, 16))}</td><td>${escapeHTML(row.company)}</td><td>${escapeHTML(row.payment_type)}</td><td>${fmt(row.trip_miles, 1)} mi</td><td>${moneyExact(row.trip_total)}</td></tr>`).join("") || '<tr><td colspan="6">No matching trips</td></tr>';
  $("trip-count").textContent = result.total ? `${fmt(result.offset + 1)}–${fmt(Math.min(result.total, result.offset + result.limit))} of ${fmt(result.total)} trips` : "0 trips";
  $("prev").disabled = state.offset === 0; $("next").disabled = state.offset + result.limit >= result.total;
  $("trip-rows").querySelectorAll("[data-trip]").forEach(button => button.addEventListener("click", () => selectTrip(button.dataset.trip)));
}
function drawQuality(summary, errors, warnings) {
  kpis("quality-kpis", [["Bronze rows", fmt(summary.bronze_rows), "Source records", "database"], ["Silver valid", fmt(summary.valid_rows), "Trusted records", "shield-check"],
    ["Rejected", fmt(summary.rejected_rows), "Quarantined records", "shield-alert"], ["Warning rows", fmt(summary.warning_rows), "Includes rejected rows", "triangle-alert"],
    ["Rejection rate", percent(summary.rejection_rate), "Of Silver input", "circle-x"], ["Warning rate", percent(summary.warning_rate), "Of Silver input", "flag"]]);
  $("quality-funnel").innerHTML = `<div class="funnel-node"><span>BRONZE</span><strong>${fmt(summary.bronze_rows)}</strong></div><i data-lucide="arrow-right"></i><div class="funnel-node"><span>SILVER INPUT</span><strong>${fmt(summary.input_rows)}</strong></div><i data-lucide="git-fork"></i><div class="funnel-node valid"><span>VALID</span><strong>${fmt(summary.valid_rows)}</strong></div><div class="funnel-node rejected"><span>QUARANTINE</span><strong>${fmt(summary.rejected_rows)}</strong></div>`;
  const rules = (id, rows, color) => { const selected = rows.slice(0, 7).reverse(); chart(id, [bar(selected.map(r => r.count), selected.map(r => r.rule.replaceAll("_", " ")), color, true)], { margin: { l: 190, r: 25, t: 25, b: 35 } }); };
  rules("error-chart", errors, palette.red); rules("warning-chart", warnings, palette.amber);
  $("run-rows").innerHTML = summary.runs.map(row => `<tr><td>${row.processing_date}</td>${["bronze_run_id", "silver_run_id", "gold_run_id"].map(key => `<td class="run-id" title="${escapeHTML(row[key])}">${escapeHTML(row[key])}</td>`).join("")}<td><span class="status-tag">${row.status}</span></td><td>${row.pagination_complete ? "Complete" : "Partial"}</td></tr>`).join("") || '<tr><td colspan="6">No published runs in this date range</td></tr>';
  icons();
}

async function ensureMap() {
  if (state.mapReady) return state.mapReady;
  state.mapReady = (async () => {
    if (!window.maplibregl) throw new Error("Map library unavailable. Check your connection and refresh.");
    const style = await api("/map-style", new URLSearchParams());
    const map = new maplibregl.Map({ container: "map", style, center: [-87.649, 41.901], zoom: 11, attributionControl: true });
    state.map = map;
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    map.on("error", () => { $("map-error").textContent = "Some map tiles could not be loaded. Data overlays remain available."; $("map-error").hidden = false; });
    await new Promise((resolve, reject) => { const timer = setTimeout(() => reject(new Error("Map loading timed out.")), 20000); map.once("load", () => { clearTimeout(timer); resolve(); }); });
    const empty = { type: "FeatureCollection", features: [] };
    ["pickups", "routes", "areas"].forEach(name => map.addSource(name, { type: "geojson", data: empty }));
    map.addLayer({ id: "density", type: "heatmap", source: "pickups", paint: { "heatmap-radius": 25, "heatmap-opacity": .7,
      "heatmap-color": ["interpolate", ["linear"], ["heatmap-density"], 0, "rgba(98,87,214,0)", .2, "#ced4eb", .5, "#9b93d8", .8, "#7566c0", 1, "#43388b"] } });
    map.addLayer({ id: "routes", type: "line", source: "routes", paint: { "line-color": "#6651c6", "line-width": 1.5, "line-opacity": .22 } });
    map.addLayer({ id: "pickup-points", type: "circle", source: "pickups", paint: { "circle-radius": 4, "circle-color": palette.primary, "circle-stroke-color": "#fff", "circle-stroke-width": 1 } });
    map.addLayer({ id: "area-circles", type: "circle", source: "areas", paint: { "circle-radius": 12, "circle-color": palette.primary, "circle-opacity": .65, "circle-stroke-width": 2, "circle-stroke-color": "#fff" } });
    map.on("click", "pickup-points", event => showTrip(event.features[0].properties));
    map.on("click", "routes", event => showTrip(event.features[0].properties));
    map.on("click", "area-circles", event => { const feature = event.features[0]; const div = document.createElement("div"); div.textContent = `Pickup area ${feature.properties.area}: ${state.mode === "revenue-area" ? money(feature.properties.value) : fmt(feature.properties.value) + " trips"}`; new maplibregl.Popup().setLngLat(feature.geometry.coordinates).setDOMContent(div).addTo(map); });
    ["pickup-points", "routes", "area-circles"].forEach(layer => { map.on("mouseenter", layer, () => { map.getCanvas().style.cursor = "pointer"; }); map.on("mouseleave", layer, () => { map.getCanvas().style.cursor = ""; }); });
    return map;
  })();
  return state.mapReady;
}
async function drawMap(geo, zones) {
  state.geo = geo; state.zones = zones;
  await ensureMap();
  state.map.getSource("routes").setData(geo);
  state.map.getSource("pickups").setData({ type: "FeatureCollection", features: geo.features.map(feature => ({ ...feature, geometry: { type: "Point", coordinates: feature.geometry.coordinates[0] } })) });
  $("map-count").textContent = `${fmt(geo.returned)} of ${fmt(geo.total)} geolocated trips`;
  updateMapMode(); fitMap();
}
function updateMapMode() {
  document.querySelectorAll("[data-mode]").forEach(button => button.setAttribute("aria-pressed", button.dataset.mode === state.mode));
  if (!state.map?.getLayer("density")) return;
  const areaMode = state.mode.endsWith("area");
  const visibility = { density: state.mode === "density", routes: state.mode === "explorer", "pickup-points": !areaMode, "area-circles": areaMode };
  for (const [layer, visible] of Object.entries(visibility)) state.map.setLayoutProperty(layer, "visibility", visible ? "visible" : "none");
  state.map.setPaintProperty("pickup-points", "circle-opacity", state.mode === "density" ? .15 : .85);
  state.map.setPaintProperty("pickup-points", "circle-stroke-opacity", state.mode === "density" ? .1 : 1);
  const points = state.zones.filter(row => row.latitude != null && row.longitude != null).map(row => ({ type: "Feature", geometry: { type: "Point", coordinates: [row.longitude, row.latitude] },
    properties: { area: row.pickup_community_area ?? "Unknown", value: state.mode === "revenue-area" ? row.total_revenue : row.trip_count } }));
  state.map.getSource("areas").setData({ type: "FeatureCollection", features: points });
  const max = Math.max(1, ...points.map(point => point.properties.value));
  state.map.setPaintProperty("area-circles", "circle-radius", ["interpolate", ["linear"], ["get", "value"], 0, 8, max, 36]);
  state.map.setPaintProperty("area-circles", "circle-color", state.mode === "revenue-area" ? palette.green : palette.primary);
  $("map-legend-text").textContent = { density: "Pickup concentration", "trips-area": "Area trip totals · proportional circles", "revenue-area": "Area revenue · proportional circles", explorer: "Published centroid connections" }[state.mode];
}
function fitMap() {
  if (!state.map || !state.geo?.features.length) return;
  const bounds = new maplibregl.LngLatBounds();
  state.geo.features.forEach(feature => feature.geometry.coordinates.forEach(point => bounds.extend(point)));
  state.map.fitBounds(bounds, { padding: 50, maxZoom: 13, duration: 350 });
}
function showTrip(row) {
  state.selectedTrip = row;
  const fields = [["Company", row.company], ["Payment", row.payment_type], ["Start", row.trip_start_timestamp?.replace("T", " ").slice(0, 19)],
    ["End", row.trip_end_timestamp?.replace("T", " ").slice(0, 19)], ["Duration", `${fmt(row.trip_duration_minutes, 1)} min`], ["Distance", `${fmt(row.trip_miles, 1)} mi`],
    ["Fare", moneyExact(row.fare)], ["Tip", moneyExact(row.tips)], ["Total", moneyExact(row.trip_total)], ["Pickup area", row.pickup_community_area], ["Dropoff area", row.dropoff_community_area]];
  $("trip-detail").innerHTML = `<span class="eyebrow">SELECTED TRIP</span><h2>Trip details</h2><div class="trip-id">${escapeHTML(row.trip_id)}</div><dl>${fields.map(([label, value]) => `<div class="${["Start", "End"].includes(label) ? "wide" : ""}"><dt>${label}</dt><dd>${escapeHTML(value)}</dd></div>`).join("")}</dl><div class="places"><h3>Nearby places</h3><button class="primary-button" id="nearby"><i data-lucide="map-pin"></i>At pickup</button><div id="place-results"></div></div>`;
  const lat = row.pickup_centroid_latitude, lon = row.pickup_centroid_longitude;
  $("nearby").disabled = lat == null || lon == null;
  $("nearby").addEventListener("click", async () => {
    $("nearby").disabled = true; $("place-results").textContent = "Loading nearby places…";
    try { const result = await api("/places/nearby", new URLSearchParams({ lat, lon }));
      $("place-results").innerHTML = result.available ? result.places.map(place => `<div class="place-item">${escapeHTML(place.name)}<small>${escapeHTML(place.category)} · ${fmt(place.distance)} m</small><small>${escapeHTML(place.address)}</small></div>`).join("") || "No nearby places found." : "Nearby places are currently unavailable.";
    } catch { $("place-results").textContent = "Nearby places are currently unavailable."; }
    finally { if ($("nearby")) $("nearby").disabled = false; }
  }); icons();
}
async function selectTrip(id) {
  try { const result = await api(`/trips/${encodeURIComponent(id)}`); showTrip(result.trip); state.mode = "explorer"; location.hash = "geography"; }
  catch (error) { feedback(error.message); }
}
async function reload() {
  const ticket = ++state.request;
  feedback(""); $("loading").hidden = false; $("content").setAttribute("aria-busy", "true");
  document.querySelectorAll(".period-label").forEach(el => { el.textContent = `${$("start-date").value} — ${$("end-date").value}`; });
  try {
    if (state.page === "overview") {
      const results = await Promise.all([fetchKPIs(), fetchDaily(), fetchHourly()]);
      if (ticket !== state.request) return;
      drawOverview(...results); if (!results[0].total_trips) feedback("No trips match the selected filters.", true);
    } else if (state.page === "operations") {
      const results = await Promise.all([fetchKPIs(), fetchHourly(), api("/companies"), api("/payments"), fetchTrips()]);
      if (ticket !== state.request) return;
      drawOperations(...results); drawTrips(results[4]);
    } else if (state.page === "quality") {
      const results = await fetchDQ(); if (ticket !== state.request) return; drawQuality(...results);
    } else {
      const results = await Promise.all([api("/geo/trips"), fetchZones()]); if (ticket !== state.request) return; await drawMap(...results);
    }
    $("updated").textContent = `Updated ${new Date().toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" })}`;
  } catch (error) { if (ticket === state.request) feedback(error.message); }
  finally { if (ticket === state.request) { $("loading").hidden = true; $("content").setAttribute("aria-busy", "false"); } }
}
function navigate() {
  state.page = Object.hasOwn(titles, location.hash.slice(1)) ? location.hash.slice(1) : "overview";
  document.querySelectorAll(".page").forEach(section => { section.hidden = section.id !== state.page; });
  document.querySelectorAll("[data-page]").forEach(link => { link.classList.toggle("active", link.dataset.page === state.page); link.setAttribute("aria-current", link.dataset.page === state.page ? "page" : "false"); });
  $("page-title").textContent = titles[state.page][0]; $("page-subtitle").textContent = titles[state.page][1];
  $("crumb").textContent = state.page === "quality" ? "Data Quality" : state.page[0].toUpperCase() + state.page.slice(1);
  $("company").disabled = $("payment").disabled = state.page === "quality";
  state.offset = 0;
  if (state.health?.ready) reload();
  if (state.map && state.page === "geography") setTimeout(() => state.map.resize(), 0);
}
async function boot() {
  icons();
  try {
    state.health = await api("/health", new URLSearchParams());
    $("connection").textContent = state.health.ready ? "Gold connected" : "No published data";
    $("demo-banner").hidden = state.health.mode !== "demo";
    $("start-date").value = state.health.dates[0] || ""; $("end-date").value = state.health.dates.at(-1) || "";
    for (const [id, key, label] of [["company", "companies", "All companies"], ["payment", "payments", "All payments"]]) {
      $(id).replaceChildren(new Option(label, ""), ...state.health[key].map(value => new Option(value, value)));
    }
    navigate();
    if (!state.health.ready) feedback("No validated Gold data is available for this workspace.", true);
  } catch (error) { feedback(error.message); $("connection").textContent = "Disconnected"; }
}
$("filters").addEventListener("submit", event => { event.preventDefault(); if ($("start-date").value > $("end-date").value) return feedback("The start date must be before the end date."); state.offset = 0; reload(); });
$("reset").addEventListener("click", () => { $("company").value = $("payment").value = ""; $("start-date").value = state.health?.dates[0] || ""; $("end-date").value = state.health?.dates.at(-1) || ""; state.offset = 0; reload(); });
$("refresh").addEventListener("click", () => state.health?.ready ? reload() : boot());
$("prev").addEventListener("click", () => { state.offset = Math.max(0, state.offset - 12); reload(); });
$("next").addEventListener("click", () => { state.offset += 12; reload(); });
let searchTimer;
$("trip-search").addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => { state.offset = 0; reload(); }, 250); });
document.querySelectorAll("[data-sort]").forEach(button => button.addEventListener("click", () => { state.descending = state.sort === button.dataset.sort ? !state.descending : true; state.sort = button.dataset.sort; state.offset = 0; reload(); }));
document.querySelectorAll("[data-mode]").forEach(button => button.addEventListener("click", () => { state.mode = button.dataset.mode; updateMapMode(); }));
$("fit-map").addEventListener("click", fitMap);
window.addEventListener("hashchange", navigate);
window.addEventListener("resize", () => {
  if (window.Plotly) document.querySelectorAll(".page:not([hidden]) .js-plotly-plot").forEach(el => Plotly.relayout(el, { width: el.clientWidth }));
});
boot();
