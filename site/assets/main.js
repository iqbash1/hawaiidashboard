const BRAND = getComputedStyle(document.documentElement).getPropertyValue("--brand").trim();
const HI = getComputedStyle(document.documentElement).getPropertyValue("--hi").trim();
const OTHER = getComputedStyle(document.documentElement).getPropertyValue("--other").trim();

async function loadMetric(slug) {
  const res = await fetch(`data/v1/${slug}.json`, { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed to load ${slug}`);
  return await res.json();
}

function makeLineChart(ctx, years, hawaii, other, title, unit) {
  return new Chart(ctx, {
    type: "line",
    data: {
      labels: years,
      datasets: [
        { label: "Hawaiʻi", data: hawaii, borderColor: HI, backgroundColor: HI, borderWidth: 2, tension: 0.2, pointRadius: 2 },
        { label: "Other U.S. States (avg)", data: other, borderColor: OTHER, backgroundColor: OTHER, borderWidth: 2, borderDash:[6,4], tension: 0.2, pointRadius: 2 }
      ]
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        title: { display: true, text: title },
        legend: { position: "bottom" },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const v = ctx.parsed.y;
              return `${ctx.dataset.label}: ${v === null || v === undefined ? "—" : v.toLocaleString(undefined, { maximumFractionDigits: 2 })}${unit ? " " + unit : ""}`;
            }
          }
        }
      },
      scales: {
        y: { title: { display: !!unit, text: unit || "" }, ticks: { callback: (v) => v } },
        x: { ticks: { autoSkip: true, maxTicksLimit: 10 } }
      }
    }
  });
}

function setMetaAndDownloads(slug, containerId, payload) {
  const el = document.getElementById(containerId);
  const src = payload.source || {};
  const notes = (payload.notes || []).map(n => `<li>${n}</li>`).join("");
  const srcLine = src.human_url ? `<a href="${src.human_url}" target="_blank" rel="noopener">Source</a>` : "Source: federal API";
  const jsonUrl = `data/v1/${slug}.json`;
  const csvUrl  = `data/v1/csv/${slug}.csv`;
  el.innerHTML = `
    <div>${srcLine} · Updated: ${new Date(payload.last_updated_utc).toLocaleString()} ·
      <a href="${jsonUrl}" target="_blank" rel="noopener">Download JSON</a> ·
      <a href="${csvUrl}"  target="_blank" rel="noopener">Download CSV</a></div>
    ${notes ? `<ul>${notes}</ul>` : ""}
  `;
}

(async () => {
  try {
    const ypllSlug = "public_health_ypll75_rate";
    const ypll = await loadMetric(ypllSlug);
    makeLineChart(document.getElementById("chart-ypll").getContext("2d"),
      ypll.years, ypll.hawaii, ypll.other_states_avg, ypll.title, ypll.unit);
    setMetaAndDownloads(ypllSlug, "meta-ypll", ypll);
  } catch (e) {
    const t = document.getElementById("meta-ypll"); if (t) t.textContent = "YPLL data unavailable.";
    console.error(e);
  }

  try {
    const unSlug = "public_health_uninsured_share";
    const un = await loadMetric(unSlug);
    makeLineChart(document.getElementById("chart-uninsured").getContext("2d"),
      un.years, un.hawaii, un.other_states_avg, un.title, un.unit);
    setMetaAndDownloads(unSlug, "meta-uninsured", un);
  } catch (e) {
    const t = document.getElementById("meta-uninsured"); if (t) t.textContent = "Uninsured data unavailable.";
    console.error(e);
  }

  try {
    const bbSlug = "broadband_adoption_households_share";
    const bb = await loadMetric(bbSlug);
    makeLineChart(document.getElementById("chart-broadband").getContext("2d"),
      bb.years, bb.hawaii, bb.other_states_avg, bb.title, bb.unit);
    setMetaAndDownloads(bbSlug, "meta-broadband", bb);
  } catch (e) {
    const t = document.getElementById("meta-broadband"); if (t) t.textContent = "Broadband data unavailable.";
    console.error(e);
  }

  try {
    const rnSlug = "electricity_renewables_generation_share";
    const rn = await loadMetric(rnSlug);
    makeLineChart(document.getElementById("chart-renewables").getContext("2d"),
      rn.years, rn.hawaii, rn.other_states_avg, rn.title, rn.unit);
    setMetaAndDownloads(rnSlug, "meta-renewables", rn);
  } catch (e) {
    const t = document.getElementById("meta-renewables"); if (t) t.textContent = "Renewables data unavailable.";
    console.error(e);
  }
})();
