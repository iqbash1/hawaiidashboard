async function loadMetric(slug) {
  const res = await fetch(`data/v1/${slug}.json`, { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed to load ${slug}`);
  return await res.json();
}

function makeLineChart(ctx, years, hawaii, other, title, unit) {
  // Build a Chart.js line chart with two series
  return new Chart(ctx, {
    type: 'line',
    data: {
      labels: years,
      datasets: [
        { label: 'Hawaiʻi', data: hawaii, borderWidth: 2, tension: 0.2 },
        { label: 'Other U.S. States (avg)', data: other, borderWidth: 2, tension: 0.2 }
      ]
    },
    options: {
      responsive: true,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        title: { display: true, text: title },
        legend: { position: 'bottom' }
      },
      scales: {
        y: { title: { display: !!unit, text: unit || '' }, ticks: { callback: (v) => v } },
        x: { ticks: { autoSkip: true, maxTicksLimit: 10 } }
      }
    }
  });
}

function setMeta(el, payload) {
  const src = payload.source || {};
  const notes = (payload.notes || []).map(n => `<li>${n}</li>`).join('');
  const srcLine = src.human_url ? `<a href="${src.human_url}" target="_blank" rel="noopener">Source</a>` : 'Source: federal API';
  el.innerHTML = `
    <div>${srcLine} · Updated: ${new Date(payload.last_updated_utc).toLocaleString()}</div>
    ${notes ? `<ul>${notes}</ul>` : ''}
  `;
}

(async () => {
  try {
    const bb = await loadMetric('broadband_adoption_households_share');
    makeLineChart(document.getElementById('chart-broadband').getContext('2d'),
      bb.years, bb.hawaii, bb.other_states_avg, bb.title, bb.unit);
    setMeta(document.getElementById('meta-broadband'), bb);
  } catch (e) {
    document.getElementById('meta-broadband').textContent = 'Broadband data unavailable.';
    console.error(e);
  }

  try {
    const rn = await loadMetric('electricity_renewables_generation_share');
    makeLineChart(document.getElementById('chart-renewables').getContext('2d'),
      rn.years, rn.hawaii, rn.other_states_avg, rn.title, rn.unit);
    setMeta(document.getElementById('meta-renewables'), rn);
  } catch (e) {
    document.getElementById('meta-renewables').textContent = 'Renewables data unavailable.';
    console.error(e);
  }
})();
