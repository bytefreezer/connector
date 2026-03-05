package main

const interactiveHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ByteFreezer Connector</title>
<style>
* { box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 20px; }
.container { max-width: 1000px; margin: 0 auto; }
h1 { color: #3b82f6; margin-bottom: 8px; font-size: 1.5rem; }
.subtitle { color: #94a3b8; margin-bottom: 24px; font-size: 14px; }
.panel { background: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 16px; }
.panel h3 { margin: 0 0 12px 0; color: #e2e8f0; font-size: 14px; text-transform: uppercase; letter-spacing: 1px; }
label { display: block; margin-bottom: 6px; color: #94a3b8; font-size: 12px; text-transform: uppercase; letter-spacing: 1px; }
select, input[type="text"], textarea {
  width: 100%; padding: 10px 12px; font-size: 14px;
  background: #0f172a; border: 1px solid #334155; color: #e2e8f0;
  border-radius: 6px; outline: none; margin-bottom: 12px;
}
select:focus, input:focus, textarea:focus { border-color: #3b82f6; }
textarea { font-family: 'Monaco', 'Menlo', monospace; min-height: 80px; resize: vertical; }
.btn { padding: 10px 20px; background: #2563eb; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 500; margin-right: 8px; }
.btn:hover { background: #3b82f6; }
.btn:disabled { background: #334155; cursor: not-allowed; }
.btn-green { background: #059669; }
.btn-green:hover { background: #10b981; }
.btn-secondary { background: #334155; }
.btn-secondary:hover { background: #475569; }
.results-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px; }
.results-table th, .results-table td { padding: 8px 12px; border: 1px solid #334155; text-align: left; max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.results-table th { background: #334155; font-weight: 600; position: sticky; top: 0; }
.results-table tr:hover { background: #1e293b; }
.results-table td:hover { white-space: normal; word-break: break-all; }
.error { color: #ff6b6b; padding: 12px; background: #2d1f1f; border-radius: 6px; margin-top: 12px; border: 1px solid #5c2a2a; }
.stats { color: #94a3b8; font-size: 13px; margin: 8px 0; }
.stats span { margin-right: 16px; }
.results-container { max-height: 400px; overflow: auto; border-radius: 6px; }
.row { display: flex; gap: 12px; }
.row > * { flex: 1; }
.step-badge { display: inline-block; background: #334155; color: #94a3b8; padding: 2px 8px; border-radius: 12px; font-size: 11px; margin-left: 8px; }
.step-badge.active { background: #2563eb; color: white; }
.spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid #fff; border-top-color: transparent; border-radius: 50%; animation: spin 0.8s linear infinite; vertical-align: middle; margin-right: 6px; }
@keyframes spin { to { transform: rotate(360deg); } }
.success { color: #10b981; padding: 12px; background: #1a2d1f; border-radius: 6px; margin-top: 12px; border: 1px solid #2a5c3a; }
</style>
</head>
<body>
<div class="container">
  <h1>ByteFreezer Connector</h1>
  <p class="subtitle">Explore data, craft queries, test destinations, export subsets</p>

  <!-- Step 1: Select Dataset -->
  <div class="panel">
    <h3>1. Select Dataset <span class="step-badge active" id="step1badge">select</span></h3>
    <select id="datasetSelect" onchange="onDatasetChange()">
      <option value="">Loading datasets...</option>
    </select>
  </div>

  <!-- Step 2: Write & Preview Query -->
  <div class="panel">
    <h3>2. Write Query <span class="step-badge" id="step2badge">query</span></h3>
    <label>SQL (use PARQUET_PATH as placeholder)</label>
    <textarea id="sqlInput" placeholder="SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true) LIMIT 100"></textarea>
    <button class="btn" id="previewBtn" onclick="previewQuery()" disabled>Preview</button>
    <div id="queryError" class="error" style="display:none;"></div>
    <div id="queryStats" class="stats"></div>
    <div class="results-container" id="resultsContainer">
      <div id="results"></div>
    </div>
  </div>

  <!-- Step 3: Configure & Test Destination -->
  <div class="panel">
    <h3>3. Destination <span class="step-badge" id="step3badge">export</span></h3>
    <div class="row">
      <div>
        <label>Type</label>
        <select id="destType" onchange="onDestChange()">
          <option value="stdout">stdout (JSON lines)</option>
          <option value="elasticsearch">Elasticsearch</option>
          <option value="webhook">Webhook (HTTP POST)</option>
        </select>
      </div>
    </div>
    <div id="destConfig"></div>
    <div style="margin-top: 8px;">
      <button class="btn btn-secondary" onclick="testDestination()">Test Destination</button>
      <button class="btn btn-green" id="exportBtn" onclick="runExport()">Export</button>
    </div>
    <div id="destResult" style="margin-top: 12px;"></div>
  </div>
</div>

<script>
let selectedTenantId = '';
let selectedDatasetId = '';

const destFields = {
  stdout: [],
  elasticsearch: [
    {key: 'url', label: 'Elasticsearch URL', placeholder: 'http://localhost:9200'},
    {key: 'index', label: 'Index Name', placeholder: 'bytefreezer-logs'},
    {key: 'username', label: 'Username (optional)', placeholder: ''},
    {key: 'password', label: 'Password (optional)', placeholder: ''}
  ],
  webhook: [
    {key: 'url', label: 'Webhook URL', placeholder: 'https://example.com/webhook'}
  ]
};

async function loadDatasets() {
  try {
    const res = await fetch('/api/v1/datasets');
    const data = await res.json();
    const select = document.getElementById('datasetSelect');
    if (data.error) { select.innerHTML = '<option>Error: ' + esc(data.error) + '</option>'; return; }
    if (!data.datasets || data.datasets.length === 0) { select.innerHTML = '<option>No datasets found</option>'; return; }
    select.innerHTML = '<option value="">Select a dataset...</option>' +
      data.datasets.map(d => '<option value="' + d.dataset_id + '" data-tenant="' + d.tenant_id + '">' +
        d.name + ' (' + d.tenant_name + ')</option>').join('');
  } catch (e) {
    document.getElementById('datasetSelect').innerHTML = '<option>Failed to load: ' + esc(e.message) + '</option>';
  }
}

function onDatasetChange() {
  const select = document.getElementById('datasetSelect');
  selectedDatasetId = select.value;
  const opt = select.options[select.selectedIndex];
  selectedTenantId = opt ? opt.dataset.tenant : '';
  document.getElementById('previewBtn').disabled = !selectedDatasetId;
  document.getElementById('step1badge').className = selectedDatasetId ? 'step-badge active' : 'step-badge';
  document.getElementById('results').innerHTML = '';
  document.getElementById('queryStats').innerHTML = '';
}

async function previewQuery() {
  if (!selectedDatasetId || !selectedTenantId) return;
  const sql = document.getElementById('sqlInput').value.trim();
  if (!sql) return;

  document.getElementById('previewBtn').innerHTML = '<span class="spinner"></span>Running...';
  document.getElementById('previewBtn').disabled = true;
  document.getElementById('queryError').style.display = 'none';

  try {
    const res = await fetch('/api/v1/query', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({tenant_id: selectedTenantId, dataset_id: selectedDatasetId, sql: sql})
    });
    const data = await res.json();
    if (data.error) {
      document.getElementById('queryError').textContent = data.error;
      document.getElementById('queryError').style.display = 'block';
    } else {
      renderResults(data);
      document.getElementById('step2badge').className = 'step-badge active';
    }
    document.getElementById('queryStats').innerHTML = '<span>' + (data.row_count || 0) + ' rows</span><span>' + (data.execution_time_ms || 0) + ' ms</span>';
  } catch (e) {
    document.getElementById('queryError').textContent = e.message;
    document.getElementById('queryError').style.display = 'block';
  }
  document.getElementById('previewBtn').innerHTML = 'Preview';
  document.getElementById('previewBtn').disabled = false;
}

function renderResults(data) {
  if (!data.rows || data.rows.length === 0) {
    document.getElementById('results').innerHTML = '<div style="color:#94a3b8;padding:16px;text-align:center;">No results</div>';
    return;
  }
  const cols = data.columns || Object.keys(data.rows[0]);
  let html = '<table class="results-table"><thead><tr>';
  cols.forEach(c => html += '<th>' + esc(c) + '</th>');
  html += '</tr></thead><tbody>';
  data.rows.forEach(row => {
    html += '<tr>';
    if (Array.isArray(row)) {
      row.forEach(cell => html += '<td>' + esc(String(cell ?? 'null')) + '</td>');
    } else {
      cols.forEach(c => html += '<td>' + esc(String(row[c] ?? 'null')) + '</td>');
    }
    html += '</tr>';
  });
  html += '</tbody></table>';
  document.getElementById('results').innerHTML = html;
}

function onDestChange() {
  const type = document.getElementById('destType').value;
  const fields = destFields[type] || [];
  document.getElementById('destConfig').innerHTML = fields.map(f =>
    '<label>' + f.label + '</label><input type="text" id="dest_' + f.key + '" placeholder="' + f.placeholder + '">'
  ).join('');
}

function getDestConfig() {
  const type = document.getElementById('destType').value;
  const fields = destFields[type] || [];
  const config = {};
  fields.forEach(f => {
    const el = document.getElementById('dest_' + f.key);
    if (el && el.value) config[f.key] = el.value;
  });
  return config;
}

async function testDestination() {
  const type = document.getElementById('destType').value;
  const config = getDestConfig();
  const resultDiv = document.getElementById('destResult');

  try {
    const res = await fetch('/api/v1/test-destination', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({type: type, config: config})
    });
    const data = await res.json();
    if (data.error) {
      resultDiv.innerHTML = '<div class="error">' + esc(data.error) + '</div>';
    } else {
      resultDiv.innerHTML = '<div class="success">' + esc(data.message) + '</div>';
    }
  } catch (e) {
    resultDiv.innerHTML = '<div class="error">' + esc(e.message) + '</div>';
  }
}

async function runExport() {
  if (!selectedDatasetId || !selectedTenantId) { alert('Select a dataset first'); return; }
  const sql = document.getElementById('sqlInput').value.trim();
  if (!sql) { alert('Write a query first'); return; }

  const type = document.getElementById('destType').value;
  const config = getDestConfig();
  const resultDiv = document.getElementById('destResult');
  const btn = document.getElementById('exportBtn');

  btn.innerHTML = '<span class="spinner"></span>Exporting...';
  btn.disabled = true;

  try {
    const res = await fetch('/api/v1/export', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        tenant_id: selectedTenantId,
        dataset_id: selectedDatasetId,
        sql: sql,
        destination_type: type,
        destination_config: config
      })
    });
    const data = await res.json();
    if (data.error) {
      resultDiv.innerHTML = '<div class="error">' + esc(data.error) + '</div>';
    } else {
      resultDiv.innerHTML = '<div class="success">Export complete to ' + esc(type) + ' (' + data.execution_time_ms + 'ms)</div>';
      document.getElementById('step3badge').className = 'step-badge active';
    }
  } catch (e) {
    resultDiv.innerHTML = '<div class="error">' + esc(e.message) + '</div>';
  }
  btn.innerHTML = 'Export';
  btn.disabled = false;
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

// Ctrl+Enter to preview
document.getElementById('sqlInput').addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') previewQuery();
});

loadDatasets();
onDestChange();
</script>
</body>
</html>`
