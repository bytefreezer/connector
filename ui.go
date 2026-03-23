package main

const interactiveHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ByteFreezer Connector</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyBzdHJva2U9ImN1cnJlbnRDb2xvciIgZmlsbD0ibm9uZSIgc3Ryb2tlLXdpZHRoPSIyIiB2aWV3Qm94PSIwIDAgMjQgMjQiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIgY29sb3I9IiMyNTYzZWIiIGhlaWdodD0iMjQiIHdpZHRoPSIyNCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiBzdHlsZT0iY29sb3I6IHJnYigzNywgOTksIDIzNSk7Ij48cGF0aCBkPSJNMTAgNGwyIDFsMiAtMSI+PC9wYXRoPjxwYXRoIGQ9Ik0xMiAydjYuNWwzIDEuNzIiPjwvcGF0aD48cGF0aCBkPSJNMTcuOTI4IDYuMjY4bC4xMzQgMi4yMzJsMS44NjYgMS4yMzIiPjwvcGF0aD48cGF0aCBkPSJNMjAuNjYgN2wtNS42MjkgMy4yNWwuMDEgMy40NTgiPjwvcGF0aD48cGF0aCBkPSJNMTkuOTI4IDE0LjI2OGwtMS44NjYgMS4yMzJsLS4xMzQgMi4yMzIiPjwvcGF0aD48cGF0aCBkPSJNMjAuNjYgMTdsLTUuNjI5IC0zLjI1bC0yLjk5IDEuNzM4Ij48L3BhdGg+PHBhdGggZD0iTTE0IDIwbC0yIC0xbC0yIDEiPjwvcGF0aD48cGF0aCBkPSJNMTIgMjJ2LTYuNWwtMyAtMS43MiI+PC9wYXRoPjxwYXRoIGQ9Ik02LjA3MiAxNy43MzJsLS4xMzQgLTIuMjMybC0xLjg2NiAtMS4yMzIiPjwvcGF0aD48cGF0aCBkPSJNMy4zNCAxN2w1LjYyOSAtMy4yNWwtLjAxIC0zLjQ1OCI+PC9wYXRoPjxwYXRoIGQ9Ik00LjA3MiA5LjczMmwxLjg2NiAtMS4yMzJsLjEzNCAtMi4yMzIiPjwvcGF0aD48cGF0aCBkPSJNMy4zNCA3bDUuNjI5IDMuMjVsMi45OSAtMS43MzgiPjwvcGF0aD48L3N2Zz4K">
<style>
:root {
  --bg-primary: #0f172a;
  --bg-card: #1e293b;
  --bg-input: #0f172a;
  --bg-header: #334155;
  --bg-hover: rgba(59,130,246,0.08);
  --border: #334155;
  --border-focus: #3b82f6;
  --text-primary: #f1f5f9;
  --text-secondary: #94a3b8;
  --text-muted: #64748b;
  --accent: #3b82f6;
  --accent-hover: #2563eb;
  --success: #10b981;
  --success-bg: rgba(16,185,129,0.1);
  --error: #ef4444;
  --error-bg: rgba(239,68,68,0.1);
  --warning: #f59e0b;
  --warning-bg: rgba(245,158,11,0.1);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
  background: var(--bg-primary);
  color: var(--text-primary);
  line-height: 1.5;
}

/* Layout */
.app { display: flex; flex-direction: column; min-height: 100vh; }
.header {
  padding: 16px 24px;
  border-bottom: 1px solid var(--border);
  background: var(--bg-card);
}
.header h1 { font-size: 18px; font-weight: 600; }
.header p { font-size: 13px; color: var(--text-secondary); margin-top: 2px; }
.main { display: flex; flex: 1; min-height: 0; }
.content { flex: 1; padding: 24px; overflow-y: auto; max-width: 1200px; }
.sidebar {
  width: 320px; min-width: 320px;
  border-left: 1px solid var(--border);
  background: var(--bg-card);
  padding: 20px;
  overflow-y: auto;
  display: flex; flex-direction: column; gap: 16px;
}

/* Cards */
.card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 16px;
  margin-bottom: 16px;
}
.card-header {
  display: flex; align-items: center; gap: 8px;
  margin-bottom: 12px;
}
.card-header svg { width: 16px; height: 16px; color: var(--accent); flex-shrink: 0; }
.card-header h3 { font-size: 14px; font-weight: 600; }
.card-label {
  display: block; font-size: 11px; font-weight: 500;
  color: var(--text-muted); text-transform: uppercase;
  letter-spacing: 0.5px; margin-bottom: 6px;
}

/* Form elements */
select, input[type="text"], textarea {
  width: 100%; padding: 8px 12px; font-size: 13px;
  background: var(--bg-input); border: 1px solid var(--border);
  color: var(--text-primary); border-radius: 6px; outline: none;
  transition: border-color 0.15s;
}
select:focus, input:focus, textarea:focus { border-color: var(--border-focus); }
select:disabled, input:disabled, textarea:disabled {
  opacity: 0.5; cursor: not-allowed;
}
textarea {
  font-family: 'SF Mono', 'Monaco', 'Menlo', 'Consolas', monospace;
  font-size: 12px; resize: vertical; min-height: 100px;
  line-height: 1.6;
}

/* Buttons */
.btn {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 8px 16px; font-size: 13px; font-weight: 500;
  border: none; border-radius: 6px; cursor: pointer;
  transition: background 0.15s;
}
.btn-primary { background: var(--accent); color: white; }
.btn-primary:hover { background: var(--accent-hover); }
.btn-primary:disabled { background: var(--bg-header); color: var(--text-muted); cursor: not-allowed; }
.btn-success { background: var(--success); color: white; }
.btn-success:hover { background: #059669; }
.btn-secondary { background: var(--bg-header); color: var(--text-secondary); }
.btn-secondary:hover { background: #475569; color: var(--text-primary); }
.btn-group { display: flex; gap: 8px; margin-top: 12px; }

/* Stats bar */
.stats-bar {
  display: flex; align-items: center; gap: 16px;
  padding: 8px 0; font-size: 13px; color: var(--text-secondary);
}
.stats-bar .stat { display: flex; align-items: center; gap: 4px; }
.stats-bar svg { width: 14px; height: 14px; }

/* Results table — matches query page */
.results-wrap {
  border: 1px solid var(--border); border-radius: 8px;
  overflow: auto; max-height: 480px; margin-top: 12px;
}
.results-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.results-table thead { position: sticky; top: 0; z-index: 1; }
.results-table th {
  padding: 10px 14px; text-align: left; font-size: 11px;
  font-weight: 600; color: var(--text-secondary);
  background: var(--bg-header); white-space: nowrap;
  border-bottom: 1px solid var(--border);
  text-transform: uppercase; letter-spacing: 0.3px;
}
.results-table td {
  padding: 8px 14px; border-bottom: 1px solid var(--border);
  max-width: 280px; overflow: hidden; text-overflow: ellipsis;
  white-space: nowrap; font-size: 13px; color: var(--text-primary);
}
.results-table tr:hover td { background: var(--bg-hover); }
.results-table td:hover { white-space: normal; word-break: break-all; }
.results-table .null { color: var(--text-muted); font-style: italic; }
.results-empty {
  padding: 48px 16px; text-align: center; color: var(--text-muted);
  background: var(--bg-card);
}
.results-empty svg { width: 40px; height: 40px; margin: 0 auto 8px; opacity: 0.4; }

/* Alerts */
.alert {
  padding: 12px 14px; border-radius: 6px; font-size: 13px;
  display: flex; align-items: flex-start; gap: 8px;
  margin-top: 12px;
}
.alert svg { width: 16px; height: 16px; flex-shrink: 0; margin-top: 1px; }
.alert-error { background: var(--error-bg); border: 1px solid rgba(239,68,68,0.3); color: #fca5a5; }
.alert-success { background: var(--success-bg); border: 1px solid rgba(16,185,129,0.3); color: #6ee7b7; }
.alert-info { background: var(--warning-bg); border: 1px solid rgba(245,158,11,0.3); color: #fcd34d; }

/* Spinner */
.spinner {
  display: inline-block; width: 14px; height: 14px;
  border: 2px solid currentColor; border-top-color: transparent;
  border-radius: 50%; animation: spin 0.6s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* Example queries */
.examples { margin-top: 16px; }
.examples h4 {
  font-size: 11px; font-weight: 500; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;
}
.example-btn {
  display: block; width: 100%; text-align: left;
  padding: 8px 12px; margin-bottom: 6px;
  background: var(--bg-input); border: 1px solid var(--border);
  border-radius: 6px; color: var(--text-secondary);
  font-size: 12px; cursor: pointer; transition: all 0.15s;
}
.example-btn:hover { border-color: var(--accent); color: var(--text-primary); }

/* Sidebar sections */
.sidebar-section { }
.sidebar-section h4 {
  font-size: 11px; font-weight: 500; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: 0.5px;
  margin-bottom: 8px;
}
.sidebar-section select, .sidebar-section input { margin-bottom: 8px; }
.sidebar-divider { border-top: 1px solid var(--border); margin: 4px 0; }

/* Schema panel */
.schema-list { font-size: 12px; }
.schema-list .col {
  display: flex; justify-content: space-between; align-items: center;
  padding: 4px 0; border-bottom: 1px solid rgba(51,65,85,0.5);
}
.schema-list .col-name { color: var(--text-primary); font-family: 'SF Mono', monospace; font-size: 12px; }
.schema-list .col-type { color: var(--text-muted); font-size: 11px; }

/* Keyboard hint */
.kbd-hint { font-size: 11px; color: var(--text-muted); margin-top: 6px; }
kbd {
  background: var(--bg-header); border: 1px solid var(--border);
  border-radius: 3px; padding: 1px 5px; font-size: 10px;
  font-family: inherit;
}

/* Responsive */
@media (max-width: 900px) {
  .main { flex-direction: column; }
  .sidebar { width: 100%; min-width: 0; border-left: none; border-top: 1px solid var(--border); }
}
</style>
</head>
<body>
<div class="app">
  <div class="header">
    <h1>Query</h1>
    <p>Search and export your data with DuckDB</p>
  </div>

  <div class="main">
    <div class="content">
      <!-- Dataset selector -->
      <div style="margin-bottom: 16px;">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px;">
          <span class="card-label" style="margin:0;">Dataset</span>
          <span id="schemaToggle" style="font-size:12px;color:var(--accent);cursor:pointer;display:none;" onclick="toggleSchema()">Show Schema</span>
        </div>
        <select id="datasetSelect" onchange="onDatasetChange()">
          <option value="">Loading datasets...</option>
        </select>
      </div>

      <!-- Testing mode warning -->
      <div id="testingWarning" style="display:none; background:var(--warning-bg); border:1px solid var(--warning); border-radius:8px; padding:12px 16px; margin-bottom:8px;">
        <div style="display:flex; align-items:flex-start; gap:8px;">
          <svg viewBox="0 0 24 24" fill="none" stroke="var(--warning)" stroke-width="2" style="width:18px;height:18px;flex-shrink:0;margin-top:1px;"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
          <div style="font-size:13px;">
            <div style="font-weight:600; color:var(--warning); margin-bottom:2px;">Testing Mode</div>
            <div style="color:var(--text-secondary);">This dataset has testing enabled. Packer produces many small parquet files (~1 per 15 seconds) instead of large accumulated batches. Queries scanning many files will be slow. For production performance, disable testing mode in the control plane.</div>
          </div>
        </div>
      </div>

      <!-- Schema (collapsed by default) -->
      <div id="schemaPanel" class="card" style="display:none;">
        <div class="card-header">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>
          <h3>Schema <span id="schemaCount" style="color:var(--text-muted);font-weight:400;"></span></h3>
        </div>
        <div id="schemaList" class="schema-list"></div>
      </div>

      <!-- SQL editor -->
      <div>
        <span class="card-label">SQL Query</span>
        <textarea id="sqlInput" placeholder="Select a dataset to auto-fill the query, or write your own SQL"></textarea>
        <div class="kbd-hint"><kbd>Ctrl</kbd>+<kbd>Enter</kbd> to execute</div>
      </div>

      <!-- Action buttons -->
      <div class="btn-group">
        <button class="btn btn-primary" id="executeBtn" onclick="previewQuery()" disabled>
          <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21"/></svg>
          Execute Query
        </button>
      </div>

      <!-- Error -->
      <div id="queryError" class="alert alert-error" style="display:none;">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>
        <span id="queryErrorText"></span>
      </div>

      <!-- Stats bar -->
      <div id="queryStats" class="stats-bar" style="display:none;">
        <div class="stat">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="3" x2="9" y2="21"/></svg>
          <span id="rowCount">0 rows</span>
        </div>
        <div class="stat">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
          <span id="execTime">0ms</span>
        </div>
        <div class="stat">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
          <span id="fileCount"></span>
        </div>
      </div>

      <!-- Results table -->
      <div id="resultsWrap" class="results-wrap" style="display:none;">
        <div id="results"></div>
      </div>

      <!-- Example queries -->
      <div class="examples" id="examplesSection">
        <h4>Example queries</h4>
        <button class="example-btn" onclick="setQuery(this.textContent)">SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true) LIMIT 100</button>
        <button class="example-btn" onclick="setQuery(this.textContent)">SELECT COUNT(*) as total FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true)</button>
        <button class="example-btn" onclick="setQuery(this.textContent)">SELECT year, month, day, hour, COUNT(*) as count FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true) GROUP BY year, month, day, hour ORDER BY year, month, day, hour</button>
        <button class="example-btn" onclick="setQuery(this.textContent)">SELECT DISTINCT source_ip, COUNT(*) as hits FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true) GROUP BY source_ip ORDER BY hits DESC LIMIT 20</button>
      </div>
    </div>

    <!-- Sidebar: Export destination -->
    <div class="sidebar">
      <div class="sidebar-section">
        <h4>Export Destination</h4>
        <select id="destType" onchange="onDestChange()">
          <option value="stdout">stdout (JSON lines)</option>
          <option value="elasticsearch">Elasticsearch</option>
          <option value="webhook">Webhook (HTTP POST)</option>
        </select>
        <div id="destConfig"></div>
      </div>

      <div class="sidebar-divider"></div>

      <div style="display:flex;gap:8px;">
        <button class="btn btn-secondary" style="flex:1;" onclick="testDestination()">Test</button>
        <button class="btn btn-success" style="flex:1;" id="exportBtn" onclick="runExport()">Export</button>
      </div>

      <div id="destResult"></div>

      <div class="sidebar-divider"></div>

      <div class="sidebar-section">
        <h4>How It Works</h4>
        <div style="font-size:12px;color:var(--text-secondary);line-height:1.7;">
          <p>This connector queries your parquet files using DuckDB over S3 and exports results to your destination.</p>
          <p style="margin-top:8px;">Select a dataset and the S3 parquet path is resolved automatically. Edit the SQL to filter or aggregate as needed.</p>
          <p style="margin-top:8px;">Use <strong style="color:var(--text-primary);">batch</strong> mode for one-off exports or <strong style="color:var(--text-primary);">watch</strong> mode for continuous delivery.</p>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
let selectedTenantId = '';
let selectedDatasetId = '';
let selectedParquetPath = '';
let schemaVisible = false;

const defaultSQL = (path) => "SELECT * FROM read_parquet('" + path + "', hive_partitioning=true, union_by_name=true) LIMIT 100";

const destFields = {
  stdout: [],
  elasticsearch: [
    {key: 'url', label: 'URL', placeholder: 'http://localhost:9200'},
    {key: 'index', label: 'Index', placeholder: 'bytefreezer-logs'},
    {key: 'username', label: 'Username', placeholder: ''},
    {key: 'password', label: 'Password', placeholder: ''}
  ],
  webhook: [
    {key: 'url', label: 'URL', placeholder: 'https://example.com/webhook'}
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
      data.datasets.map(d => '<option value="' + d.dataset_id + '" data-tenant="' + d.tenant_id + '" data-name="' + esc(d.name) + '" data-path="' + esc(d.parquet_path || '') + '" data-testing="' + (d.testing ? '1' : '') + '">' +
        esc(d.name) + (d.testing ? ' [testing]' : '') + ' (' + esc(d.tenant_name) + ')</option>').join('');
  } catch (e) {
    document.getElementById('datasetSelect').innerHTML = '<option>Failed to load: ' + esc(e.message) + '</option>';
  }
}

function onDatasetChange() {
  const select = document.getElementById('datasetSelect');
  selectedDatasetId = select.value;
  const opt = select.options[select.selectedIndex];
  selectedTenantId = opt ? opt.dataset.tenant : '';
  selectedParquetPath = opt ? (opt.dataset.path || '') : '';
  const isTesting = opt ? opt.dataset.testing === '1' : false;
  document.getElementById('testingWarning').style.display = isTesting ? 'block' : 'none';
  document.getElementById('executeBtn').disabled = !selectedDatasetId;
  document.getElementById('results').innerHTML = '';
  document.getElementById('resultsWrap').style.display = 'none';
  document.getElementById('queryStats').style.display = 'none';
  document.getElementById('queryError').style.display = 'none';
  document.getElementById('schemaToggle').style.display = selectedDatasetId ? 'inline' : 'none';
  if (selectedDatasetId && selectedTenantId) {
    // Resolve optimal partition-scoped path from server
    fetch('/api/v1/parquet-path?tenant_id=' + encodeURIComponent(selectedTenantId) + '&dataset_id=' + encodeURIComponent(selectedDatasetId))
      .then(r => r.json())
      .then(data => {
        if (data.parquet_path) {
          selectedParquetPath = data.parquet_path;
        }
        document.getElementById('sqlInput').value = defaultSQL(selectedParquetPath);
        updateExampleQueries(selectedParquetPath);
      })
      .catch(() => {
        document.getElementById('sqlInput').value = defaultSQL(selectedParquetPath);
        updateExampleQueries(selectedParquetPath);
      });
    loadSchema();
  }
}

function updateExampleQueries(path) {
  const section = document.getElementById('examplesSection');
  section.innerHTML = '<h4>Example queries</h4>' +
    '<button class="example-btn" onclick="setQuery(this.textContent)">SELECT * FROM read_parquet(\'' + path + '\', hive_partitioning=true, union_by_name=true) LIMIT 100</button>' +
    '<button class="example-btn" onclick="setQuery(this.textContent)">SELECT COUNT(*) as total FROM read_parquet(\'' + path + '\', hive_partitioning=true, union_by_name=true)</button>' +
    '<button class="example-btn" onclick="setQuery(this.textContent)">SELECT year, month, day, hour, COUNT(*) as count FROM read_parquet(\'' + path + '\', hive_partitioning=true, union_by_name=true) GROUP BY year, month, day, hour ORDER BY year, month, day, hour</button>' +
    '<button class="example-btn" onclick="setQuery(this.textContent)">SELECT DISTINCT source_ip, COUNT(*) as hits FROM read_parquet(\'' + path + '\', hive_partitioning=true, union_by_name=true) GROUP BY source_ip ORDER BY hits DESC LIMIT 20</button>';
}

async function loadSchema() {
  const panel = document.getElementById('schemaPanel');
  const list = document.getElementById('schemaList');
  const count = document.getElementById('schemaCount');
  list.innerHTML = '<div style="color:var(--text-muted);font-size:12px;">Loading...</div>';

  try {
    const res = await fetch('/api/v1/query', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        tenant_id: selectedTenantId,
        dataset_id: selectedDatasetId,
        sql: "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('PARQUET_PATH', hive_partitioning=true, union_by_name=true) LIMIT 1)"
      })
    });
    const data = await res.json();
    if (data.error) {
      list.innerHTML = '<div style="color:var(--text-muted);font-size:12px;">' + esc(data.error) + '</div>';
      count.textContent = '';
      return;
    }
    if (!data.rows || data.rows.length === 0) {
      list.innerHTML = '<div style="color:var(--text-muted);font-size:12px;">No schema available</div>';
      count.textContent = '';
      return;
    }
    const cols = data.columns || [];
    const nameIdx = cols.indexOf('column_name');
    const typeIdx = cols.indexOf('column_type');
    list.innerHTML = data.rows.map(r => {
      const name = Array.isArray(r) ? r[nameIdx >= 0 ? nameIdx : 0] : r.column_name;
      const type = Array.isArray(r) ? r[typeIdx >= 0 ? typeIdx : 1] : r.column_type;
      return '<div class="col"><span class="col-name">' + esc(String(name)) + '</span><span class="col-type">' + esc(String(type)) + '</span></div>';
    }).join('');
    count.textContent = '(' + data.rows.length + ')';
  } catch (e) {
    list.innerHTML = '<div style="color:var(--text-muted);font-size:12px;">' + esc(e.message) + '</div>';
    count.textContent = '';
  }
}

function toggleSchema() {
  schemaVisible = !schemaVisible;
  document.getElementById('schemaPanel').style.display = schemaVisible ? 'block' : 'none';
  document.getElementById('schemaToggle').textContent = schemaVisible ? 'Hide Schema' : 'Show Schema';
}

async function previewQuery() {
  if (!selectedDatasetId || !selectedTenantId) return;
  const sql = document.getElementById('sqlInput').value.trim();
  if (!sql) return;

  const btn = document.getElementById('executeBtn');
  btn.innerHTML = '<span class="spinner"></span> Executing...';
  btn.disabled = true;
  document.getElementById('queryError').style.display = 'none';
  document.getElementById('queryStats').style.display = 'none';

  try {
    const res = await fetch('/api/v1/query', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({tenant_id: selectedTenantId, dataset_id: selectedDatasetId, sql: sql})
    });
    const data = await res.json();
    if (data.error) {
      document.getElementById('queryErrorText').textContent = data.error;
      document.getElementById('queryError').style.display = 'flex';
      document.getElementById('resultsWrap').style.display = 'none';
    } else {
      renderResults(data);
    }
    document.getElementById('rowCount').textContent = (data.row_count || 0) + ' rows';
    document.getElementById('execTime').textContent = (data.execution_time_ms || 0) + 'ms';
    document.getElementById('fileCount').textContent = data.files_scanned ? data.files_scanned + ' files' : '';
    document.getElementById('queryStats').style.display = 'flex';
  } catch (e) {
    document.getElementById('queryErrorText').textContent = e.message;
    document.getElementById('queryError').style.display = 'flex';
  }
  btn.innerHTML = '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21"/></svg> Execute Query';
  btn.disabled = false;
}

function renderResults(data) {
  const wrap = document.getElementById('resultsWrap');
  const container = document.getElementById('results');
  if (!data.rows || data.rows.length === 0) {
    container.innerHTML = '<div class="results-empty"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg><div>No results found</div></div>';
    wrap.style.display = 'block';
    return;
  }
  const cols = data.columns || Object.keys(data.rows[0]);
  let html = '<table class="results-table"><thead><tr>';
  cols.forEach(c => html += '<th>' + esc(c) + '</th>');
  html += '</tr></thead><tbody>';
  data.rows.forEach(row => {
    html += '<tr>';
    if (Array.isArray(row)) {
      row.forEach(cell => {
        if (cell === null || cell === undefined) html += '<td class="null">null</td>';
        else html += '<td title="' + esc(String(cell)) + '">' + esc(String(cell)) + '</td>';
      });
    } else {
      cols.forEach(c => {
        const cell = row[c];
        if (cell === null || cell === undefined) html += '<td class="null">null</td>';
        else html += '<td title="' + esc(String(cell)) + '">' + esc(String(cell)) + '</td>';
      });
    }
    html += '</tr>';
  });
  html += '</tbody></table>';
  container.innerHTML = html;
  wrap.style.display = 'block';
}

function setQuery(sql) {
  document.getElementById('sqlInput').value = sql;
}

function onDestChange() {
  const type = document.getElementById('destType').value;
  const fields = destFields[type] || [];
  document.getElementById('destConfig').innerHTML = fields.map(f =>
    '<span class="card-label">' + f.label + '</span><input type="text" id="dest_' + f.key + '" placeholder="' + f.placeholder + '">'
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
      resultDiv.innerHTML = '<div class="alert alert-error"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg><span>' + esc(data.error) + '</span></div>';
    } else {
      resultDiv.innerHTML = '<div class="alert alert-success"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg><span>' + esc(data.message) + '</span></div>';
    }
  } catch (e) {
    resultDiv.innerHTML = '<div class="alert alert-error"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg><span>' + esc(e.message) + '</span></div>';
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

  btn.innerHTML = '<span class="spinner"></span> Exporting...';
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
      resultDiv.innerHTML = '<div class="alert alert-error"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg><span>' + esc(data.error) + '</span></div>';
    } else {
      resultDiv.innerHTML = '<div class="alert alert-success"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg><span>Exported ' + (data.row_count || 0) + ' rows to ' + esc(type) + ' in ' + (data.execution_time_ms || 0) + 'ms</span></div>';
    }
  } catch (e) {
    resultDiv.innerHTML = '<div class="alert alert-error"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg><span>' + esc(e.message) + '</span></div>';
  }
  btn.innerHTML = 'Export';
  btn.disabled = false;
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

document.getElementById('sqlInput').addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') previewQuery();
});

loadDatasets();
onDestChange();
</script>
</body>
</html>` + "\n"
