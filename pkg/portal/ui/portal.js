(() => {
  const pageSize = 25;
  const state = { workflows: [], runs: [], total: 0, metrics: {}, offset: 0, selectedWorkflow: '', selectedRun: '', workflowSignature: '', runSignature: '', refreshing: false };
  const byId = (id) => document.getElementById(id);
  const tokenInput = byId('api-token');
  const tokenField = byId('token-field');
  const signOutButton = byId('sign-out-button');
  let oidcMode = false;

  function escapeHTML(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[character]);
  }

  function tokenHeaders() {
    const token = tokenInput.value.trim();
    return token ? { Authorization: `Bearer ${token}` } : {};
  }

  async function api(path, options = {}) {
    const response = await fetch(path, {
      ...options,
      headers: { ...tokenHeaders(), ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...(options.headers || {}) }
    });
    const contentType = response.headers.get('content-type') || '';
    const body = contentType.includes('application/json') ? await response.json() : await response.text();
    if (response.status === 401 && oidcMode) {
      window.location.assign('/auth/login');
      throw new Error('Your sign-in session expired');
    }
    if (!response.ok) throw new Error(body?.error || `Request failed (${response.status})`);
    return body;
  }

  async function configureAuthentication() {
    const response = await fetch('/auth/session', { credentials: 'same-origin' });
    if (response.ok && (response.headers.get('content-type') || '').includes('application/json')) {
      const session = await response.json();
      if (session.authenticated === true) {
        oidcMode = true;
        tokenField.hidden = true;
        signOutButton.hidden = false;
        return true;
      }
    }
    if (response.status === 404 || response.ok) {
      tokenInput.value = sessionStorage.getItem('zephyr-token') || '';
      return true;
    }
    if (response.status === 401) {
      window.location.assign('/auth/login');
      return false;
    }
    throw new Error(`Could not check sign-in status (${response.status})`);
  }

  function showToast(message, isError = false) {
    const toast = byId('toast');
    toast.textContent = message;
    toast.classList.toggle('error', isError);
    toast.hidden = false;
    clearTimeout(showToast.timer);
    showToast.timer = setTimeout(() => { toast.hidden = true; }, 3600);
  }

  function statusPill(status) {
    const normalized = String(status || 'PENDING').toLowerCase();
    const label = normalized === 'running' ? 'IN PROGRESS' : normalized.replaceAll('_', ' ').toUpperCase();
    return `<span class="status-pill status-${escapeHTML(normalized)}">${escapeHTML(label)}</span>`;
  }

  function shortID(id) {
    const value = String(id || '');
    return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value;
  }

  function timeAgo(value) {
    if (!value) return '—';
    const timestamp = new Date(value).getTime();
    if (!Number.isFinite(timestamp)) return '—';
    const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
  }

  function dateTime(value) {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  async function refresh() {
    if (state.refreshing) return;
    state.refreshing = true;
    byId('refresh-button').classList.add('is-loading');
    try {
      const workflowData = await api('/v1/workflows');
      const workflows = Array.isArray(workflowData) ? workflowData : [];
      const workflowSignature = JSON.stringify(workflows);
      if (workflowSignature !== state.workflowSignature) {
        state.workflows = workflows;
        state.workflowSignature = workflowSignature;
        renderWorkflows();
      }
      const [metrics] = await Promise.all([api('/v1/metrics'), loadRuns()]);
      state.metrics = metrics;
      const runSignature = JSON.stringify({ runs: state.runs, total: state.total, offset: state.offset });
      if (runSignature !== state.runSignature) {
        state.runSignature = runSignature;
        renderRuns();
      }
      updateMetrics();
      if (state.selectedRun) await loadDetail(state.selectedRun, true);
      byId('connection-endpoint').textContent = window.location.host;
      byId('updated-at').textContent = `Updated ${new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}`;
    } catch (error) {
      byId('connection-endpoint').textContent = 'API unavailable';
      byId('updated-at').textContent = 'Connection error';
      showToast(error.message || 'Could not load workflow data', true);
    } finally {
      state.refreshing = false;
      byId('refresh-button').classList.remove('is-loading');
    }
  }

  async function loadRuns() {
    const query = new URLSearchParams({ limit: String(pageSize), offset: String(state.offset) });
    const status = byId('status-filter').value;
    if (status) query.set('status', status);
    if (state.selectedWorkflow) query.set('workflow_name', state.selectedWorkflow);
    const page = await api(`/v1/instances?${query}`);
    state.runs = page.items || [];
    state.total = page.total || 0;
  }

  function renderWorkflows() {
    const filterButton = byId('clear-workflow-filter');
    filterButton.hidden = !state.selectedWorkflow;
    byId('workflow-count').textContent = state.workflows.length;
    const rows = byId('workflow-rows');
    if (!state.workflows.length) {
      rows.innerHTML = '<tr><td colspan="5" class="table-empty">No workflow definitions registered</td></tr>';
      return;
    }
    rows.innerHTML = state.workflows.map((workflow) => {
      const selected = state.selectedWorkflow === workflow.name ? ' selected-workflow' : '';
      return `<tr class="workflow-row${selected}" data-workflow="${escapeHTML(workflow.name)}" tabindex="0">
        <td><button class="workflow-name" data-workflow="${escapeHTML(workflow.name)}">${escapeHTML(workflow.name)}</button><span class="workflow-sub">${workflow.versions?.length || 0} definition${workflow.versions?.length === 1 ? '' : 's'}</span></td>
        <td><span class="version-tag">v${escapeHTML(workflow.latest_version)}</span></td>
        <td><span class="step-count">${escapeHTML(workflow.task_count)} tasks</span></td>
        <td><button class="row-action" data-start-workflow="${escapeHTML(workflow.name)}" aria-label="Start ${escapeHTML(workflow.name)}">＋</button></td>
      </tr>`;
    }).join('');
  }

  function renderRuns() {
    const search = byId('run-search').value.trim().toLowerCase();
    const runs = state.runs.filter((run) => !search || `${run.id} ${run.workflow_name}`.toLowerCase().includes(search));
    byId('run-count').textContent = state.total;
    byId('nav-run-count').textContent = state.total;
    const rows = byId('run-rows');
    if (!runs.length) {
      const message = state.selectedWorkflow ? 'No runs for this workflow yet' : 'No runs match this view';
      rows.innerHTML = `<tr><td colspan="5" class="table-empty">${message}</td></tr>`;
    } else {
      rows.innerHTML = runs.map((run) => {
        const progress = run.task_count ? Math.round((run.completed_tasks / run.task_count) * 100) : 0;
        const selected = run.id === state.selectedRun ? ' selected-run' : '';
        return `<tr class="run-row${selected}" data-run-id="${escapeHTML(run.id)}" tabindex="0">
          <td><button class="run-id" data-run-id="${escapeHTML(run.id)}" aria-label="View details for run ${escapeHTML(run.id)}" title="View run details">${escapeHTML(shortID(run.id))}<span aria-hidden="true">↗</span></button><span class="workflow-sub">v${escapeHTML(run.version)}</span></td>
          <td>${escapeHTML(run.workflow_name)}</td>
          <td>${statusPill(run.status)}</td>
          <td><span class="task-progress"><span>${escapeHTML(run.completed_tasks)}/${escapeHTML(run.task_count)}</span><span class="progress-track"><span class="progress-fill" style="display:block;width:${progress}%"></span></span></span></td>
          <td><span class="run-time">${escapeHTML(timeAgo(run.updated_at))}</span></td>
        </tr>`;
      }).join('');
    }
    const start = state.total ? state.offset + 1 : 0;
    const end = Math.min(state.offset + pageSize, state.total);
    byId('run-pagination-label').textContent = `Showing ${start}–${end} of ${state.total} runs`;
    byId('previous-page').disabled = state.offset === 0;
    byId('next-page').disabled = state.offset + pageSize >= state.total;
  }

  function updateMetrics() {
    byId('metric-total').textContent = state.metrics.total ?? 0;
    byId('metric-running').textContent = (state.metrics.running || 0) + (state.metrics.compensating || 0) + (state.metrics.pending || 0);
    byId('metric-completed').textContent = (state.metrics.completed || 0) + (state.metrics.compensated || 0);
    byId('metric-failed').textContent = state.metrics.failed ?? 0;
  }

  async function loadDetail(id, quiet = false) {
    state.selectedRun = id;
    try {
      const detail = await api(`/v1/instances/${encodeURIComponent(id)}`);
      byId('detail-placeholder').hidden = true;
      const root = byId('run-detail');
      root.hidden = false;
      root.innerHTML = `<div class="detail-content">
        <div class="detail-top"><div><div class="section-kicker">RUN INSPECTOR · v${escapeHTML(detail.version)}</div><div class="detail-id">${escapeHTML(detail.id)}</div></div>${statusPill(detail.status)}</div>
        ${detail.failure_reason ? `<div class="workflow-failure"><span>FAILURE REASON</span><p>${escapeHTML(detail.failure_reason)}</p></div>` : ''}
        <div class="detail-meta"><div><span>WORKFLOW</span><strong>${escapeHTML(detail.workflow_name)}</strong></div><div><span>STARTED</span><strong>${escapeHTML(dateTime(detail.started_at))}</strong></div><div><span>TASKS</span><strong>${detail.tasks.length} total</strong></div><div><span>SEQUENCE</span><strong>#${escapeHTML(detail.next_sequence)}</strong></div></div>
        <section class="detail-section"><div class="detail-section-heading"><h3>Task state</h3><span>${detail.tasks.length} nodes</span></div><div class="task-list">${detail.tasks.length ? detail.tasks.map((task) => `<article class="task-item"><div class="task-item-top"><span class="task-name">${escapeHTML(task.task_name || task.node_id)}${task.is_compensation ? ' · COMPENSATION' : ''}</span>${statusPill(task.status)}</div><div class="task-node">${escapeHTML(task.node_id)}${task.lease_token ? ` · fence ${escapeHTML(task.lease_token)}` : ''}${task.original_node_id ? ` · for ${escapeHTML(task.original_node_id)}` : ''}</div>${task.error ? `<div class="task-error">${escapeHTML(task.error)}</div>` : ''}<details class="task-payload"><summary>Task payload</summary><div class="payload-label">INPUT</div><pre class="json-view">${escapeHTML(JSON.stringify(task.input || {}, null, 2))}</pre>${task.result ? `<div class="payload-label">RESULT</div><pre class="json-view">${escapeHTML(JSON.stringify(task.result, null, 2))}</pre>` : ''}</details></article>`).join('') : '<div class="table-empty">No task nodes scheduled</div>'}</div></section>
        <section class="detail-section"><div class="detail-section-heading"><h3>Event history</h3><span>${detail.events.length} events</span></div><div class="event-list">${detail.events.slice().reverse().slice(0, 12).map((event) => `<div class="event-item"><div class="event-type">${escapeHTML(event.type || event.Type)} <span>#${escapeHTML(event.sequence || event.Sequence)}</span></div><div class="event-time">${escapeHTML(dateTime(event.occurred_at || event.OccurredAt))}</div></div>`).join('') || '<div class="table-empty">No events recorded</div>'}</div></section>
        <section class="detail-section"><div class="detail-section-heading"><h3>Input context</h3></div><pre class="json-view">${escapeHTML(JSON.stringify(detail.context || {}, null, 2))}</pre></section>
      </div>`;
      if (!quiet) renderRuns();
    } catch (error) {
      if (!quiet) showToast(error.message || 'Could not load run details', true);
    }
  }

  async function loadWorkflowDefinition(name) {
    try {
      const workflow = await api(`/v1/workflows/${encodeURIComponent(name)}`);
      const nodes = workflow.nodes || workflow.Nodes || {};
      const starts = workflow.start || workflow.Start || [];
      const versions = state.workflows.find((item) => item.name === name)?.versions || [];
      byId('detail-placeholder').hidden = true;
      const root = byId('run-detail');
      root.hidden = false;
      const nodeRows = Object.entries(nodes).sort(([left], [right]) => left.localeCompare(right)).map(([nodeID, node]) => {
        const task = node.task || node.Task;
        const taskName = task?.name || task?.Name || '';
        const dependencies = node.depends_on || node.DependsOn || [];
        const nodeType = node.type || node.Type || 'NODE';
        return `<article class="task-item"><div class="task-item-top"><span class="task-name">${escapeHTML(taskName || nodeID)}</span><span class="node-type">${escapeHTML(nodeType)}</span></div><div class="task-node">${escapeHTML(nodeID)}</div>${dependencies.length ? `<div class="dependency-list">AFTER ${dependencies.map(escapeHTML).join(' · ')}</div>` : ''}</article>`;
      }).join('');
      root.innerHTML = `<div class="detail-content">
        <div class="detail-top"><div><div class="section-kicker">WORKFLOW DEFINITION</div><div class="detail-id definition-name">${escapeHTML(name)}</div></div><span class="version-tag">v${escapeHTML(workflow.version || workflow.Version)}</span></div>
        <div class="detail-meta"><div><span>VERSIONS</span><strong>${versions.map((version) => `v${version}`).join(' · ')}</strong></div><div><span>START NODES</span><strong>${starts.map(escapeHTML).join(', ') || '—'}</strong></div><div><span>GRAPH NODES</span><strong>${Object.keys(nodes).length}</strong></div><div><span>RECENT RUNS</span><strong>${state.total}</strong></div></div>
        <section class="detail-section"><div class="detail-section-heading"><h3>Execution graph</h3><span>${Object.keys(nodes).length} nodes</span></div><div class="task-list">${nodeRows || '<div class="table-empty">No graph nodes</div>'}</div></section>
        <section class="detail-section"><details class="definition-json"><summary>Full compiled definition</summary><pre class="json-view">${escapeHTML(JSON.stringify(workflow, null, 2))}</pre></details></section>
        <section class="detail-section"><button class="button button-primary definition-start" type="button">Start this workflow <span>→</span></button></section>
      </div>`;
      root.querySelector('.definition-start').addEventListener('click', () => openStartDialog(name));
    } catch (error) {
      showToast(error.message || 'Could not load workflow definition', true);
    }
  }

  function openStartDialog(workflowName = '') {
    const select = byId('workflow-select');
    select.innerHTML = state.workflows.map((workflow) => `<option value="${escapeHTML(workflow.name)}">${escapeHTML(workflow.name)}</option>`).join('');
    if (!state.workflows.length) {
      showToast('Register a workflow definition before starting a run', true);
      return;
    }
    if (workflowName) select.value = workflowName;
    updateVersions();
    byId('workflow-context').value = '{\n  \n}';
    byId('form-error').hidden = true;
    byId('start-dialog').showModal();
  }

  function updateVersions() {
    const workflow = state.workflows.find((item) => item.name === byId('workflow-select').value);
    const versions = workflow?.versions || [];
    byId('version-select').innerHTML = versions.slice().sort((a, b) => b - a).map((version) => `<option value="${version}">Version ${version}${version === workflow.latest_version ? ' · latest' : ''}</option>`).join('');
  }

  byId('start-button').addEventListener('click', () => openStartDialog());
  byId('workflow-rows').addEventListener('click', async (event) => {
    const startButton = event.target.closest('[data-start-workflow]');
    if (startButton) {
      event.stopPropagation();
      openStartDialog(startButton.dataset.startWorkflow);
      return;
    }
    const workflowButton = event.target.closest('[data-workflow]');
    if (!workflowButton) return;
    const name = workflowButton.dataset.workflow;
    state.selectedWorkflow = state.selectedWorkflow === name ? '' : name;
    state.selectedRun = '';
    state.offset = 0;
    byId('clear-workflow-filter').hidden = !state.selectedWorkflow;
    byId('workflow-rows').querySelectorAll('tr[data-workflow]').forEach((row) => {
      row.classList.toggle('selected-workflow', row.dataset.workflow === state.selectedWorkflow);
    });
    try {
      await loadRuns();
      state.runSignature = JSON.stringify({ runs: state.runs, total: state.total, offset: state.offset });
      renderRuns();
      renderWorkflows();
    } catch (error) {
      showToast(error.message || 'Could not filter workflow runs', true);
    }
    if (state.selectedWorkflow) await loadWorkflowDefinition(state.selectedWorkflow);
  });
  byId('workflow-rows').addEventListener('keydown', (event) => {
    const workflowRow = event.target.closest('tr[data-workflow]');
    if (workflowRow && !event.target.closest('[data-start-workflow]') && (event.key === 'Enter' || event.key === ' ')) {
      event.preventDefault();
      workflowRow.querySelector('.workflow-name')?.click();
    }
  });
  byId('run-rows').addEventListener('click', (event) => {
    const runButton = event.target.closest('[data-run-id]');
    const runRow = event.target.closest('tr[data-run-id]');
    const workflowRunID = runButton?.dataset.runId || runRow?.dataset.runId;
    if (workflowRunID) loadDetail(workflowRunID);
  });
  byId('run-rows').addEventListener('keydown', (event) => {
    const runRow = event.target.closest('tr[data-run-id]');
    if (runRow && (event.key === 'Enter' || event.key === ' ')) {
      event.preventDefault();
      loadDetail(runRow.dataset.runId);
    }
  });
  byId('workflow-select').addEventListener('change', updateVersions);
  byId('cancel-start').addEventListener('click', () => byId('start-dialog').close());
  byId('start-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const errorNode = byId('form-error');
    try {
      const context = JSON.parse(byId('workflow-context').value || '{}');
      const name = byId('workflow-select').value;
      const response = await api(`/v1/workflows/${encodeURIComponent(name)}/instances`, {
        method: 'POST', body: JSON.stringify({ version: Number(byId('version-select').value), context })
      });
      byId('start-dialog').close();
      state.offset = 0;
      await refresh();
      await loadDetail(response.ID || response.id);
      showToast(`Started ${name}`);
    } catch (error) {
      errorNode.textContent = error instanceof SyntaxError ? 'Input context must be valid JSON' : (error.message || 'Could not start workflow');
      errorNode.hidden = false;
    }
  });
  byId('refresh-button').addEventListener('click', refresh);
  byId('status-filter').addEventListener('change', () => { state.offset = 0; refresh(); });
  byId('run-search').addEventListener('input', renderRuns);
  byId('previous-page').addEventListener('click', () => { state.offset = Math.max(0, state.offset - pageSize); refresh(); });
  byId('next-page').addEventListener('click', () => { state.offset += pageSize; refresh(); });
  byId('clear-workflow-filter').addEventListener('click', async () => {
    state.selectedWorkflow = '';
    state.selectedRun = '';
    state.offset = 0;
    byId('clear-workflow-filter').hidden = true;
    byId('workflow-rows').querySelectorAll('tr[data-workflow]').forEach((row) => row.classList.remove('selected-workflow'));
    try {
      await loadRuns();
      state.runSignature = JSON.stringify({ runs: state.runs, total: state.total, offset: state.offset });
      renderRuns();
      byId('detail-placeholder').hidden = false;
      byId('run-detail').hidden = true;
    } catch (error) {
      showToast(error.message || 'Could not clear workflow filter', true);
    }
  });
  tokenInput.addEventListener('change', () => { sessionStorage.setItem('zephyr-token', tokenInput.value.trim()); refresh(); });
  signOutButton.addEventListener('click', async () => {
    const response = await fetch('/auth/logout', { method: 'POST', credentials: 'same-origin' });
    if (response.ok) window.location.assign('/auth/login');
  });
  document.querySelectorAll('[data-view]').forEach((button) => button.addEventListener('click', () => {
    document.querySelectorAll('[data-view]').forEach((item) => item.classList.toggle('active', item === button));
    const workflows = button.dataset.view === 'workflows';
    byId('page-title').textContent = workflows ? 'Workflow catalog' : 'Workflow operations';
    byId('page-subtitle').textContent = workflows ? 'Registered definitions and their recent executions.' : 'A clear view of what is defined, running, and finished.';
    byId('breadcrumb-current').textContent = workflows ? 'Workflows' : 'Overview';
    document.getElementById(workflows ? 'workflow-section' : 'runs-section').scrollIntoView({ behavior: 'smooth', block: 'start' });
  }));

  configureAuthentication().then((ready) => {
    if (!ready) return;
    refresh();
    setInterval(refresh, 5000);
  }).catch((error) => showToast(error.message, true));
})();
