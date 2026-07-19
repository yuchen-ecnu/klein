// SPDX-License-Identifier: Apache-2.0
"use strict";

const POLL_INTERVAL_MS = 2500;
const TERMINAL_STATES = new Set(["FINISHED", "FAILED", "CANCELLED"]);
const SVG_NAMESPACE = "http://www.w3.org/2000/svg";
const GRAPH_NODE_WIDTH = 230;
const GRAPH_NODE_HEIGHT = 96;
const GRAPH_COLUMN_GAP = 92;
const GRAPH_ROW_GAP = 28;
const GRAPH_MARGIN = 34;
const elements = {
  picker: document.querySelector("#job-picker"),
  refresh: document.querySelector("#refresh"),
  notice: document.querySelector("#notice"),
  jobName: document.querySelector("#job-name"),
  jobId: document.querySelector("#job-id"),
  jobStatus: document.querySelector("#job-status"),
  operatorCount: document.querySelector("#operator-count"),
  taskCount: document.querySelector("#task-count"),
  rowsIn: document.querySelector("#rows-in"),
  rowsOut: document.querySelector("#rows-out"),
  restartCount: document.querySelector("#restart-count"),
  updatedAt: document.querySelector("#updated-at"),
  topology: document.querySelector("#topology"),
  topologyEmpty: document.querySelector("#topology-empty"),
  operators: document.querySelector("#operators"),
  empty: document.querySelector("#empty"),
  toast: document.querySelector("#toast"),
};

let selectedJobId = "";
let refreshInFlight = false;
let toastTimer;
const pendingOperations = new Set();

async function request(path, options) {
  const response = await fetch(path, {
    headers: {"Content-Type": "application/json"},
    ...options,
  });
  let payload;
  try {
    payload = await response.json();
  } catch (_) {
    payload = {};
  }
  if (!response.ok) {
    throw new Error(payload.error || `Request failed (${response.status})`);
  }
  return payload;
}

function text(node, value) {
  node.textContent = value == null ? "—" : String(value);
}

function compactNumber(value) {
  const number = Number(value || 0);
  return Intl.NumberFormat(undefined, {notation: "compact", maximumFractionDigits: 1}).format(number);
}

function rate(value) {
  return Intl.NumberFormat(undefined, {maximumFractionDigits: 1}).format(Number(value || 0));
}

function percent(value) {
  return `${Math.round(Number(value || 0))}%`;
}

function finitePercent(value) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.min(100, number)) : 0;
}

function maximumSubtaskPercent(operator, key, fallback) {
  const subtasks = Array.isArray(operator.subtasks) ? operator.subtasks : [];
  if (subtasks.length === 0) return finitePercent(fallback);
  return subtasks.reduce((maximum, subtask) => Math.max(maximum, finitePercent(subtask[key])), 0);
}

function operatorPerformance(operator) {
  const busy = operator.max_busy_percent ?? maximumSubtaskPercent(operator, "busy_percent", operator.busy_percent);
  const backpressure = operator.max_backpressure_percent
    ?? maximumSubtaskPercent(operator, "backpressure_percent", operator.backpressure_percent);
  return {busy: finitePercent(busy), backpressure: finitePercent(backpressure)};
}

function performanceColor({busy, backpressure}) {
  // Busy and backpressure maxima can come from different subtasks and add up
  // to more than 100%. Normalizing the color weights preserves both hotspot
  // signals without producing an invalid negative idle component.
  const weights = [
    Math.max(0, 1 - (busy + backpressure) / 100),
    busy / 100,
    backpressure / 100,
  ];
  const colors = [
    [35, 112, 183],  // idle: blue
    [205, 60, 67],   // busy: red
    [10, 17, 22],    // backpressured: near-black
  ];
  const total = weights.reduce((sum, weight) => sum + weight, 0) || 1;
  const channels = [0, 1, 2].map((channel) => Math.round(
    weights.reduce((sum, weight, index) => sum + weight * colors[index][channel], 0) / total,
  ));
  return `rgb(${channels.join(" ")})`;
}

function svgElement(tagName, attributes = {}) {
  const node = document.createElementNS(SVG_NAMESPACE, tagName);
  for (const [name, value] of Object.entries(attributes)) node.setAttribute(name, String(value));
  return node;
}

function compareOperatorIds(left, right) {
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) return leftNumber - rightNumber;
  return String(left).localeCompare(String(right));
}

function graphLayout(operators, snapshotEdges) {
  const byId = new Map(operators.map((operator) => [String(operator.op_id), operator]));
  const candidateEdges = Array.isArray(snapshotEdges) && snapshotEdges.length > 0
    ? snapshotEdges
    : operators.flatMap((operator) => (operator.downstream || []).map((target) => ({source: operator.op_id, target})));
  const seenEdges = new Set();
  const edges = candidateEdges.flatMap((edge) => {
    const source = String(edge.source);
    const target = String(edge.target);
    const key = `${source}\u0000${target}`;
    if (!byId.has(source) || !byId.has(target) || source === target || seenEdges.has(key)) return [];
    seenEdges.add(key);
    return [{source, target}];
  });

  const incoming = new Map([...byId.keys()].map((id) => [id, 0]));
  const outgoing = new Map([...byId.keys()].map((id) => [id, []]));
  for (const edge of edges) {
    incoming.set(edge.target, incoming.get(edge.target) + 1);
    outgoing.get(edge.source).push(edge.target);
  }
  for (const targets of outgoing.values()) targets.sort(compareOperatorIds);

  const levels = new Map([...byId.keys()].map((id) => [id, 0]));
  const queue = [...byId.keys()].filter((id) => incoming.get(id) === 0).sort(compareOperatorIds);
  const visited = new Set();
  while (queue.length > 0) {
    const source = queue.shift();
    visited.add(source);
    for (const target of outgoing.get(source)) {
      levels.set(target, Math.max(levels.get(target), levels.get(source) + 1));
      incoming.set(target, incoming.get(target) - 1);
      if (incoming.get(target) === 0) {
        queue.push(target);
        queue.sort(compareOperatorIds);
      }
    }
  }
  // The runtime emits a DAG, but malformed/older snapshots should still show
  // every operator instead of leaving the graph blank.
  for (const id of byId.keys()) if (!visited.has(id)) levels.set(id, 0);

  const columns = new Map();
  for (const [id, level] of levels) {
    if (!columns.has(level)) columns.set(level, []);
    columns.get(level).push(id);
  }
  for (const ids of columns.values()) ids.sort(compareOperatorIds);
  const columnCount = [...columns.keys()].reduce((maximum, level) => Math.max(maximum, level), 0) + 1;
  const largestColumn = [...columns.values()].reduce((maximum, ids) => Math.max(maximum, ids.length), 1);
  const width = Math.max(
    560,
    GRAPH_MARGIN * 2 + columnCount * GRAPH_NODE_WIDTH + (columnCount - 1) * GRAPH_COLUMN_GAP,
  );
  const height = Math.max(
    180,
    GRAPH_MARGIN * 2 + largestColumn * GRAPH_NODE_HEIGHT + (largestColumn - 1) * GRAPH_ROW_GAP,
  );
  const positions = new Map();
  for (const [level, ids] of columns) {
    const columnHeight = ids.length * GRAPH_NODE_HEIGHT + (ids.length - 1) * GRAPH_ROW_GAP;
    const top = (height - columnHeight) / 2;
    ids.forEach((id, index) => positions.set(id, {
      x: GRAPH_MARGIN + level * (GRAPH_NODE_WIDTH + GRAPH_COLUMN_GAP),
      y: top + index * (GRAPH_NODE_HEIGHT + GRAPH_ROW_GAP),
    }));
  }
  return {byId, edges, positions, width, height};
}

function topologyNode(operator, position) {
  const performance = operatorPerformance(operator);
  const name = operator.name || `Operator ${operator.op_id}`;
  const status = String(operator.status || "UNKNOWN").toUpperCase();
  const group = svgElement("g", {
    class: "topology-node",
    transform: `translate(${position.x} ${position.y})`,
    tabindex: "0",
    role: "img",
    "aria-label": `${name}, ${status}, busy ${Math.round(performance.busy)}%, backpressure ${Math.round(performance.backpressure)}%`,
  });
  const title = svgElement("title");
  title.textContent = `${name}\nBusy ${percent(performance.busy)} · Backpressure ${percent(performance.backpressure)}`;
  const shape = svgElement("rect", {
    class: "topology-node-shape",
    width: GRAPH_NODE_WIDTH,
    height: GRAPH_NODE_HEIGHT,
    rx: 11,
    fill: performanceColor(performance),
  });
  const nameText = svgElement("text", {class: "topology-node-name", x: 16, y: 27});
  nameText.textContent = name.length > 29 ? `${name.slice(0, 28)}…` : name;
  const detailText = svgElement("text", {class: "topology-node-detail", x: 16, y: 51});
  detailText.textContent = `${status} · parallelism ${operator.parallelism ?? 0}`;
  const metricText = svgElement("text", {class: "topology-node-metric", x: 16, y: 77});
  metricText.textContent = `Busy ${percent(performance.busy)}  ·  BP ${percent(performance.backpressure)}`;
  group.append(title, shape, nameText, detailText, metricText);
  return group;
}

function renderTopology(operators, edges) {
  if (operators.length === 0) {
    elements.topology.replaceChildren();
    elements.topology.hidden = true;
    elements.topologyEmpty.hidden = false;
    return;
  }
  const layout = graphLayout(operators, edges);
  const defs = svgElement("defs");
  const marker = svgElement("marker", {
    id: "topology-arrow",
    viewBox: "0 0 10 10",
    refX: 9,
    refY: 5,
    markerWidth: 7,
    markerHeight: 7,
    orient: "auto-start-reverse",
  });
  marker.append(svgElement("path", {d: "M 0 0 L 10 5 L 0 10 z", class: "topology-arrow"}));
  defs.append(marker);
  const edgeLayer = svgElement("g", {class: "topology-edges", "aria-hidden": "true"});
  for (const edge of layout.edges) {
    const source = layout.positions.get(edge.source);
    const target = layout.positions.get(edge.target);
    const startX = source.x + GRAPH_NODE_WIDTH;
    const startY = source.y + GRAPH_NODE_HEIGHT / 2;
    const endX = target.x;
    const endY = target.y + GRAPH_NODE_HEIGHT / 2;
    const bend = Math.max(35, (endX - startX) / 2);
    edgeLayer.append(svgElement("path", {
      class: "topology-edge",
      d: `M ${startX} ${startY} C ${startX + bend} ${startY}, ${endX - bend} ${endY}, ${endX} ${endY}`,
      "marker-end": "url(#topology-arrow)",
    }));
  }
  const nodeLayer = svgElement("g", {class: "topology-nodes"});
  for (const [id, operator] of layout.byId) nodeLayer.append(topologyNode(operator, layout.positions.get(id)));
  elements.topology.setAttribute("viewBox", `0 0 ${layout.width} ${layout.height}`);
  elements.topology.setAttribute("width", String(layout.width));
  elements.topology.setAttribute("height", String(layout.height));
  elements.topology.replaceChildren(defs, edgeLayer, nodeLayer);
  elements.topology.hidden = false;
  elements.topologyEmpty.hidden = true;
}

function statusClass(status) {
  const value = String(status || "neutral").toLowerCase();
  return ["running", "finished", "failed", "cancelled", "recovering"].includes(value) ? value : "neutral";
}

function showToast(message, error = false) {
  window.clearTimeout(toastTimer);
  elements.toast.textContent = message;
  elements.toast.className = error ? "toast error" : "toast";
  elements.toast.hidden = false;
  toastTimer = window.setTimeout(() => { elements.toast.hidden = true; }, 5000);
}

function setNotice(snapshot) {
  if (snapshot.dashboard_stale) {
    elements.notice.textContent = `Showing the last known state. ${snapshot.dashboard_error || "The job manager is unavailable."}`;
    elements.notice.hidden = false;
    return;
  }
  elements.notice.hidden = true;
}

function rescaleCapability(operator) {
  const explicit = typeof operator.can_rescale === "boolean" ? operator.can_rescale : operator.scalable;
  const allowed = explicit === true;
  return {
    allowed,
    reason: operator.rescale_disabled_reason || operator.rescale_reason || "This operator cannot be rescaled safely.",
  };
}

function renderOperator(operator, jobStatus, stale) {
  const jobId = selectedJobId;
  const operationKey = `${jobId}:${operator.op_id}`;
  const row = document.createElement("tr");
  const identity = document.createElement("td");
  const name = document.createElement("span");
  name.className = "operator-name";
  name.textContent = operator.name || "Unnamed operator";
  const id = document.createElement("span");
  id.className = "operator-id";
  id.textContent = `operator ${operator.op_id}`;
  identity.append(name, id);

  const stateCell = document.createElement("td");
  const state = document.createElement("span");
  state.className = `status ${statusClass(operator.status)}`;
  state.textContent = String(operator.status || "unknown").toUpperCase();
  stateCell.append(state);

  const current = document.createElement("td");
  current.textContent = String(operator.parallelism ?? 0);

  const rowsIn = document.createElement("td");
  rowsIn.textContent = rate(operator.rows_in_per_second);
  const rowsOut = document.createElement("td");
  rowsOut.textContent = rate(operator.rows_out_per_second);
  const busy = document.createElement("td");
  busy.textContent = percent(operator.busy_percent);
  const backpressure = document.createElement("td");
  backpressure.textContent = percent(operator.backpressure_percent);

  const action = document.createElement("td");
  const control = document.createElement("form");
  control.className = "scale-control";
  const input = document.createElement("input");
  input.type = "number";
  input.min = "1";
  input.step = "1";
  input.required = true;
  input.value = String(operator.parallelism ?? 1);
  input.setAttribute("aria-label", `Target parallelism for ${operator.name || operator.op_id}`);
  const button = document.createElement("button");
  button.type = "submit";
  button.textContent = pendingOperations.has(operationKey) ? "Applying…" : "Apply";
  const capability = rescaleCapability(operator);
  button.disabled = stale || TERMINAL_STATES.has(jobStatus) || !capability.allowed || pendingOperations.has(operationKey);
  input.disabled = button.disabled;
  if (!capability.allowed) {
    input.title = capability.reason;
    button.title = capability.reason;
  }
  control.append(input, button);
  control.addEventListener("submit", async (event) => {
    event.preventDefault();
    const parallelism = Number(input.value);
    if (!Number.isInteger(parallelism) || parallelism < 1) {
      showToast("Parallelism must be a positive integer.", true);
      return;
    }
    if (parallelism === Number(operator.parallelism)) {
      showToast(`${operator.name || `Operator ${operator.op_id}`} is already at parallelism ${parallelism}.`);
      return;
    }
    input.disabled = true;
    button.disabled = true;
    button.textContent = "Applying…";
    pendingOperations.add(operationKey);
    try {
      const operation = await request(
        `/api/jobs/${encodeURIComponent(jobId)}/operators/${encodeURIComponent(operator.op_id)}/rescale`,
        {method: "POST", body: JSON.stringify({parallelism})},
      );
      if (["REJECTED", "FAILED"].includes(String(operation.status || "").toUpperCase())) {
        throw new Error(operation.error || `Scale operation ${operation.status.toLowerCase()}.`);
      }
      pendingOperations.delete(operationKey);
      showToast(`${operator.name || `Operator ${operator.op_id}`} scaled to ${parallelism}.`);
      try {
        await refreshSnapshot();
      } catch (error) {
        showToast(`Scale completed, but the snapshot refresh failed: ${error.message}`, true);
      }
    } catch (error) {
      showToast(error.message, true);
      input.disabled = false;
      button.disabled = false;
    } finally {
      pendingOperations.delete(operationKey);
      button.textContent = "Apply";
    }
  });
  action.append(control);
  if (!capability.allowed) {
    const reason = document.createElement("span");
    reason.className = "scale-disabled-reason";
    reason.textContent = capability.reason;
    action.append(reason);
  }
  row.append(identity, stateCell, current, rowsIn, rowsOut, busy, backpressure, action);
  return row;
}

function renderSnapshot(snapshot) {
  const overview = snapshot.overview || {};
  const operators = snapshot.operators || [];
  const jobStatus = String(snapshot.status || "UNKNOWN").toUpperCase();
  text(elements.jobName, snapshot.job_name || "Unnamed Klein job");
  text(elements.jobId, snapshot.namespace || snapshot.job_id || selectedJobId);
  text(elements.jobStatus, jobStatus);
  elements.jobStatus.className = `status ${statusClass(jobStatus)}`;
  text(elements.operatorCount, overview.operators ?? operators.length);
  text(elements.taskCount, overview.task_instances ?? operators.reduce((sum, item) => sum + Number(item.parallelism || 0), 0));
  text(elements.rowsIn, compactNumber(overview.rows_in));
  text(elements.rowsOut, compactNumber(overview.rows_out));
  text(elements.restartCount, overview.restarts ?? 0);
  text(elements.updatedAt, `Updated ${new Date().toLocaleTimeString()}`);
  setNotice(snapshot);
  renderTopology(operators, snapshot.edges);
  elements.operators.replaceChildren(...operators.map((operator) => renderOperator(operator, jobStatus, snapshot.dashboard_stale)));
  elements.empty.hidden = operators.length > 0;
}

function renderNoJobs() {
  selectedJobId = "";
  elements.picker.replaceChildren();
  elements.picker.disabled = true;
  text(elements.jobName, "No published Klein jobs");
  text(elements.jobId, "Start a streaming job with dashboard publication enabled.");
  text(elements.jobStatus, "EMPTY");
  elements.jobStatus.className = "status neutral";
  for (const node of [elements.operatorCount, elements.taskCount, elements.rowsIn, elements.rowsOut, elements.restartCount]) text(node, 0);
  renderTopology([], []);
  elements.operators.replaceChildren();
  elements.empty.hidden = false;
}

async function refreshJobs() {
  const payload = await request("/api/jobs");
  const jobs = payload.jobs || [];
  if (jobs.length === 0) {
    renderNoJobs();
    return;
  }
  const ids = jobs.map((job) => String(job.job_id || job.namespace || "")).filter(Boolean);
  if (!ids.includes(selectedJobId)) selectedJobId = ids[0];
  const options = jobs.flatMap((job) => {
    const jobId = String(job.job_id || job.namespace || "");
    if (!jobId) return [];
    const option = document.createElement("option");
    option.value = jobId;
    option.textContent = `${job.job_name || jobId} · ${job.status || "UNKNOWN"}`;
    option.selected = jobId === selectedJobId;
    return [option];
  });
  elements.picker.replaceChildren(...options);
  elements.picker.disabled = false;
  const selected = jobs.find((job) => String(job.job_id || job.namespace || "") === selectedJobId);
  if (selected) renderSnapshot(selected);
}

async function refreshSnapshot() {
  if (!selectedJobId) return;
  const snapshot = await request(`/api/jobs/${encodeURIComponent(selectedJobId)}`);
  renderSnapshot(snapshot);
}

async function refresh() {
  if (refreshInFlight) return;
  refreshInFlight = true;
  elements.refresh.disabled = true;
  try {
    await refreshJobs();
  } catch (error) {
    showToast(error.message, true);
  } finally {
    elements.refresh.disabled = false;
    refreshInFlight = false;
  }
}

elements.picker.addEventListener("change", async () => {
  selectedJobId = elements.picker.value;
  try { await refreshSnapshot(); } catch (error) { showToast(error.message, true); }
});
elements.refresh.addEventListener("click", refresh);
window.setInterval(refresh, POLL_INTERVAL_MS);
refresh();
