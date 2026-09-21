"use strict";

const state = {
  incidents: [],
  current: null,
  view: null,
  corrected: false,
  edgeFrom: null,
  edgeTo: null,
};

async function api(method, path, body, raw) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    if (raw) { opts.body = body; opts.headers["Content-Type"] = "application/json"; }
    else { opts.body = JSON.stringify(body); opts.headers["Content-Type"] = "application/json"; }
  }
  const res = await fetch(path, opts);
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (_) { data = text; }
  if (!res.ok) {
    const err = new Error((data && data.error) || ("HTTP " + res.status));
    err.payload = data;
    err.status = res.status;
    throw err;
  }
  return data;
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}

function toast(msg, kind) {
  const box = document.getElementById("toast");
  const el = document.createElement("div");
  el.className = "toast " + (kind || "err");
  el.textContent = msg;
  box.appendChild(el);
  setTimeout(() => el.remove(), kind === "ok" ? 3500 : 7000);
}

function fmtEventKey(k) {
  const i = k.indexOf("/");
  return i < 0 ? k : k.slice(0, i) + " / " + k.slice(i + 1);
}

function shortTime(t) {
  if (!t) return "";
  const d = new Date(t);
  return isNaN(d) ? t : d.toISOString().replace("T", " ").replace(/\..*Z$/, "Z");
}

async function loadIncidents(selectId) {
  state.incidents = await api("GET", "/api/incidents");
  renderLeft();
  if (selectId) selectIncident(selectId);
}

function renderLeft() {
  const p = document.getElementById("leftPanel");
  p.innerHTML = `
    <h2>事故</h2>
    <div class="row">
      <input type="text" id="incTitle" placeholder="新事故标题，如：支付中断" style="flex:1;min-width:0">
      <button id="incCreate">新建事故</button>
    </div>
    <ul class="clean">
      ${state.incidents.map(i => `
        <li class="inc ${state.current === i.id ? "active" : ""}" data-id="${esc(i.id)}">
          <div class="t">${esc(i.title)} <span class="muted">${esc(i.id)}</span></div>
          <div class="m">${i.events} 个事件 · ${i.active_edges} 条有效边</div>
        </li>`).join("")}
    </ul>
  `;
  p.querySelector("#incCreate").onclick = async () => {
    const title = p.querySelector("#incTitle").value.trim();
    try {
      const inc = await api("POST", "/api/incidents", { title });
      toast("已创建事故 " + inc.id, "ok");
      await loadIncidents(inc.id);
    } catch (e) { toast(e.message); }
  };
  p.querySelectorAll(".inc").forEach(li => {
    li.onclick = () => selectIncident(li.dataset.id);
  });
}

async function selectIncident(id) {
  state.current = id;
  state.edgeFrom = null; state.edgeTo = null;
  renderLeft();
  await loadView();
}

async function loadView() {
  if (!state.current) return;
  const v = await api("GET", `/api/incidents/${encodeURIComponent(state.current)}?corrected=${state.corrected ? 1 : 0}`);
  state.view = v;
  renderRight();
}

function eventCard(ev, zone) {
  const tags = [];
  if (ev.receive_count > 1) tags.push(`<span class="tag dup">重复 ×${ev.receive_count}</span>`);
  if (ev.corrected) tags.push(`<span class="tag corr">已校正 v${ev.version}</span>`);
  if (ev.revoked) tags.push(`<span class="tag rev">已撤销</span>`);
  const cls = ["evt"];
  if (state.edgeFrom === ev.key) cls.push("from-sel");
  if (state.edgeTo === ev.key) cls.push("to-sel");
  if (ev.revoked) cls.push("revoked");
  return `
    <div class="${cls.join(" ")}" data-key="${esc(ev.key)}" draggable="${ev.revoked ? "false" : "true"}">
      <div>${tags.join("")}<strong>${esc(fmtEventKey(ev.key))}</strong></div>
      <div class="meta">${esc(shortTime(ev.timestamp))} · 接收序号 ${ev.first_seen_seq} · ${zone}序位 ${ev.order + 1}</div>
    </div>`;
}

function renderRight() {
  const v = state.view;
  const p = document.getElementById("rightPanel");
  p.innerHTML = `
    <h2 style="display:flex;align-items:center;gap:10px;">
      ${esc(v.title)} <span class="muted" style="font-weight:400;text-transform:none;letter-spacing:0">${esc(v.id)}</span>
    </h2>

    <div class="panel" style="background:var(--panel2);margin-bottom:14px;">
      <h2>批量导入事件</h2>
      <p class="hint">粘贴一个 JSON 数组，每行（每个元素）需要 source、event_id、RFC3339 timestamp；其余字段原样保留。逐行独立入库，坏行不影响好行。</p>
      <textarea id="bulkInput" placeholder='[&#10;  {"source":"app","event_id":"e1","timestamp":"2026-09-21T10:00:00Z","msg":"..."},&#10;  {"source":"db","event_id":"e2","timestamp":"2026-09-21T10:01:00Z"}&#10;]'></textarea>
      <div class="row" style="margin-top:8px">
        <button id="bulkBtn">导入到本事故</button>
        <span id="bulkSummary" class="muted"></span>
      </div>
    </div>

    <div class="panel" style="background:var(--panel2);margin-bottom:14px;">
      <h2>建立关系</h2>
      <p class="hint">方式一：把“因/在先”事件的卡片拖到“果/在后”卡片上；方式二：依次点击两个事件（先点起点，再点终点）。然后选择类型并提交。</p>
      <div class="row">
        <span>起点：<strong id="fromLabel" class="imp-ok">${state.edgeFrom ? esc(fmtEventKey(state.edgeFrom)) : "未选择"}</strong></span>
        <span>→ 终点：<strong id="toLabel" style="color:var(--warn)">${state.edgeTo ? esc(fmtEventKey(state.edgeTo)) : "未选择"}</strong></span>
      </div>
      <div class="row">
        <select id="edgeKind">
          <option value="before">先后约束 before</option>
          <option value="causes">可能因果 causes</option>
        </select>
        <input type="text" id="edgeWhy" placeholder="为什么成立？依据/证据（必填建议）" style="flex:1;min-width:220px">
        <button id="edgeAdd">提交边</button>
        <button class="ghost" id="edgeClear">清除选择</button>
      </div>
    </div>

    <div class="switch row">
      <label><input type="radio" name="tl" value="raw" ${!state.corrected ? "checked" : ""}> 原始时间线</label>
      <label><input type="radio" name="tl" value="corr" ${state.corrected ? "checked" : ""}> 校正后时间线</label>
    </div>
    <div class="grid2">
      <div>
        <h2>原始接收顺序</h2>
        <div id="receptionList">${v.reception_order.map(e => eventCard(e, "接收")).join("") || '<p class="muted">暂无事件</p>'}</div>
      </div>
      <div>
        <h2>${state.corrected ? "校正后" : "按事件时间"}时间线</h2>
        <div id="timelineList">${v.timeline.map(e => eventCard(e, "时间线")).join("") || '<p class="muted">暂无事件</p>'}</div>
      </div>
    </div>

    <div style="margin-top:14px;">
      <h2>边与成立依据</h2>
      <div id="edgeList">
        ${v.edges.map(edgeRow).join("") || '<p class="muted">还没有边。</p>'}
      </div>
    </div>
  `;

  p.querySelectorAll('input[name=tl]').forEach(r => {
    r.onchange = () => { state.corrected = r.value === "corr"; loadView(); };
  });
  p.querySelector("#bulkBtn").onclick = doBulk;
  p.querySelector("#edgeAdd").onclick = doAddEdge;
  p.querySelector("#edgeClear").onclick = () => {
    state.edgeFrom = null; state.edgeTo = null; renderRight();
  };
  p.querySelectorAll(".evt").forEach(card => wireCard(card, p));
}

function edgeRow(e) {
  return `
    <div class="edge">
      <div><span class="kind">${esc(e.kind_label)}</span>
        <strong>${esc(fmtEventKey(e.from))}</strong> → <strong>${esc(fmtEventKey(e.to))}</strong>
        <button class="ghost" style="float:right" data-del-edge data-from="${esc(e.from)}" data-to="${esc(e.to)}">删除</button>
      </div>
      <div class="why">${esc(e.explanation)}</div>
    </div>`;
}

function wireCard(card, root) {
  const key = card.dataset.key;
  card.onclick = (ev) => {
    if (ev.target.closest("button")) return;
    if (ev.detail === 2 || card.dataset.detail === "1") {
      if (card.dataset.detail === "1") { openEvent(key); card.dataset.detail = ""; return; }
      card.dataset.detail = "1";
      setTimeout(() => { card.dataset.detail = ""; }, 400);
      return;
    }
    if (!state.edgeFrom) state.edgeFrom = key;
    else if (!state.edgeTo && key !== state.edgeFrom) state.edgeTo = key;
    else { state.edgeFrom = key; state.edgeTo = null; }
    renderRight();
  };
  card.ondblclick = () => openEvent(key);
  card.ondragstart = (ev) => { ev.dataTransfer.setData("text/plain", key); ev.dataTransfer.effectAllowed = "link"; };
  card.ondragover = (ev) => { ev.preventDefault(); ev.dataTransfer.dropEffect = "link"; card.style.outline = "2px dashed var(--warn)"; };
  card.ondragleave = () => { card.style.outline = ""; };
  card.ondrop = (ev) => {
    ev.preventDefault();
    card.style.outline = "";
    const from = ev.dataTransfer.getData("text/plain");
    if (!from || from === key) return;
    state.edgeFrom = from;
    state.edgeTo = key;
    renderRight();
  };
}

async function doBulk() {
  const ta = document.getElementById("bulkInput");
  const summary = document.getElementById("bulkSummary");
  let arr;
  try {
    arr = JSON.parse(ta.value);
    if (!Array.isArray(arr)) throw new Error("顶层必须是数组");
  } catch (e) { summary.textContent = "解析失败：" + e.message; return; }
  try {
    const res = await api("POST", `/api/incidents/${encodeURIComponent(state.current)}/bulk`, { events: arr });
    summary.innerHTML = `共 ${res.total}，<span class="imp-ok">成功 ${res.accepted}</span>，<span class="imp-bad">失败 ${res.failed}</span>`;
    const lines = res.results.map(r => r.accepted
      ? `#${r.index} ✓ 入库 ${r.key}`
      : `#${r.index} ✗ ${r.error}`);
    toast("批量导入结果\n" + lines.join("\n"), res.failed ? "err" : "ok");
    ta.value = "";
    await loadView();
  } catch (e) { toast(e.message); }
}

async function doAddEdge() {
  if (!state.edgeFrom || !state.edgeTo) { toast("请先选择起点和终点事件"); return; }
  const kind = document.getElementById("edgeKind").value;
  const rationale = document.getElementById("edgeWhy").value.trim();
  try {
    await api("POST", `/api/incidents/${encodeURIComponent(state.current)}/edges`, {
      from: state.edgeFrom, to: state.edgeTo, kind, rationale,
    });
    toast("边已建立", "ok");
    state.edgeFrom = null; state.edgeTo = null;
    await loadView();
  } catch (e) {
    if (e.payload && e.payload.code === "cycle_detected") {
      toast("拒绝：会形成环路\n路径：" + e.payload.path.join(" -> "));
    } else { toast(e.message); }
  }
}

document.addEventListener("click", async (ev) => {
  const del = ev.target.closest("[data-del-edge]");
  if (del) {
    if (!confirm("删除这条边（逻辑删除，写入审计）？")) return;
    const q = new URLSearchParams({ from: del.dataset.from, to: del.dataset.to });
    try {
      await api("DELETE", `/api/incidents/${encodeURIComponent(state.current)}/edges?${q}`);
      toast("边已删除", "ok");
      await loadView();
    } catch (e) { toast(e.message); }
  }
});

async function openEvent(key) {
  const ev = await api("GET", "/api/event?key=" + encodeURIComponent(key));
  const inIncident = state.view && state.view.timeline.some(t => t.key === key);
  const versions = ev.versions.map(v => `
    <tr>
      <td>v${v.version}${v.version === 0 ? "（原始）" : ""}</td>
      <td>${esc(shortTime(v.timestamp))}</td>
      <td>${esc(v.note || "")}</td>
      <td>${esc(shortTime(v.created_at))} · seq ${v.seq}</td>
    </tr>`).join("");
  const recs = ev.receptions.map(r => `
    <tr><td>${esc(shortTime(r.at))}</td><td>${r.seq}</td></tr>`).join("");
  const dlg = document.getElementById("dlg");
  dlg.innerHTML = `
    <div class="dlg-head">
      <strong>${esc(fmtEventKey(ev.key))}</strong>
      <button class="ghost" id="dlgClose">关闭</button>
    </div>
    <div class="dlg-body">
      <p class="muted">接收次数：<strong>${ev.receive_count}</strong> ·
      当前版本：<strong>v${ev.versions[ev.versions.length - 1].version}</strong> ·
      状态：${ev.revoked ? "已逻辑撤销" : "有效"}</p>

      <h2>原始事件（原样保存，永不改写）</h2>
      <pre id="rawBox"></pre>

      <h2>版本历史</h2>
      <table><thead><tr><th>版本</th><th>时间戳</th><th>说明</th><th>写入时间 / seq</th></tr></thead>
      <tbody>${versions}</tbody></table>

      <h2>接收历史</h2>
      <table><thead><tr><th>接收时间</th><th>审计序号</th></tr></thead>
      <tbody>${recs}</tbody></table>

      <h2>提交时间校正</h2>
      <div class="row">
        <input type="datetime-local" id="corrTs" step="1">
        <input type="text" id="corrNote" placeholder="校正原因" style="flex:1;min-width:160px">
        <button id="corrBtn">基于 v${ev.versions.length - 1} 提交新版本</button>
      </div>
      ${inIncident && !ev.revoked ? `
      <h2>危险操作</h2>
      <button class="danger" id="revokeBtn">逻辑撤销该事件</button>
      <p class="hint">仍被有效边引用时服务器会拒绝撤销。</p>` : ""}
   </div>`;
  try {
    dlg.querySelector("#rawBox").textContent = JSON.stringify(JSON.parse(ev.raw), null, 2);
  } catch (_) { dlg.querySelector("#rawBox").textContent = ev.raw; }

  dlg.querySelector("#dlgClose").onclick = () => dlg.close();
  const curVersion = ev.versions.length - 1;
  dlg.querySelector("#corrBtn").onclick = async () => {
    const val = dlg.querySelector("#corrTs").value;
    if (!val) { toast("请选择校正后的时间"); return; }
    const ts = new Date(val).toISOString();
    try {
      await api("POST", "/api/event/correct", {
        key, timestamp: ts,
        note: dlg.querySelector("#corrNote").value.trim(),
        base_version: curVersion,
      });
      toast("校正已形成新版本", "ok");
      dlg.close();
      await loadView();
    } catch (e) {
      if (e.payload && e.payload.code === "version_conflict") {
        toast("版本冲突：该事件已被其他人校正，当前版本 v" + e.payload.current_version + "，请刷新后基于新版本重试。");
      } else { toast(e.message); }
    }
  };
  const rev = dlg.querySelector("#revokeBtn");
  if (rev) rev.onclick = async () => {
    if (!confirm("确认逻辑撤销？原始记录仍保留。")) return;
    try {
      await api("POST", "/api/event/revoke", { key });
      toast("事件已逻辑撤销", "ok");
      dlg.close();
      await loadView();
    } catch (e) { toast(e.message); }
  };
  dlg.showModal();
}

document.getElementById("refreshBtn").onclick = async () => {
  try { await loadIncidents(state.current); toast("已刷新", "ok"); } catch (e) { toast(e.message); }
};

loadIncidents().catch(e => toast(e.message));
