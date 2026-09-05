/**
 * The entire frontend: one page, no build step, no framework.
 *
 * It exists to make the /dashboard WebSocket channel observable by a human -
 * queue depths per tenant, throughput, worker liveness, recent failures. The
 * scope stops there deliberately; this is a backend service.
 *
 * Note the client-side backpressure story is the mirror of the server's: the
 * page only ever renders the most recent snapshot, so if it falls behind it
 * simply skips frames rather than queueing them.
 */
export const DASHBOARD_HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Job Scheduler</title>
<style>
  :root { color-scheme: light dark; --bg:#0f1115; --fg:#e6e8eb; --dim:#8b93a1; --line:#242833;
          --ok:#3fb950; --warn:#d29922; --bad:#f85149; --accent:#58a6ff; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--fg); font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace; }
  header { display:flex; align-items:baseline; gap:16px; padding:14px 20px; border-bottom:1px solid var(--line); }
  h1 { font-size:14px; margin:0; letter-spacing:.08em; text-transform:uppercase; }
  .status { font-size:12px; color:var(--dim); }
  .dot { display:inline-block; width:8px; height:8px; border-radius:50%; background:var(--bad); margin-right:6px; }
  .dot.live { background:var(--ok); }
  main { display:grid; grid-template-columns:repeat(auto-fit,minmax(340px,1fr)); gap:16px; padding:16px 20px; }
  section { border:1px solid var(--line); border-radius:8px; padding:12px 14px; min-width:0; }
  h2 { font-size:11px; margin:0 0 10px; color:var(--dim); letter-spacing:.1em; text-transform:uppercase; }
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th { text-align:left; color:var(--dim); font-weight:500; padding:3px 8px 3px 0; }
  td { padding:3px 8px 3px 0; white-space:nowrap; }
  td.num { text-align:right; font-variant-numeric:tabular-nums; }
  .wrap { overflow-x:auto; }
  .big { font-size:26px; font-variant-numeric:tabular-nums; }
  .kpis { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; }
  .err { color:var(--bad); } .warnc { color:var(--warn); } .okc { color:var(--ok); }
  .muted { color:var(--dim); }
  .trunc { max-width:340px; overflow:hidden; text-overflow:ellipsis; display:inline-block; vertical-align:bottom; }
  ul.events { list-style:none; margin:0; padding:0; max-height:220px; overflow-y:auto; }
  ul.events li { padding:3px 0; border-bottom:1px solid var(--line); font-size:12px; }
</style>
</head>
<body>
<header>
  <h1>Job Scheduler</h1>
  <span class="status"><span id="dot" class="dot"></span><span id="conn">connecting</span></span>
  <span class="status" id="stamp"></span>
</header>
<main>
  <section>
    <h2>Throughput</h2>
    <div class="kpis">
      <div><div class="big okc" id="rate">0</div><div class="muted">jobs/sec</div></div>
      <div><div class="big" id="pending">0</div><div class="muted">pending</div></div>
      <div><div class="big" id="running">0</div><div class="muted">in flight</div></div>
    </div>
  </section>
  <section>
    <h2>Queues</h2>
    <div class="wrap"><table id="queues"></table></div>
  </section>
  <section>
    <h2>Depth by tenant</h2>
    <div class="wrap"><table id="tenants"></table></div>
  </section>
  <section>
    <h2>Workers</h2>
    <div class="wrap"><table id="workers"></table></div>
  </section>
  <section>
    <h2>Recent failures</h2>
    <div class="wrap"><table id="failures"></table></div>
  </section>
  <section>
    <h2>Events</h2>
    <ul class="events" id="events"></ul>
  </section>
</main>
<script>
(function () {
  var proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
  var ws, retry = 500;

  function el(id) { return document.getElementById(id); }
  function text(v) { return document.createTextNode(String(v)); }

  function table(node, headers, rows) {
    node.innerHTML = '';
    var thead = document.createElement('tr');
    for (var h = 0; h < headers.length; h++) {
      var th = document.createElement('th');
      th.appendChild(text(headers[h]));
      thead.appendChild(th);
    }
    node.appendChild(thead);
    for (var r = 0; r < rows.length; r++) {
      var tr = document.createElement('tr');
      for (var c = 0; c < rows[r].length; c++) {
        var td = document.createElement('td');
        var cell = rows[r][c];
        if (cell && typeof cell === 'object') {
          td.className = cell.cls || '';
          td.appendChild(text(cell.v));
        } else {
          if (typeof cell === 'number') td.className = 'num';
          td.appendChild(text(cell));
        }
        tr.appendChild(td);
      }
      node.appendChild(tr);
    }
  }

  function render(s) {
    el('stamp').textContent = 'updated ' + new Date(s.at).toLocaleTimeString();
    el('rate').textContent = s.throughput.per_second.toFixed(1);

    var pending = 0, running = 0;
    var qrows = [];
    for (var i = 0; i < s.queues.length; i++) {
      var q = s.queues[i];
      pending += q.pending; running += q.claimed + q.running;
      qrows.push([q.queue_name, q.pending, q.blocked, q.claimed + q.running,
                  { v: q.dead, cls: q.dead > 0 ? 'num err' : 'num' },
                  q.oldest_pending_seconds.toFixed(1) + 's']);
    }
    el('pending').textContent = pending;
    el('running').textContent = running;
    table(el('queues'), ['queue', 'pending', 'blocked', 'in flight', 'dead', 'oldest'], qrows);

    var trows = [];
    for (var t = 0; t < s.by_tenant.length; t++) {
      var row = s.by_tenant[t];
      trows.push([row.tenant_id, row.queue_name, row.state, row.count]);
    }
    table(el('tenants'), ['tenant', 'queue', 'state', 'jobs'], trows);

    var wrows = [];
    for (var w = 0; w < s.workers.length; w++) {
      var wk = s.workers[w];
      var cls = wk.state === 'DEAD' ? 'err' : (wk.state === 'BUSY' ? 'warnc' : 'okc');
      wrows.push([wk.id.slice(0, 18), { v: wk.state, cls: cls },
                  wk.inflight_count + '/' + wk.max_concurrency,
                  wk.seconds_since_heartbeat.toFixed(1) + 's', wk.queues.join(',')]);
    }
    table(el('workers'), ['worker', 'state', 'load', 'last beat', 'queues'], wrows);

    var frows = [];
    for (var f = 0; f < s.recent_failures.length; f++) {
      var fail = s.recent_failures[f];
      frows.push([fail.job_type, { v: fail.state, cls: fail.state === 'DEAD' ? 'err' : 'warnc' },
                  fail.attempt_count, String(fail.last_error || '').slice(0, 70)]);
    }
    table(el('failures'), ['type', 'state', 'attempts', 'error'], frows);
  }

  function pushEvent(ev) {
    var list = el('events');
    var li = document.createElement('li');
    li.appendChild(text(new Date(ev.at || Date.now()).toLocaleTimeString() + '  ' + ev.type + '  ' +
      (ev.job_type || ev.worker_id || ev.node_id || '')));
    if (ev.type === 'job_dead' || ev.type === 'breaker_opened') li.className = 'err';
    list.insertBefore(li, list.firstChild);
    while (list.children.length > 100) list.removeChild(list.lastChild);
  }

  function connect() {
    ws = new WebSocket(proto + location.host + '/dashboard');
    ws.onopen = function () {
      retry = 500;
      el('dot').className = 'dot live';
      el('conn').textContent = 'live';
    };
    ws.onclose = function () {
      el('dot').className = 'dot';
      el('conn').textContent = 'reconnecting';
      setTimeout(connect, retry);
      retry = Math.min(retry * 2, 10000);
    };
    ws.onmessage = function (m) {
      var frame = JSON.parse(m.data);
      if (frame.type === 'snapshot') render(frame);
      else if (frame.type === 'event') pushEvent(frame.event);
    };
  }
  connect();
})();
</script>
</body>
</html>`;
