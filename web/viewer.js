// Просмотрщик записей симулятора Raft. Без зависимостей: читает traces/index.json и выбранный сценарий.
(() => {
  'use strict';
  const $ = (id) => document.getElementById(id);
  const NS = 'http://www.w3.org/2000/svg';
  const ROLE = { leader: 'лидер', candidate: 'кандидат', follower: 'последователь', dead: 'не работает' };
  const FATE = { dropped: 'потеряно', cut: 'отрезано сетью', dead: 'узел не работает' };

  let trace = null;
  let idx = 0;
  let timer = null;
  let msgs = [];

  const el = (name, attrs = {}, text) => {
    const e = document.createElementNS(NS, name);
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, String(v));
    if (text !== undefined) e.textContent = text;
    return e;
  };
  const html = (tag, attrs = {}, text) => {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, String(v));
    if (text !== undefined) e.textContent = text;
    return e;
  };

  async function load() {
    const index = await (await fetch('traces/index.json')).json();
    const tabs = $('tabs');
    tabs.replaceChildren();
    index.forEach((item, i) => {
      const b = html('button', { type: 'button', 'aria-pressed': 'false', 'data-name': item.name, class: item.bug ? 'bug' : '' }, item.title);
      b.addEventListener('click', () => open(item, b));
      tabs.append(b);
      if (i === 0) open(item, b);
    });
  }

  async function open(item, button) {
    stop();
    document.querySelectorAll('#tabs button').forEach((b) => b.setAttribute('aria-pressed', String(b === button)));
    const t = await (await fetch('traces/' + item.file)).json();
    // полные журналы для каждого кадра: кадр хранит журнал, только если он изменился
    const cur = [];
    for (const f of t.frames) {
      for (const n of f.nodes) {
        if (n.log) cur[n.id] = { log: n.log, base: n.logBase || 0 };
        n.full = cur[n.id] || { log: [], base: 0 };
      }
    }
    trace = t;
    msgs = t.messages;
    $('title').textContent = item.title;
    $('desc').textContent = item.description;
    const v = $('violation');
    if (t.violation) {
      v.hidden = false;
      v.textContent = 'Проверка поймала нарушение. ' + t.violation.split('\n').slice(0, 1).join(' ');
    } else v.hidden = true;
    const slider = $('slider');
    $('jump').hidden = !t.violation;
    slider.max = String(t.frames.length - 1);
    idx = 0;
    slider.value = '0';
    render();
    document.body.dataset.loaded = item.name; // признак для тестов: запись загружена и показана
    history.replaceState(null, '', '#' + item.name);
  }

  function pos(i, n) {
    const a = (Math.PI * 2 * i) / n - Math.PI / 2;
    return { x: 320 + 160 * Math.cos(a), y: 205 + 135 * Math.sin(a) };
  }

  function render() {
    if (!trace) return;
    const f = trace.frames[idx];
    const tick = f.t;
    const n = trace.nodes;
    $('tick').textContent = 'тик ' + tick;
    $('slider').value = String(idx);
    const stage = $('stage');
    stage.replaceChildren();

    // связи
    for (let a = 0; a < n; a++) for (let b = a + 1; b < n; b++) {
      const p = pos(a, n), q = pos(b, n);
      const cut = f.nodes[a].group !== f.nodes[b].group;
      stage.append(el('line', { x1: p.x, y1: p.y, x2: q.x, y2: q.y, stroke: cut ? 'var(--bad)' : 'var(--line)', 'stroke-width': cut ? 2 : 1, 'stroke-dasharray': cut ? '4 6' : '0' }));
    }
    // узлы
    for (const node of f.nodes) {
      const p = pos(node.id, n);
      const role = node.alive ? node.role : 'dead';
      const g = el('g', { class: 'node ' + role, transform: `translate(${p.x},${p.y})` });
      g.append(el('circle', { class: 'body', r: 46 }));
      g.append(el('text', { class: 'big', y: -8 }, String(node.id)));
      g.append(el('text', { class: 'sub', y: 9 }, ROLE[role]));
      g.append(el('text', { class: 'sub', y: 23 }, `терм ${node.term}`));
      stage.append(g);
      if (node.group) stage.append(el('text', { class: 'group', x: p.x, y: p.y + 62, 'text-anchor': 'middle' }, 'группа ' + node.group));
    }
    // сообщения: точка бежит от отправителя к получателю; потерянные исчезают с крестиком
    let drawn = 0;
    for (const m of msgs) {
      if (m.sent > tick || m.deliver < tick - 1) continue;
      if (drawn++ > 160) break;
      const p = pos(m.from, n), q = pos(m.to, n);
      const span = Math.max(1, m.deliver - m.sent);
      let pr = Math.min(1, (tick - m.sent) / span);
      const lostAt = m.fate === 'dropped' ? 0.5 : m.fate === 'cut' ? 0.3 : 1;
      const lost = m.fate === 'dropped' || m.fate === 'cut' || m.fate === 'dead';
      if (lost && pr >= lostAt) {
        const x = p.x + (q.x - p.x) * lostAt, y = p.y + (q.y - p.y) * lostAt;
        if (tick - (m.sent + span * lostAt) < 3) stage.append(el('text', { class: 'lost', x, y: y + 5 }, '✕'));
        continue;
      }
      const c = el('circle', { class: 'msg ' + m.kind + (m.dup ? ' dup' : ''), cx: p.x + (q.x - p.x) * pr, cy: p.y + (q.y - p.y) * pr, r: m.kind === 'append' && !/Heartbeat/.test(m.text) ? 6 : 4.5 });
      const title = el('title', {}, `${m.from}→${m.to}: ${m.text}`);
      c.append(title);
      stage.append(c);
    }

    // журналы
    const logs = $('logs');
    logs.replaceChildren();
    for (const node of f.nodes) {
      const row = html('div', { class: 'logrow' });
      row.append(html('span', { class: 'who' }, 'Узел ' + node.id));
      const box = html('div', { class: 'entries', role: 'list', 'aria-label': `Журнал узла ${node.id}` });
      const { log, base } = node.full;
      if (base > 0) box.append(html('span', { class: 'skipped' }, `…${base} зап.`));
      log.forEach((e, i) => {
        const index = base + i + 1;
        const cls = `e t${e.term % 6}` + (index <= node.commit ? ' c' : '');
        const s = html('span', { class: cls, role: 'listitem', title: `индекс ${index}, терм ${e.term}: ${e.cmd}${index <= node.commit ? ' (закоммичена)' : ''}`, 'aria-label': `индекс ${index}, терм ${e.term}, ${e.cmd}` }, String(e.term));
        box.append(s);
      });
      if (log.length === 0 && base === 0) box.append(html('span', { class: 'skipped' }, 'пусто'));
      row.append(box);
      logs.append(row);
    }

    // таблица
    const tb = $('table');
    tb.replaceChildren();
    for (const node of f.nodes) {
      const tr = html('tr');
      const role = node.alive ? ROLE[node.role] : ROLE.dead;
      [node.id, role, node.term, node.commit, node.applied, node.votedFor === null || node.votedFor === undefined ? '—' : node.votedFor, node.group || '—'].forEach((v) => tr.append(html('td', {}, String(v))));
      tb.append(tr);
    }

    // события
    const feed = $('feed');
    feed.replaceChildren();
    const evs = trace.events.filter((e) => e.t <= tick).slice(-14);
    for (const e of evs) {
      const li = html('li', { class: e.text.startsWith('НАРУШЕНИЕ') ? 'bad' : '' });
      li.append(html('span', { class: 't' }, 'тик ' + e.t), document.createTextNode(e.text));
      feed.append(li);
    }
    feed.scrollTop = feed.scrollHeight;

    const leaders = f.nodes.filter((x) => x.alive && x.role === 'leader');
    $('summary').textContent = `Тик ${tick}. ` + (leaders.length ? 'Лидер: ' + leaders.map((l) => `узел ${l.id} (терм ${l.term})`).join(', ') : 'лидера нет') + `. Работает узлов: ${f.nodes.filter((x) => x.alive).length} из ${n}.`;
  }

  function step(d) {
    if (!trace) return;
    idx = Math.max(0, Math.min(trace.frames.length - 1, idx + d));
    render();
    if (idx === trace.frames.length - 1) stop();
  }

  function play() {
    if (!trace) return;
    if (idx >= trace.frames.length - 1) idx = 0;
    $('play').textContent = '⏸ Пауза';
    const speed = Number($('speed').value);
    timer = setInterval(() => step(1), 1000 / (8 * speed));
  }

  function stop() {
    if (timer) clearInterval(timer);
    timer = null;
    $('play').textContent = '▶ Играть';
  }

  $('jump').addEventListener('click', () => { stop(); idx = trace.frames.length - 1; render(); });
  $('play').addEventListener('click', () => (timer ? stop() : play()));
  $('back').addEventListener('click', () => { stop(); step(-1); });
  $('fwd').addEventListener('click', () => { stop(); step(1); });
  $('slider').addEventListener('input', (e) => { stop(); idx = Number(e.target.value); render(); });
  $('speed').addEventListener('change', () => { if (timer) { stop(); play(); } });
  document.addEventListener('keydown', (e) => {
    if (e.target instanceof HTMLInputElement || e.target instanceof HTMLSelectElement || e.target instanceof HTMLButtonElement) return;
    if (e.key === ' ') { e.preventDefault(); timer ? stop() : play(); }
    else if (e.key === 'ArrowRight') step(1);
    else if (e.key === 'ArrowLeft') step(-1);
  });

  load().catch((e) => { $('title').textContent = 'Не удалось загрузить записи: ' + e.message; });
})();
