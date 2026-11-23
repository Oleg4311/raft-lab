import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';

async function openScenario(page: Page, name: string): Promise<void> {
  await page.goto('./');
  await page.locator(`button[data-name="${name}"]`).click();
  await expect(page.locator('body')).toHaveAttribute('data-loaded', name);
}

const gotoTick = async (page: Page, i: number): Promise<void> => {
  await page.evaluate((t) => {
    const s = document.getElementById('slider') as HTMLInputElement;
    s.value = String(Math.min(Number(s.max), t));
    s.dispatchEvent(new Event('input'));
  }, i);
};

test('на странице девять записей: три сценария и шесть пойманных ошибок', async ({ page }) => {
  await page.goto('./');
  await expect(page.locator('#tabs button')).toHaveCount(9);
  await expect(page.locator('#tabs button.bug')).toHaveCount(6);
  await expect(page.locator('#tabs button[aria-pressed="true"]')).toHaveText('Обычная работа');
  expect(Number(await page.locator('#slider').getAttribute('max'))).toBeGreaterThan(100);
});

test('воспроизведение идёт вперёд, пауза останавливает, шаги и клавиши работают', async ({ page }) => {
  await page.goto('./');
  await expect(page.locator('#tick')).toHaveText('тик 0');
  await page.locator('#speed').selectOption('4');
  await page.locator('#play').click();
  await expect(page.locator('#play')).toContainText('Пауза');
  await expect.poll(async () => Number((await page.locator('#slider').inputValue()))).toBeGreaterThan(10);
  await page.locator('#play').click();
  const at = await page.locator('#slider').inputValue();
  await page.waitForTimeout(400);
  expect(await page.locator('#slider').inputValue()).toBe(at);
  await page.locator('#fwd').click();
  expect(Number(await page.locator('#slider').inputValue())).toBe(Number(at) + 1);
  await page.locator('#back').click();
  await page.locator('#back').click();
  expect(Number(await page.locator('#slider').inputValue())).toBe(Number(at) - 1);
  await page.locator('body').click({ position: { x: 5, y: 5 } });
  await page.keyboard.press('ArrowRight');
  expect(Number(await page.locator('#slider').inputValue())).toBe(Number(at));
});

test('в обычной работе к концу записи ровно один лидер и журналы совпадают', async ({ page }) => {
  await openScenario(page, 'normal');
  await gotoTick(page, 10_000);
  const rows = await page.locator('#table tr').allInnerTexts();
  expect(rows.filter((r) => r.includes('лидер')).length).toBe(1);
  const logs = await page.locator('#logs .entries').allInnerTexts();
  expect(new Set(logs.map((l) => l.replace(/\s+/g, ''))).size).toBeLessThanOrEqual(2); // у последователей возможна лишь отстающая на запись копия
  await expect(page.locator('#summary')).toContainText('Лидер: узел');
});

test('при разделении сети видно двух «лидеров» в разных термах и красные разрывы', async ({ page }) => {
  await openScenario(page, 'partition');
  await gotoTick(page, 130);
  const rows = await page.locator('#table tr').allInnerTexts();
  expect(rows.filter((r) => r.includes('лидер')).length).toBe(2);
  const terms = rows.filter((r) => r.includes('лидер')).map((r) => Number(r.split('\t')[2]));
  expect(new Set(terms).size).toBe(2);
  await expect(page.locator('#stage line[stroke="var(--bad)"]').first()).toBeVisible();
  await expect(page.locator('#stage text.group').first()).toContainText('группа');
  await gotoTick(page, 10_000);
  expect((await page.locator('#table tr').allInnerTexts()).filter((r) => r.includes('лидер')).length).toBe(1); // после починки лидер один
});

test('падение лидера: узел не работает, потом возвращается и догоняет', async ({ page }) => {
  await openScenario(page, 'leader-crash');
  await gotoTick(page, 120);
  await expect(page.locator('#table')).toContainText('не работает');
  await gotoTick(page, 10_000);
  await expect(page.locator('#table')).not.toContainText('не работает');
  const commits = (await page.locator('#table tr').allInnerTexts()).map((r) => r.split('\t')[3]);
  expect(new Set(commits).size).toBeLessThanOrEqual(2);
});

test('сценарий с ошибкой показывает нарушение, кнопка ведёт к моменту нарушения', async ({ page }) => {
  await openScenario(page, 'bug-commitprevioustermbycount');
  await expect(page.locator('#violation')).toContainText('LeaderCompleteness');
  await page.locator('#jump').click();
  await expect(page.locator('#feed .bad').first()).toContainText('НАРУШЕНИЕ LeaderCompleteness');
  await openScenario(page, 'normal');
  await expect(page.locator('#violation')).toBeHidden();
  await expect(page.locator('#jump')).toBeHidden();
});

test('каждый сценарий с ошибкой называет вид нарушения', async ({ page }) => {
  const expected: Record<string, string> = {
    'bug-nouptodatecheck': 'LeaderCompleteness',
    'bug-truncatealways': 'CommittedPrefix',
    'bug-localreads': 'Linearizability',
    'bug-nodedup': 'Linearizability',
    'bug-commitprevioustermbycount': 'LeaderCompleteness',
    'bug-votenotpersisted': 'ElectionSafety',
  };
  for (const [name, kind] of Object.entries(expected)) {
    await openScenario(page, name);
    await expect(page.locator('#violation')).toContainText(kind);
  }
});

test('записи не противоречат сами себе: терм узла не убывает, потерянные сообщения имеют причину', async ({ page }) => {
  await page.goto('./');
  const result = await page.evaluate(async () => {
    const problems: string[] = [];
    for (const name of ['normal', 'leader-crash', 'partition']) {
      const t = await (await fetch(`traces/${name}.json`)).json();
      const last: number[] = [];
      for (const f of t.frames) for (const n of f.nodes) {
        if (last[n.id] !== undefined && n.term < last[n.id]) problems.push(`${name}: терм узла ${n.id} убыл на тике ${f.t}`);
        last[n.id] = n.term;
      }
      for (const m of t.messages) if (!['delivered', 'dropped', 'cut', 'dead', 'inflight'].includes(m.fate)) problems.push(`${name}: сообщение ${m.id} с судьбой ${m.fate}`);
      if (t.violation) problems.push(`${name}: в корректном сценарии записано нарушение`);
    }
    return problems;
  });
  expect(result).toEqual([]);
});

test('доступность без серьёзных нарушений, в том числе в тёмной теме', async ({ page }) => {
  for (const scheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await openScenario(page, 'partition');
    expect((await new AxeBuilder({ page }).analyze()).violations.map((v) => v.id)).toEqual([]); // нулевой тик: журналы пусты
    await gotoTick(page, 130);
    expect((await new AxeBuilder({ page }).analyze()).violations.map((v) => v.id)).toEqual([]);
    await openScenario(page, 'bug-nodedup');
    expect((await new AxeBuilder({ page }).analyze()).violations.map((v) => v.id)).toEqual([]);
  }
});

test('нет горизонтальной прокрутки', async ({ page }) => {
  await openScenario(page, 'bug-truncatealways');
  await gotoTick(page, 30);
  expect(await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth)).toBeLessThanOrEqual(0);
});
