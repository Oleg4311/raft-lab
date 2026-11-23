import { defineConfig, devices } from '@playwright/test';

// BASE_URL задаётся для проверки опубликованной версии, иначе просмотрщик отдаётся локально из web/.
const live = process.env.BASE_URL;

export default defineConfig({
  testDir: 'e2e',
  fullyParallel: true,
  reporter: [['list']],
  use: { baseURL: live ?? 'http://127.0.0.1:4176/' },
  projects: [
    { name: 'desktop', use: { ...devices['Desktop Chrome'] } },
    { name: 'mobile', use: { ...devices['Pixel 7'] } },
  ],
  webServer: live ? undefined : { command: 'node scripts/serve.mjs', url: 'http://127.0.0.1:4176/', reuseExistingServer: !process.env.CI, timeout: 60_000 },
});
