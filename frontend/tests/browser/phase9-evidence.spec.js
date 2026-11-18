import {test,expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';

test('generated Phase 9 matrix and measured performance render without manual result controls',async({page})=>{
  const evidence=JSON.parse(await readFile('../order-service/src/main/resources/evidence/test-runs.json','utf8'));
  expect(evidence.verification.matrix.scenarios).toHaveLength(22);
  expect(evidence.verification.performance.result).toBe('PASS');
  await page.route('**/auth/login',route=>route.fulfill({json:{accessToken:'browser-test-only',roles:['ADMIN'],userId:'11111111-1111-4111-8111-111111111111'}}));
  await page.route('**/api/dashboard/overview/stats',route=>route.fulfill({status:503,json:{code:'DEPENDENCY_UNAVAILABLE'}}));
  await page.route('**/api/dashboard/test-runs',route=>route.fulfill({json:evidence}));
  await page.goto('/');
  await page.getByLabel('Email').fill('admin@example.test');
  await page.getByLabel('Password',{exact:true}).fill('browser-test-only');
  await page.getByRole('button',{name:'Sign in'}).click();
  await page.locator('[data-page="5"]').click();
  await expect(page.getByText('Measured performance',{exact:true})).toBeVisible();
  await expect(page.getByText('Recovery verification matrix',{exact:true})).toBeVisible();
  for(const scenario of evidence.verification.matrix.scenarios)await expect(page.getByRole('cell',{name:scenario.scenarioId,exact:true})).toBeVisible();
  const p95=Number(evidence.verification.performance.httpMilliseconds.p95).toFixed(2)+' ms';
  await expect(page.getByRole('cell',{name:p95,exact:true})).toBeVisible();
  await expect(page.locator('input,select,textarea')).toHaveCount(0);
  await page.screenshot({path:'../docs/evidence/phase9-evidence.png',fullPage:true});
});
