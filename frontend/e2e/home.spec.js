/**
 * Tests E2E — Page d'accueil (/)
 * Vérifie le hero, les KPIs, les cartes modules et l'état de l'API.
 * AMDEC R3 (NPR=120) : indicateur API doit refléter l'état réel du service.
 */
import { test, expect } from '@playwright/test'
import { mockApiOk, mockApiDown, MOCK_STATS } from './helpers.js'

test.describe('Home — hero section', () => {
  test.beforeEach(async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/')
  })

  test('affiche le titre principal', async ({ page }) => {
    await expect(page.locator('#hero-title')).toBeVisible()
    await expect(page.locator('#hero-title')).toContainText("Remplacer l'avion")
  })

  test('affiche le badge MSPR EPSI', async ({ page }) => {
    await expect(page.locator('.hero__badge')).toContainText('Bloc E6.3')
  })

  test('affiche la pilule API opérationnelle', async ({ page }) => {
    await expect(page.locator('.hero__api-pill.pill-ok')).toBeVisible({ timeout: 5000 })
    await expect(page.locator('.hero__api-pill.pill-ok')).toContainText('API opérationnelle')
  })

  test('affiche les 3 KPIs du hero', async ({ page }) => {
    const kpis = page.locator('.hero__kpi')
    await expect(kpis).toHaveCount(3)
    await expect(kpis.first()).toContainText('corridors analysés')
  })

  test('affiche le bouton Explorer les trajets', async ({ page }) => {
    const cta = page.locator('.hero__actions .btn-primary')
    await expect(cta).toContainText('Explorer les trajets')
    await expect(cta).toHaveAttribute('href', '/trajets')
  })

  test('clic sur Explorer les trajets navigue vers /trajets', async ({ page }) => {
    await page.locator('.hero__actions .btn-primary').click()
    await expect(page).toHaveURL('/trajets')
  })

  test('affiche le lien Tester la prédiction IA', async ({ page }) => {
    await expect(page.locator('.hero__cta-ghost')).toContainText('Tester la prédiction IA')
  })
})

test.describe('Home — section modules', () => {
  test.beforeEach(async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/')
  })

  test('affiche exactement 3 cartes de modules', async ({ page }) => {
    await expect(page.locator('.module-card')).toHaveCount(3)
  })

  test('la carte Trajets ferroviaires est présente', async ({ page }) => {
    const card = page.locator('.module-card').filter({ hasText: 'Trajets ferroviaires' })
    await expect(card).toBeVisible()
    await expect(card).toContainText('Explorer les trajets')
  })

  test('la carte Prédiction IA est présente', async ({ page }) => {
    const card = page.locator('.module-card').filter({ hasText: 'Prédiction IA' })
    await expect(card).toBeVisible()
  })

  test('la carte Monitoring est présente', async ({ page }) => {
    const card = page.locator('.module-card').filter({ hasText: 'Monitoring' })
    await expect(card).toBeVisible()
  })

  test('clic sur carte Trajets navigue vers /trajets', async ({ page }) => {
    await page.locator('.module-card').filter({ hasText: 'Trajets ferroviaires' }).click()
    await expect(page).toHaveURL('/trajets')
  })
})

test.describe('Home — statistiques en direct', () => {
  test('affiche les stats globales renvoyées par l\'API', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/')
    // Les stats doivent être chargées depuis l'API mockée
    const total = MOCK_STATS.global.total_trajets.toLocaleString('fr-FR')
    await expect(page.locator('.stat-big__num').first()).toContainText(total, { timeout: 5000 })
  })

  test('affiche le tableau de répartition par type de train', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/')
    await expect(page.locator('.live-stats__table table')).toBeVisible({ timeout: 5000 })
    await expect(page.locator('.live-stats__table table thead th').first()).toContainText('Type de train')
  })
})

test.describe('Home — barre santé API (AMDEC R3)', () => {
  test('health-bar affiche le statut ok quand API répond', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/')
    const bar = page.locator('.health-bar')
    await expect(bar).toBeVisible({ timeout: 5000 })
    await expect(bar).toContainText('ok')
  })

  test('health-bar contient le lien vers monitoring', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/')
    const link = page.locator('.health-bar__link')
    await expect(link).toBeVisible()
    await expect(link).toHaveAttribute('href', '/monitoring')
  })

  test('pilule API passe à pill-down quand API est indisponible', async ({ page }) => {
    await mockApiDown(page)
    await page.goto('/')
    await expect(page.locator('.hero__api-pill')).not.toHaveClass(/pill-ok/, { timeout: 5000 })
  })
})
