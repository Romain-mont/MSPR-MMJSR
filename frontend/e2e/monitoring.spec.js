/**
 * Tests E2E — Page /monitoring
 * Vérifie l'affichage de l'état du service, le bouton de vérification et les outils.
 * AMDEC R3 (NPR=120) : la page de monitoring doit refléter fidèlement l'état de l'API.
 */
import { test, expect } from '@playwright/test'
import { mockApiOk, mockApiDown, MOCK_HEALTH_OK } from './helpers.js'

test.describe('Monitoring — état du service (AMDEC R3)', () => {
  test('affiche le titre de la page', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/monitoring')
    await expect(page.locator('h1')).toContainText('Monitoring')
  })

  test('affiche le badge Opérationnel quand l\'API est OK', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/monitoring')
    await expect(page.locator('.monitoring__status-badge.status-ok')).toContainText('Opérationnel', { timeout: 5000 })
  })

  test('affiche le statut API ok dans les détails', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/monitoring')
    const dl = page.locator('.monitoring__details')
    await expect(dl).toContainText('ok', { timeout: 5000 })
  })

  test('affiche base de données connectée quand db=true', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/monitoring')
    await expect(page.locator('.monitoring__details')).toContainText('connectée', { timeout: 5000 })
  })

  test('affiche la version API', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/monitoring')
    await expect(page.locator('.monitoring__details')).toContainText(MOCK_HEALTH_OK.version, { timeout: 5000 })
  })

  test('affiche le badge status-error quand l\'API est indisponible', async ({ page }) => {
    await mockApiDown(page)
    await page.goto('/monitoring')
    await expect(page.locator('.monitoring__status-badge')).not.toHaveClass(/status-ok/, { timeout: 5000 })
    await expect(page.locator('.monitoring__status-badge')).toContainText('Indisponible')
  })

  test('affiche l\'heure de la dernière vérification', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/monitoring')
    // Après le chargement, lastCheck doit afficher une heure (pas "—")
    await expect(page.locator('.monitoring__details')).not.toContainText('Dernière vérification—', { timeout: 5000 })
  })
})

test.describe('Monitoring — bouton Vérifier maintenant', () => {
  test('le bouton de vérification est présent', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/monitoring')
    await expect(page.locator('button[aria-label="Vérifier l\'état maintenant"]')).toBeVisible()
  })

  test('clic sur Vérifier maintenant relance l\'appel health', async ({ page }) => {
    let callCount = 0
    await page.route('http://localhost:8000/health', route => {
      callCount++
      return route.fulfill({ contentType: 'application/json', body: JSON.stringify({ status: 'ok', db: true, version: '1.0.0' }) })
    })
    await page.goto('/monitoring')
    await page.waitForLoadState('networkidle')
    const countBeforeClick = callCount
    await page.click('button[aria-label="Vérifier l\'état maintenant"]')
    await page.waitForTimeout(500)
    expect(callCount).toBeGreaterThan(countBeforeClick)
  })

  test('affiche une alerte si l\'API répond en erreur', async ({ page }) => {
    await mockApiDown(page)
    await page.goto('/monitoring')
    await expect(page.locator('.monitoring__error[role="alert"]')).toBeVisible({ timeout: 5000 })
  })
})

test.describe('Monitoring — outils (Grafana & Prometheus)', () => {
  test.beforeEach(async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/monitoring')
  })

  test('affiche la section Outils de monitoring', async ({ page }) => {
    await expect(page.locator('#tools-title')).toContainText('Outils de monitoring')
  })

  test('affiche un lien vers Grafana', async ({ page }) => {
    const link = page.locator('a[aria-label*="Grafana"]')
    await expect(link).toBeVisible()
    await expect(link).toHaveAttribute('target', '_blank')
  })

  test('affiche un lien vers Prometheus', async ({ page }) => {
    const link = page.locator('a[aria-label*="Prometheus"]')
    await expect(link).toBeVisible()
    await expect(link).toHaveAttribute('target', '_blank')
  })
})

test.describe('Monitoring — tableau des endpoints', () => {
  test.beforeEach(async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/monitoring')
  })

  test('affiche la section Endpoints disponibles', async ({ page }) => {
    await expect(page.locator('#endpoints-title')).toContainText('Endpoints disponibles')
  })

  test('le tableau contient /health', async ({ page }) => {
    await expect(page.locator('.monitoring__endpoints table')).toContainText('/health')
  })

  test('le tableau contient /predict/substitution', async ({ page }) => {
    await expect(page.locator('.monitoring__endpoints table')).toContainText('/predict/substitution')
  })

  test('les méthodes GET et POST sont affichées', async ({ page }) => {
    await expect(page.locator('.monitoring__endpoints .badge-grey').first()).toContainText('GET')
    await expect(page.locator('.monitoring__endpoints .badge-green').first()).toContainText('POST')
  })
})
