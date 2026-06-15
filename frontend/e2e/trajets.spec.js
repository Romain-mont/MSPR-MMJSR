/**
 * Tests E2E — Page /trajets
 * Vérifie l'affichage de la liste, les filtres et les états vides.
 * AMDEC R2 (NPR=81) : corruption DB → message d'erreur affiché, pas de crash.
 */
import { test, expect } from '@playwright/test'
import { mockApiOk, MOCK_TRAJETS } from './helpers.js'

test.describe('Trajets — affichage initial', () => {
  test.beforeEach(async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/trajets')
  })

  test('affiche le titre de la page', async ({ page }) => {
    await expect(page.locator('h1')).toContainText('Trajets ferroviaires')
  })

  test('affiche le formulaire de filtres', async ({ page }) => {
    await expect(page.locator('form[aria-label="Filtres de recherche"]')).toBeVisible()
  })

  test('affiche les champs Gare de départ et d\'arrivée', async ({ page }) => {
    await expect(page.locator('label[for="origine"]')).toContainText('Gare de départ')
    await expect(page.locator('label[for="destination"]')).toContainText("Gare d'arrivée")
  })

  test('affiche le sélecteur de substituabilité', async ({ page }) => {
    const select = page.locator('select#substituable')
    await expect(select).toBeVisible()
    await expect(select.locator('option[value="true"]')).toContainText('Substituables')
    await expect(select.locator('option[value="false"]')).toContainText('Non substituables')
  })

  test('affiche le tableau avec les bonnes colonnes', async ({ page }) => {
    const headers = page.locator('table thead th')
    await expect(headers).toHaveCount(6)
    await expect(headers.first()).toContainText('Origine')
  })

  test('affiche les trajets mockés', async ({ page }) => {
    const rows = page.locator('table tbody tr')
    await expect(rows).toHaveCount(MOCK_TRAJETS.length, { timeout: 5000 })
  })

  test('affiche les badges de substituabilité', async ({ page }) => {
    await expect(page.locator('.badge-green').first()).toContainText('Oui')
    await expect(page.locator('.badge-red').first()).toContainText('Non')
  })

  test('affiche le compteur de résultats', async ({ page }) => {
    await expect(page.locator('.trajets__count')).toContainText('2', { timeout: 5000 })
  })
})

test.describe('Trajets — filtres de recherche', () => {
  test('peut saisir une gare de départ dans le champ origine', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/trajets')
    await page.fill('input#origine', 'Paris')
    await expect(page.locator('input#origine')).toHaveValue('Paris')
  })

  test('peut saisir une gare d\'arrivée dans le champ destination', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/trajets')
    await page.fill('input#destination', 'Lyon')
    await expect(page.locator('input#destination')).toHaveValue('Lyon')
  })

  test('peut soumettre le formulaire de recherche', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/trajets')
    await page.fill('input#origine', 'Paris')
    await page.click('button[type="submit"]')
    // La soumission relance l'API — les trajets sont toujours affichés
    await expect(page.locator('table tbody tr')).toHaveCount(MOCK_TRAJETS.length, { timeout: 5000 })
  })

  test('le bouton Réinitialiser vide les champs', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/trajets')
    await page.fill('input#origine', 'Paris')
    await page.fill('input#destination', 'Lyon')
    await page.click('button:has-text("Réinitialiser")')
    await expect(page.locator('input#origine')).toHaveValue('')
    await expect(page.locator('input#destination')).toHaveValue('')
  })

  test('filtre par substituable change la sélection du select', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/trajets')
    await page.selectOption('select#substituable', 'true')
    await expect(page.locator('select#substituable')).toHaveValue('true')
  })
})

const API = 'http://localhost:8000'
const HEALTH_OK = { status: 'ok', db: true, version: '1.0.0' }

test.describe('Trajets — état vide (AMDEC R4)', () => {
  test('affiche "Aucun résultat" quand la liste est vide', async ({ page }) => {
    await page.route(`${API}/health`, route =>
      route.fulfill({ contentType: 'application/json', body: JSON.stringify(HEALTH_OK) })
    )
    await page.route(`${API}/trajets**`, route =>
      route.fulfill({ contentType: 'application/json', body: '[]' })
    )
    await page.goto('/trajets')
    await expect(page.locator('.trajets__empty')).toContainText('Aucun résultat', { timeout: 5000 })
  })

  test('affiche une alerte en cas d\'erreur API', async ({ page }) => {
    await page.route(`${API}/health`, route =>
      route.fulfill({ contentType: 'application/json', body: JSON.stringify(HEALTH_OK) })
    )
    await page.route(`${API}/trajets**`, route =>
      route.fulfill({ status: 503, contentType: 'application/json', body: '{}' })
    )
    await page.goto('/trajets')
    await expect(page.locator('[role="alert"]')).toBeVisible({ timeout: 5000 })
  })
})

test.describe('Trajets — bouton retour', () => {
  test('le bouton retour est présent sur la page', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/trajets')
    await expect(page.locator('.trajets__topbar')).toBeVisible()
  })
})
