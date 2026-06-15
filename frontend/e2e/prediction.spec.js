/**
 * Tests E2E — Page /prediction
 * Vérifie le formulaire de prédiction, la validation et l'affichage des résultats.
 * AMDEC R1 (NPR=252, CRITIQUE) : le formulaire doit rendre le résultat ML intelligible.
 * AMDEC R4 (NPR=80) : les entrées invalides doivent déclencher un message d'erreur.
 */
import { test, expect } from '@playwright/test'
import { mockApiOk, MOCK_PREDICT_OK, MOCK_PREDICT_NO } from './helpers.js'

test.describe('Prediction — formulaire', () => {
  test.beforeEach(async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/prediction')
  })

  test('affiche le titre de la page', async ({ page }) => {
    await expect(page.locator('h1')).toContainText('Analyser un corridor')
  })

  test('affiche la description du formulaire', async ({ page }) => {
    await expect(page.locator('.pred__header p')).toBeVisible()
  })

  test('le champ Distance est présent et requis', async ({ page }) => {
    const input = page.locator('input#distance')
    await expect(input).toBeVisible()
    await expect(input).toHaveAttribute('required')
  })

  test('affiche le sélecteur de type de train', async ({ page }) => {
    await expect(page.locator('select#vtype')).toBeVisible()
  })

  test('le champ CO₂ avion est optionnel', async ({ page }) => {
    const input = page.locator('input#co2avion')
    await expect(input).toBeVisible()
    await expect(input).not.toHaveAttribute('required')
  })

  test('le bouton Analyser est présent', async ({ page }) => {
    await expect(page.locator('button.pred__submit')).toContainText('Analyser le corridor')
  })
})

test.describe('Prediction — validation des entrées (AMDEC R4)', () => {
  test('soumettre avec distance nulle affiche un message d\'erreur React', async ({ page }) => {
    // distance=0 passe la validation HTML (champ non vide) mais échoue la validation JS (distVal <= 0)
    await mockApiOk(page)
    await page.goto('/prediction')
    await page.fill('input#distance', '0')
    await page.click('button.pred__submit')
    await expect(page.locator('[role="alert"]')).toContainText('distance', { timeout: 3000 })
  })

  test('soumettre sans distance ne soumet pas le formulaire', async ({ page }) => {
    // Le navigateur bloque via l'attribut required avant d'appeler handleSubmit
    await mockApiOk(page)
    await page.goto('/prediction')
    await page.click('button.pred__submit')
    // On reste sur la page et aucun verdict n'est affiché
    await expect(page).toHaveURL('/prediction')
    await expect(page.locator('.pred__verdict')).not.toBeVisible()
  })

  test('soumettre avec distance invalide affiche une erreur', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/prediction')
    await page.fill('input#distance', '-50')
    await page.click('button.pred__submit')
    await expect(page.locator('[role="alert"]')).toBeVisible({ timeout: 3000 })
  })

  test('distance valide permet la soumission', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/prediction')
    await page.fill('input#distance', '450')
    await page.click('button.pred__submit')
    // Le bouton passe en état "loading"
    await expect(page.locator('button.pred__submit')).toContainText(/Analyse|Analyser/, { timeout: 3000 })
  })
})

test.describe('Prediction — résultat substituable (AMDEC R1)', () => {
  test.beforeEach(async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/prediction')
    await page.fill('input#distance', '450')
    await page.click('button.pred__submit')
    await expect(page.locator('.pred__verdict')).toBeVisible({ timeout: 10_000 })
  })

  test('affiche le verdict de substitution possible', async ({ page }) => {
    await expect(page.locator('.pred__verdict')).toContainText('Substitution possible')
  })

  test('la carte verdict a la classe verdict-yes', async ({ page }) => {
    await expect(page.locator('.pred__verdict')).toHaveClass(/verdict-yes/)
  })

  test('affiche le gain CO₂ économisé', async ({ page }) => {
    await expect(page.locator('.pred__co2')).toBeVisible()
    await expect(page.locator('.pred__co2-saving-value')).toContainText('kg économisés')
  })

  test('affiche la comparaison avion vs train', async ({ page }) => {
    await expect(page.locator('.pred__co2-bar--avion')).toBeVisible()
    await expect(page.locator('.pred__co2-bar--train')).toBeVisible()
  })

  test('affiche la latence de la prédiction', async ({ page }) => {
    await expect(page.locator('.pred__latency')).toContainText('ms')
  })

  test('affiche la section Pourquoi cette prédiction', async ({ page }) => {
    await expect(page.locator('.pred__why')).toBeVisible()
  })

  test('affiche le profil du corridor (cluster)', async ({ page }) => {
    await expect(page.locator('.pred__cluster')).toBeVisible()
  })

  test('affiche le bouton Réinitialiser après le résultat', async ({ page }) => {
    await expect(page.locator('button:has-text("Réinitialiser")')).toBeVisible()
  })
})

const API_BASE = 'http://localhost:8000'
const HEALTH_MOCK = { status: 'ok', db: true, version: '1.0.0' }

test.describe('Prediction — résultat non substituable', () => {
  test('affiche le verdict non recommandé', async ({ page }) => {
    await page.route(`${API_BASE}/health`, route =>
      route.fulfill({ contentType: 'application/json', body: JSON.stringify(HEALTH_MOCK) })
    )
    await page.route(`${API_BASE}/trajets**`, route =>
      route.fulfill({ contentType: 'application/json', body: '[]' })
    )
    await page.route(`${API_BASE}/predict/**`, route =>
      route.fulfill({ contentType: 'application/json', body: JSON.stringify(MOCK_PREDICT_NO) })
    )
    await page.goto('/prediction')
    await page.fill('input#distance', '1200')
    await page.click('button.pred__submit')
    await expect(page.locator('.pred__verdict')).toBeVisible({ timeout: 10_000 })
    await expect(page.locator('.pred__verdict')).toContainText('non recommandée')
    await expect(page.locator('.pred__verdict')).toHaveClass(/verdict-no/)
  })
})

test.describe('Prediction — erreur API', () => {
  test('affiche une erreur si l\'API de prédiction échoue', async ({ page }) => {
    await page.route(`${API_BASE}/health`, route =>
      route.fulfill({ contentType: 'application/json', body: JSON.stringify(HEALTH_MOCK) })
    )
    await page.route(`${API_BASE}/trajets**`, route =>
      route.fulfill({ contentType: 'application/json', body: '[]' })
    )
    await page.route(`${API_BASE}/predict/**`, route =>
      route.fulfill({ status: 500, contentType: 'application/json', body: '{}' })
    )
    await page.goto('/prediction')
    await page.fill('input#distance', '450')
    await page.click('button.pred__submit')
    await expect(page.locator('[role="alert"]')).toBeVisible({ timeout: 10_000 })
  })
})

test.describe('Prediction — réinitialisation', () => {
  test('Réinitialiser efface le résultat et les champs', async ({ page }) => {
    await mockApiOk(page)
    await page.goto('/prediction')
    await page.fill('input#distance', '450')
    await page.click('button.pred__submit')
    await expect(page.locator('.pred__verdict')).toBeVisible({ timeout: 10_000 })
    await page.click('button:has-text("Réinitialiser")')
    await expect(page.locator('.pred__verdict')).not.toBeVisible()
    await expect(page.locator('input#distance')).toHaveValue('')
  })
})
