/**
 * Tests E2E — Navigation globale
 * Vérifie que la navbar et le routage React Router fonctionnent correctement.
 * Stratégie : pyramide de tests — ces tests couvrent la couche UI (sommet).
 * AMDEC R3 (API down) : la navbar doit rester fonctionnelle même sans API.
 */
import { test, expect } from '@playwright/test'
import { mockApiOk } from './helpers.js'

test.beforeEach(async ({ page }) => {
  await mockApiOk(page)
})

test.describe('Navbar — structure et accessibilité', () => {
  test('affiche le logo ObRail Europe', async ({ page }) => {
    await page.goto('/')
    await expect(page.locator('.navbar__brand')).toContainText('ObRail Europe')
  })

  test('contient les 4 liens de navigation', async ({ page }) => {
    await page.goto('/')
    const nav = page.locator('nav[aria-label="Navigation principale"]')
    await expect(nav.getByRole('link', { name: /Accueil/i })).toBeVisible()
    await expect(nav.getByRole('link', { name: /Trajets/i })).toBeVisible()
    await expect(nav.getByRole('link', { name: /Prédiction/i })).toBeVisible()
    await expect(nav.getByRole('link', { name: /Monitoring/i })).toBeVisible()
  })

  test('le lien Accueil est actif sur "/"', async ({ page }) => {
    await page.goto('/')
    const homeLink = page.locator('.navbar__link--active')
    await expect(homeLink).toContainText('Accueil')
  })

  test('le lien de saut accessibilité est présent', async ({ page }) => {
    await page.goto('/')
    const skipLink = page.locator('a.skip-link')
    await expect(skipLink).toHaveAttribute('href', '#main-content')
  })
})

test.describe('Routage — navigation entre les pages', () => {
  test('clic sur Trajets navigue vers /trajets', async ({ page }) => {
    await page.goto('/')
    await page.locator('nav').getByRole('link', { name: /Trajets/i }).click()
    await expect(page).toHaveURL('/trajets')
    await expect(page.locator('h1')).toContainText('Trajets ferroviaires')
  })

  test('clic sur Prédiction IA navigue vers /prediction', async ({ page }) => {
    await page.goto('/')
    await page.locator('nav').getByRole('link', { name: /Prédiction/i }).click()
    await expect(page).toHaveURL('/prediction')
    await expect(page.locator('h1')).toContainText('Analyser un corridor')
  })

  test('clic sur Monitoring navigue vers /monitoring', async ({ page }) => {
    await page.goto('/')
    await page.locator('nav').getByRole('link', { name: /Monitoring/i }).click()
    await expect(page).toHaveURL('/monitoring')
    await expect(page.locator('h1')).toContainText('Monitoring')
  })

  test('clic sur Accueil depuis /trajets revient à "/"', async ({ page }) => {
    await page.goto('/trajets')
    await page.locator('nav').getByRole('link', { name: /Accueil/i }).click()
    await expect(page).toHaveURL('/')
    await expect(page.locator('#hero-title')).toBeVisible()
  })

  test('navigation directe vers /trajets charge la page correcte', async ({ page }) => {
    await page.goto('/trajets')
    await expect(page.locator('h1')).toContainText('Trajets ferroviaires')
  })

  test('navigation directe vers /prediction charge la page correcte', async ({ page }) => {
    await page.goto('/prediction')
    await expect(page.locator('h1')).toContainText('Analyser un corridor')
  })

  test('navigation directe vers /monitoring charge la page correcte', async ({ page }) => {
    await page.goto('/monitoring')
    await expect(page.locator('h1')).toContainText('Monitoring')
  })
})
