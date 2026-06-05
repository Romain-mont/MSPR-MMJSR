import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Navbar     from './components/Navbar'
import Home       from './pages/Home'
import Trajets    from './pages/Trajets'
import Prediction from './pages/Prediction'
import Monitoring from './pages/Monitoring'
import './index.css'

export default function App() {
  return (
    <BrowserRouter>
      <a href="#main-content" className="skip-link">Aller au contenu principal</a>
      <Navbar />
      <Routes>
        <Route path="/"           element={<Home />} />
        <Route path="/trajets"    element={<Trajets />} />
        <Route path="/prediction" element={<Prediction />} />
        <Route path="/monitoring" element={<Monitoring />} />
      </Routes>
    </BrowserRouter>
  )
}
