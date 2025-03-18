import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Home from './pages/Home'
import Settings from './pages/Settings'
import DataQuality from './pages/DataQuality'

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/settings" element={<Settings />} />
        <Route path="/data-quality" element={<DataQuality />} />
      </Routes>
    </Layout>
  )
} 