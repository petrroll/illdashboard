import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import Files from "./pages/Files";
import FileDetail from "./pages/FileDetail";
import MarkerChart from "./pages/MarkerChart";
import "./App.css";

function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <nav className="sidebar">
          <h1 className="logo">🩺 Health Dashboard</h1>
          <NavLink to="/" end>
            Dashboard
          </NavLink>
          <NavLink to="/files">Lab Files</NavLink>
          <NavLink to="/charts">Biomarkers</NavLink>
        </nav>
        <main className="content">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/files" element={<Files />} />
            <Route path="/files/:id" element={<FileDetail />} />
            <Route path="/charts" element={<MarkerChart />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;
