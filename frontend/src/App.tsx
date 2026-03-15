import { BrowserRouter, Routes, Route, NavLink, Navigate } from "react-router-dom";
import { useEffect, useState } from "react";
import api from "./api";
import Settings from "./pages/Settings";
import Files from "./pages/Files";
import FileDetail from "./pages/FileDetail";
import MarkerChart from "./pages/MarkerChart";
import "./App.css";

const navigationItems = [
  { to: "/charts", label: "Biomarkers" },
  { to: "/files", label: "Lab Files" },
  { to: "/settings", label: "Settings" },
];

function HomeRedirect() {
  const [target, setTarget] = useState<string | null>(null);

  useEffect(() => {
    api
      .get<string[]>("/measurements/markers")
      .then((r) => setTarget(r.data.length > 0 ? "/charts" : "/files"))
      .catch(() => setTarget("/files"));
  }, []);

  if (!target) return null;
  return <Navigate to={target} replace />;
}

function App() {
  return (
    <BrowserRouter>
      <div className="app">
        <nav className="sidebar">
          <h1 className="logo">
            <img className="logo-mark" src="/favicon.svg" alt="" aria-hidden="true" />
            <span>Health Dashboard</span>
          </h1>
          {navigationItems.map((item) => (
            <NavLink key={item.to} to={item.to}>
              {item.label}
            </NavLink>
          ))}
        </nav>
        <main className="content">
          <Routes>
            <Route path="/" element={<HomeRedirect />} />
            <Route path="/files" element={<Files />} />
            <Route path="/files/:id" element={<FileDetail />} />
            <Route path="/charts" element={<MarkerChart />} />
            <Route path="/settings" element={<Settings />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;
