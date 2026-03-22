import { BrowserRouter, HashRouter, Routes, Route, NavLink, Navigate } from "react-router-dom";
import { useEffect, useState } from "react";
import { fetchMarkerNames } from "./api";
import { getShareExportBundle, isShareExportMode } from "./export/runtime";
import Settings from "./pages/Settings";
import Files from "./pages/Files";
import FileDetail from "./pages/FileDetail";
import MarkerChart from "./pages/MarkerChart";
import Medications from "./pages/Medications";
import Search from "./pages/Search";
import "./App.css";

const navigationItems = [
  { to: "/charts", label: "Biomarkers" },
  { to: "/meds", label: "Meds", hideInShareExport: true },
  { to: "/search", label: "Search" },
  { to: "/files", label: "Lab Files" },
  { to: "/settings", label: "Settings" },
];

function HomeRedirect() {
  const [target, setTarget] = useState<string | null>(null);

  useEffect(() => {
    fetchMarkerNames()
      .then((markerNames) => setTarget(markerNames.length > 0 ? "/charts" : "/files"))
      .catch(() => setTarget("/files"));
  }, []);

  if (!target) return null;
  return <Navigate to={target} replace />;
}

function App() {
  const shareExportMode = isShareExportMode();
  const shareExport = getShareExportBundle();
  const Router = shareExportMode ? HashRouter : BrowserRouter;
  const visibleNavigationItems = navigationItems.filter((item) => !shareExportMode || !item.hideInShareExport);

  return (
    <Router>
      <div className="app">
        <nav className="sidebar">
          <h1 className="logo">
            {!shareExportMode && <img className="logo-mark" src="/favicon.svg" alt="" aria-hidden="true" />}
            <span>Health Dashboard</span>
          </h1>
          {visibleNavigationItems.map((item) => (
            <NavLink key={item.to} to={item.to}>
              {item.label}
            </NavLink>
          ))}
        </nav>
        <main className="content">
          {shareExportMode && shareExport && (
            <div className="card" style={{ marginBottom: "1rem", padding: "0.9rem 1rem" }}>
              <strong>Shareable snapshot.</strong>{" "}
              Opened from a single exported HTML file created{" "}
              {new Date(shareExport.exported_at).toLocaleString()}. Browsing and search work locally,
              but uploads, reprocessing, and summaries are disabled.
            </div>
          )}
          <Routes>
            <Route path="/" element={<HomeRedirect />} />
            <Route path="/files" element={<Files />} />
            <Route path="/files/:id" element={<FileDetail />} />
            <Route path="/charts" element={<MarkerChart />} />
            <Route path="/meds" element={<Medications />} />
            <Route path="/search" element={<Search />} />
            <Route path="/settings" element={<Settings />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
