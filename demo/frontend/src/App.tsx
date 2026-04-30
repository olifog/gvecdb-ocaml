import { Route, Routes } from "react-router-dom";
import ExplorerPage from "@/pages/ExplorerPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<ExplorerPage />} />
      <Route path="/explorer/:nodeId" element={<ExplorerPage />} />
    </Routes>
  );
}
