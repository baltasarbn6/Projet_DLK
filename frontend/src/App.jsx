import { BrowserRouter as Router, Route, Routes } from "react-router-dom";
import Home from "./components/Home";
import ArtistList from "./components/ArtistList";
import SongDetails from "./components/SongDetails";
import ResultPage from "./components/ResultPage";

export default function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/artists" element={<ArtistList />} />
        <Route path="/song/:title" element={<SongDetails />} />
        <Route path="/result" element={<ResultPage />} />
      </Routes>
    </Router>
  );
}
