import React, { useState, useEffect } from "react";
import axios from "axios";
import SearchBar from "./components/SearchBar";
import SongList from "./components/SongList";
import LyricsGame from "./components/LyricsGame";

export default function App() {
  const [search, setSearch] = useState("");
  const [songs, setSongs] = useState([]);
  const [selectedSong, setSelectedSong] = useState(null);

  useEffect(() => {
    if (search.length > 2) {
      axios.get(`http://localhost:8000/curated`)
        .then((res) => {
          console.log("Réponse API:", res.data);
          if (Array.isArray(res.data.curated_data)) {
            const filtered = res.data.curated_data.filter((song) =>
              song.title.toLowerCase().includes(search.toLowerCase())
            );
            console.log("Résultats filtrés:", filtered);
            setSongs(filtered);
          } else {
            console.error("Format inattendu:", res.data);
          }
        })
        .catch((err) => console.error("Erreur API:", err));
    }
  }, [search]);
  
  return (
    <div className="p-4 space-y-4">
      <SearchBar setSearch={setSearch} />
      <SongList songs={songs} onSelect={setSelectedSong} />
      {selectedSong && <LyricsGame song={selectedSong} />}
    </div>
  );
}
