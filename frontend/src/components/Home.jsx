import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import axios from "axios";

export default function Home() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);

  useEffect(() => {
    if (query.length > 2) {
      axios.get("http://localhost:8000/curated").then((response) => {
        const songs = response.data.curated_data;

        // Filtrer par titre ou artiste
        const filteredResults = songs.filter(song =>
          song.title.toLowerCase().includes(query.toLowerCase()) ||
          song.artist.toLowerCase().includes(query.toLowerCase())
        );

        setResults(filteredResults);
      });
    } else {
      setResults([]);
    }
  }, [query]);

  return (
    <div className="home-container">
      <h1 className="title">🎵 Lyrics Challenge 🎵</h1>
      <p className="subtitle">Testez vos connaissances en paroles de chansons !</p>
      <input
        type="text"
        placeholder="Rechercher une chanson ou un artiste..."
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        className="search-bar"
      />
      <ul className="result-list">
        {results.map((item, index) => (
          <li key={index}>
            <Link to={`/song/${encodeURIComponent(item.title)}`}>{item.title} - {item.artist}</Link>
          </li>
        ))}
      </ul>
    </div>
  );
}
