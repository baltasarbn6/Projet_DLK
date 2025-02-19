import { useState, useEffect } from "react";
import { Link, useNavigate } from "react-router-dom";
import axios from "axios";

export default function Home() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const navigate = useNavigate();

  useEffect(() => {
    if (query.length > 2) {
      axios.get("http://localhost:8000/curated").then((response) => {
        const songs = response.data.curated_data;

        // Filtrer par titre ou artiste
        const filteredResults = songs.filter(song =>
          song.title.toLowerCase().includes(query.toLowerCase()) ||
          song.artist.name.toLowerCase().includes(query.toLowerCase())
        );

        setResults(filteredResults);
      }).catch((error) => {
        console.error("Erreur lors de la récupération des données :", error);
      });
    } else {
      setResults([]);
    }
  }, [query]);

  // Regrouper les chansons par artiste
  const groupedSongs = results.reduce((acc, song) => {
    const artistName = song.artist.name;
    if (!acc[artistName]) {
      acc[artistName] = { 
        artistName: artistName, 
        artistImage: song.artist.image_url, 
        songs: [] 
      };
    }
    acc[artistName].songs.push(song);
    return acc;
  }, {});

  // Vérifier si le query correspond au nom d'un artiste
  const isArtistQuery = query.length > 2 && Object.values(groupedSongs).some(group =>
    group.artistName.toLowerCase().includes(query.toLowerCase())
  );

  return (
    <div className="home-container">
      <h1 className="title">🎵 Lyrics Challenge 🎵</h1>
      <p className="subtitle">Testez vos connaissances en paroles de chansons !</p>

      <div className="search-container">
        <input
          type="text"
          placeholder="🔍 Rechercher une chanson ou un artiste..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="styled-search-bar"
        />
      </div>

      <div className="button-container">
        <button className="home-button" onClick={() => navigate("/random-game")}>🎯 Jeu d'extraits aléatoires</button>
        <button className="home-button" onClick={() => navigate("/guess-artist")}>🎤 Devinez l'artiste</button>
        <button className="home-button" onClick={() => navigate("/game/decade-language")}>📅 Jeu par décennie/langue</button>
        <button className="home-button">🌍 Traduction mystère</button>
      </div>
      
      <ul className="result-list">
        {Object.values(groupedSongs).map((group, index) => (
          <div key={index}>
            {/* Affiche l'artiste uniquement si la recherche correspond à un artiste */}
            {isArtistQuery && (
              <li className="artist-item">
                <img src={group.artistImage} alt={group.artistName} className="song-image-right" />
                <Link to={`/artist/${encodeURIComponent(group.artistName)}`} className="song-link">
                  🎤 {group.artistName}
                </Link>
              </li>
            )}

            {/* Afficher les chansons dans tous les cas */}
            {group.songs.map((song, idx) => (
              <li key={`${song.title}-${idx}`} className="song-item">
                <div className="song-info">
                  <Link to={`/song/${encodeURIComponent(song.title)}`} className="song-link">
                    {song.title}
                  </Link>
                </div>
                <img src={song.image_url} alt={song.title} className="song-image-right" />
              </li>
            ))}
          </div>
        ))}
      </ul>
    </div>
  );
}
