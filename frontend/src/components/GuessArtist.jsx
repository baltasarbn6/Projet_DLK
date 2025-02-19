import React, { useState, useEffect } from 'react';
import axios from 'axios';

export default function GuessArtistGame() {
  const [artist, setArtist] = useState('');
  const [lyricsLines, setLyricsLines] = useState([]);
  const [currentLine, setCurrentLine] = useState('');
  const [input, setInput] = useState('');
  const [artistSuggestions, setArtistSuggestions] = useState([]);
  const [score, setScore] = useState({ attempts: 0, correct: 0, errors: 0 });
  const [gameOver, setGameOver] = useState(false);

  // Charger aléatoirement les paroles d'un seul artiste
  useEffect(() => {
    axios.get('http://localhost:8000/curated').then(response => {
      const allSongs = response.data.curated_data;
      const groupedByArtist = {};

      // Grouper les chansons par artiste
      allSongs.forEach(song => {
        const artistName = song.artist.name;
        if (!groupedByArtist[artistName]) {
          groupedByArtist[artistName] = [];
        }
        const lines = song.lyrics.split('\n').filter(line => line.trim() !== '');
        groupedByArtist[artistName].push(...lines);
      });

      // Choisir un artiste aléatoire avec au moins 5 lignes
      const artistsWithLines = Object.entries(groupedByArtist).filter(([, lines]) => lines.length >= 5);
      if (artistsWithLines.length > 0) {
        const randomArtist = artistsWithLines[Math.floor(Math.random() * artistsWithLines.length)];
        const [artistName, lines] = randomArtist;

        setArtist(artistName);
        setLyricsLines(lines.sort(() => 0.5 - Math.random()).slice(0, 10));
        setCurrentLine(lines[Math.floor(Math.random() * lines.length)]);
      }
    }).catch(error => console.error("Erreur lors du chargement des paroles :", error));
  }, []);

  // Gestion des suggestions d'artistes
  const handleInputChange = (e) => {
    const query = e.target.value;
    setInput(query);
    if (query.length > 1) {
      axios.get('http://localhost:8000/curated').then(response => {
        const allArtists = [...new Set(response.data.curated_data.map(song => song.artist.name))];
        const filtered = allArtists.filter(artist => artist.toLowerCase().includes(query.toLowerCase()));
        setArtistSuggestions(filtered.slice(0, 5));
      });
    } else {
      setArtistSuggestions([]);
    }
  };

  // Sélectionner une suggestion
  const handleSuggestionClick = (suggestion) => {
    setInput(suggestion);
    setArtistSuggestions([]);
  };

  // Afficher une nouvelle ligne
  const getNextLine = () => {
    if (lyricsLines.length > 0) {
      const nextLine = lyricsLines[Math.floor(Math.random() * lyricsLines.length)];
      setCurrentLine(nextLine);
    } else {
      setGameOver(true);
    }
  };

  // Vérifier la réponse
  const handleSubmit = () => {
    const isCorrect = input.trim().toLowerCase() === artist.toLowerCase();
    setScore((prev) => ({
      attempts: prev.attempts + 1,
      correct: prev.correct + (isCorrect ? 1 : 0),
      errors: prev.errors + (isCorrect ? 0 : 1)
    }));

    if (isCorrect) {
      setGameOver(true);
    } else {
      getNextLine();
    }
    setInput('');
  };

  // Passer à la ligne suivante (compte comme erreur)
  const handlePass = () => {
    setScore((prev) => ({
      attempts: prev.attempts + 1,
      errors: prev.errors + 1,
      correct: prev.correct
    }));
    getNextLine();
  };

  // Affichage de fin
  if (gameOver) {
    return (
      <div className="random-game-container">
        <h2>🎤 Jeu Devinez l'artiste</h2>
        <p>✅ Artiste correct : {artist}</p>
        <p>❌ Erreurs : {score.errors}</p>
        <button className="restart-button" onClick={() => window.location.reload()}>🔄 Rejouer</button>
      </div>
    );
  }

  return (
    <div className="random-game-container">
      <h2>🎤 Jeu Devinez l'artiste</h2>
      <div className="lyrics-excerpt">
        🎶 {currentLine}
      </div>
      <input
        type="text"
        className="artist-input"
        placeholder="Tapez le nom de l'artiste..."
        value={input}
        onChange={handleInputChange}
      />
      {artistSuggestions.length > 0 && (
        <ul className="artist-suggestions">
          {artistSuggestions.map((suggestion, index) => (
            <li key={index} className="suggestion-item" onClick={() => handleSuggestionClick(suggestion)}>
              {suggestion}
            </li>
          ))}
        </ul>
      )}
      <div>
        <button className="submit-button" onClick={handleSubmit}>✅ Valider</button>
        <button className="restart-button" onClick={handlePass} style={{ marginLeft: '10px' }}>⏭️ Passer</button>
      </div>
    </div>
  );
}
