import React, { useState, useEffect } from 'react';
import axios from 'axios';

export default function DecadeLanguageGame() {
  const [decade, setDecade] = useState('1970');
  const [language, setLanguage] = useState('fr');
  const [gameStarted, setGameStarted] = useState(false);
  const [songs, setSongs] = useState([]);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [currentLyrics, setCurrentLyrics] = useState([]);
  const [userInput, setUserInput] = useState('');
  const [suggestions, setSuggestions] = useState([]);
  const [score, setScore] = useState(0);
  const [attempts, setAttempts] = useState(0);
  const [gameOver, setGameOver] = useState(false);

  // Récupérer les chansons filtrées par décennie et langue
  const startGame = () => {
    axios.get('http://localhost:8000/curated').then(response => {
      const filteredSongs = response.data.curated_data.filter(song => {
        const releaseYear = new Date(song.release_date).getFullYear();
        const decadeStart = parseInt(decade);
        return (
          releaseYear >= decadeStart &&
          releaseYear < decadeStart + 10 &&
          song.language === language
        );
      });

      const uniqueSongs = Array.from(new Set(filteredSongs.map(s => s.title)))
        .map(title => filteredSongs.find(song => song.title === title))
        .slice(0, 10);

      setSongs(uniqueSongs);
      if (uniqueSongs.length > 0) {
        setCurrentLyrics(getRandomExcerpt(uniqueSongs[0].lyrics));
      }
      setGameStarted(true);
    }).catch(error => console.error('Erreur de récupération des données :', error));
  };

  // Extraire un bloc aléatoire de 5 lignes
  const getRandomExcerpt = (lyrics) => {
    const lines = lyrics.split('\n').filter(line => line.trim() !== '');
    if (lines.length === 0) return ['🔇 Aucune parole disponible.'];
    const startIndex = Math.max(0, Math.floor(Math.random() * Math.max(1, lines.length - 5)));
    return lines.slice(startIndex, startIndex + 5);
  };

  // Gérer la saisie de l'utilisateur et afficher les suggestions
  const handleInputChange = (e) => {
    const query = e.target.value;
    setUserInput(query);
    if (query.length > 1) {
      const matchingTitles = songs.map(song => song.title).filter(title =>
        title.toLowerCase().includes(query.toLowerCase())
      );
      setSuggestions(matchingTitles.slice(0, 5));
    } else {
      setSuggestions([]);
    }
  };

  // Gérer la sélection d'une suggestion
  const handleSuggestionClick = (suggestion) => {
    setUserInput(suggestion);
    setSuggestions([]);
  };

  // Vérifier la réponse et passer à l'extrait suivant
  const handleSubmit = () => {
    if (songs.length === 0) return;

    const correctTitle = songs[currentIndex].title;
    setAttempts(attempts + 1);
    if (userInput.toLowerCase() === correctTitle.toLowerCase()) {
      setScore(score + 1);
    }

    if (currentIndex < songs.length - 1) {
      setCurrentIndex(currentIndex + 1);
      setCurrentLyrics(getRandomExcerpt(songs[currentIndex + 1].lyrics));
      setUserInput('');
    } else {
      setGameOver(true);
    }
  };

  // Passer à l'extrait suivant
  const handlePass = () => {
    setAttempts(attempts + 1);
    if (currentIndex < songs.length - 1) {
      setCurrentIndex(currentIndex + 1);
      setCurrentLyrics(getRandomExcerpt(songs[currentIndex + 1].lyrics));
      setUserInput('');
    } else {
      setGameOver(true);
    }
  };

  if (!gameStarted) {
    return (
      <div className="random-game-container">
        <h2>📅 Jeu par décennie/langue</h2>
        <p>🎯 Règle : Trouvez le titre de la chanson à partir d'un extrait de 5 lignes de paroles, en fonction de la décennie et de la langue choisies.</p>
        <label>
          🕒 Décennie :
          <select value={decade} onChange={(e) => setDecade(e.target.value)}>
            <option value="1970">1970s</option>
            <option value="1980">1980s</option>
            <option value="1990">1990s</option>
            <option value="2000">2000s</option>
            <option value="2010">2010s</option>
            <option value="2020">2020s</option>
          </select>
        </label>
        <br />
        <label>
          🌐 Langue :
          <select value={language} onChange={(e) => setLanguage(e.target.value)}>
            <option value="fr">Français</option>
            <option value="en">Anglais</option>
            <option value="es">Espagnol</option>
          </select>
        </label>
        <br />
        <button className="submit-button" onClick={startGame}>🎯 Jouer</button>
      </div>
    );
  }

  if (gameOver) {
    return (
      <div className="random-game-container">
        <h2>🎉 Fin du jeu !</h2>
        <p>🎯 Score final : {score}/10</p>
        <button className="restart-button" onClick={() => window.location.reload()}>🔄 Rejouer</button>
      </div>
    );
  }

  return (
    <div className="random-game-container">
      <h2>📅 Jeu par décennie/langue</h2>
      <div className="lyrics-excerpt">
        {currentLyrics.map((line, index) => (
          <p key={index}>🎶 {line}</p>
        ))}
      </div>
      <input
        type="text"
        className="artist-input"
        placeholder="Tapez le titre..."
        value={userInput}
        onChange={handleInputChange}
      />
      {suggestions.length > 0 && (
        <ul className="artist-suggestions">
          {suggestions.map((suggestion, index) => (
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
