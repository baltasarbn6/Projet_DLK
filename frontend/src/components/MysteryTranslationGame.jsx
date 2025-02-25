import React, { useState, useEffect } from 'react';
import axios from 'axios';

export default function MysteryTranslationGame() {
  const [songs, setSongs] = useState([]);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [currentLyrics, setCurrentLyrics] = useState([]);
  const [userInput, setUserInput] = useState('');
  const [suggestions, setSuggestions] = useState([]);
  const [score, setScore] = useState(0);
  const [gameOver, setGameOver] = useState(false);

  useEffect(() => {
    axios.get('http://localhost:8000/curated').then((response) => {
      const { songs } = response.data.curated_data;

      // Filtrer uniquement les chansons ayant des traductions françaises valides
      const translatedSongs = songs.filter(
        (song) => song.french_lyrics && song.french_lyrics !== 'Paroles indisponibles' && song.language !== 'fr'
      );

      // Sélectionner 10 chansons de manière aléatoire
      const shuffledSongs = translatedSongs.sort(() => 0.5 - Math.random()).slice(0, 10);
      setSongs(shuffledSongs);
      if (shuffledSongs.length > 0) {
        setCurrentLyrics(getRandomExcerpt(shuffledSongs[0].french_lyrics));
      }
    }).catch(error => console.error('Erreur lors de la récupération des données :', error));
  }, []);

  // Extraire un bloc aléatoire de 15 lignes
  const getRandomExcerpt = (lyrics) => {
    const lines = lyrics.split('\n').filter(line => line.trim() !== '');
    if (lines.length === 0) return ['Aucune parole disponible.'];
    const startIndex = Math.max(0, Math.floor(Math.random() * Math.max(1, lines.length - 15)));
    return lines.slice(startIndex, startIndex + 15);
  };

  const handleInputChange = (e) => {
    const query = e.target.value;
    setUserInput(query);
    if (query.length > 1) {
      const matchingTitles = songs
        .map(song => song.title)
        .filter(title =>
          title.toLowerCase().includes(query.toLowerCase())
        );
      setSuggestions(matchingTitles.slice(0, 5));
    } else {
      setSuggestions([]);
    }
  };

  const handleSuggestionClick = (suggestion) => {
    setUserInput(suggestion);
    setSuggestions([]);
  };

  const handleSubmit = () => {
    if (songs.length === 0) return;

    const correctTitle = songs[currentIndex].title;
    const isCorrect = userInput.toLowerCase() === correctTitle.toLowerCase();

    if (isCorrect) {
      setScore(score + 1);
      alert(`✅ Bonne réponse ! Le titre original était bien "${correctTitle}".`);
    } else {
      alert(`❌ Mauvaise réponse. Le bon titre original était : "${correctTitle}".`);
    }

    if (currentIndex < songs.length - 1) {
      setCurrentIndex(currentIndex + 1);
      setCurrentLyrics(getRandomExcerpt(songs[currentIndex + 1].french_lyrics));
      setUserInput('');
    } else {
      setGameOver(true);
    }
  };

  return (
    <div className="random-game-container">
      {gameOver ? (
        <>
          <h2>🎉 Fin du jeu !</h2>
          <p>🎯 Score final : {score}/{songs.length}</p>
          <button className="restart-button" onClick={() => window.location.reload()}>🔄 Rejouer</button>
        </>
      ) : (
        <>
          <h2>🌍 Jeu de Traduction Mystère</h2>
          <div className="lyrics-excerpt">
            <p>🎶 Extrait traduit en français :</p>
            {currentLyrics.map((line, index) => (
              <p key={index}>{line}</p>
            ))}
          </div>
          <input
            type="text"
            className="artist-input"
            placeholder="Tapez le titre original..."
            value={userInput}
            onChange={handleInputChange}
          />
          {suggestions.length > 0 && (
            <ul className="artist-suggestions">
              {suggestions.map((suggestion, index) => (
                <li key={index} onClick={() => handleSuggestionClick(suggestion)}>
                  {suggestion}
                </li>
              ))}
            </ul>
          )}
          <button className="submit-button" onClick={handleSubmit}>✅ Valider</button>
        </>
      )}
    </div>
  );
}
