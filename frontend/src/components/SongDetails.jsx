import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import axios from "axios";

export default function SongDetails() {
  const { title } = useParams();
  const [song, setSong] = useState(null);
  const [difficulty, setDifficulty] = useState("");
  const [userAnswers, setUserAnswers] = useState({});
  const [showLyrics, setShowLyrics] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    axios.get("http://localhost:8000/curated").then((response) => {
      setSong(response.data.curated_data.find((s) => s.title === title) || null);
    });
  }, [title]);

  if (!song) return <div>Chargement...</div>;

  const lyricsArray = song.difficulty_versions[difficulty]?.split("\n") || [];

  const handleChange = (index, value) => {
    setUserAnswers({ ...userAnswers, [index]: value });
  };

  const descriptions = {
    facile: "Trouvez les paroles manquantes dans des extraits simples.",
    medium: "Trouvez les paroles manquantes dans des extraits de difficulté moyenne.",
    difficile: "Trouvez les paroles manquantes dans des extraits complexes."
  };

  return (
    <div className="song-container">
      <div className="song-header">
        <img src={song.image_url} alt={song.title} className="song-image" />
        <div className="song-info">
          <h2 className="song-title">{song.title} - {song.artist.name}</h2>
          <p className="release-date">📅 {new Date(song.release_date).toLocaleDateString('fr-FR')}</p>
        </div>
      </div>
      {!showLyrics && (
  <div className="difficulty-selection">
    <select
      className="difficulty-dropdown"
      value={difficulty}
      onChange={(e) => {
        setDifficulty(e.target.value);
        setShowLyrics(false);
      }}
    >
      <option value="">Sélectionner une difficulté</option>
      {Object.keys(song.difficulty_versions).map((level) => (
        <option key={level} value={level}>
          {level.charAt(0).toUpperCase() + level.slice(1)}
        </option>
      ))}
    </select>
    {difficulty && (
      <div className="difficulty-description">
        {descriptions[difficulty.toLowerCase()] || "Description non disponible."}
      </div>
    )}
    {difficulty && !showLyrics && (
      <button className="play-button" onClick={() => setShowLyrics(true)}>
        Jouer
      </button>
    )}
  </div>
)}

      {showLyrics && (
        <div className="lyrics-container">
          {lyricsArray.map((line, index) => (
            <p key={index} className="lyrics-line">
              {line.split(" ").map((word, i) =>
                word === "____" ? (
                  <input
                    key={i}
                    type="text"
                    value={userAnswers[`${index}-${i}`] || ""}
                    onChange={(e) => handleChange(`${index}-${i}`, e.target.value)}
                    className="word-input"
                  />
                ) : (
                  word + " "
                )
              )}
            </p>
          ))}
          <button
            className="finish-button"
            onClick={() => navigate("/result", { state: { userAnswers, song, difficulty } })}
          >
            Fin du jeu
          </button>
        </div>
      )}
    </div>
  );
}
