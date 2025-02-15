import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import axios from "axios";

export default function SongDetails() {
  const { title } = useParams();
  const [song, setSong] = useState(null);
  const [difficulty, setDifficulty] = useState("");
  const [userAnswers, setUserAnswers] = useState({});
  const navigate = useNavigate();

  useEffect(() => {
    axios.get("http://localhost:8000/curated").then((response) => {
      setSong(response.data.curated_data.find(s => s.title === title) || null);
    });
  }, [title]);

  if (!song) return <div>Chargement...</div>;

  const lyricsArray = song.difficulty_versions[difficulty]?.split("\n") || [];

  const handleChange = (index, value) => {
    setUserAnswers({ ...userAnswers, [index]: value });
  };

  return (
    <div className="song-container">
      <h2>{song.title} - {song.artist}</h2>
      <select value={difficulty} onChange={(e) => setDifficulty(e.target.value)}>
        <option value="">Sélectionner une difficulté</option>
        {Object.keys(song.difficulty_versions).map(level => (
          <option key={level} value={level}>{level.charAt(0).toUpperCase() + level.slice(1)}</option>
        ))}
      </select>
      {difficulty && (
        <button onClick={() => navigate("/result", { state: { userAnswers, song, difficulty } })}>
          Jouer
        </button>
      )}
      {difficulty && (
        <div className="lyrics-container">
          {lyricsArray.map((line, index) => (
            <p key={index}>
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
          <button onClick={() => navigate("/result", { state: { userAnswers, song, difficulty } })}>
            Fin du jeu
          </button>
        </div>
      )}
    </div>
  );
}
