import React, { useState } from "react";
import { Select, MenuItem, Button } from "@mui/material";

export default function LyricsGame({ song }) {
  const [difficulty, setDifficulty] = useState("easy");
  const [answers, setAnswers] = useState({});

  const handleChange = (index, value) => {
    setAnswers({ ...answers, [index]: value });
  };

  const checkAnswers = () => {
    const words = song.difficulty_versions[difficulty].split(" ");
    let correct = 0;
    Object.keys(answers).forEach((key) => {
      if (words[key] === answers[key]) {
        correct++;
      }
    });
    alert(`Score: ${correct}/${Object.keys(answers).length}`);
  };

  return (
    <div>
      <h2>{song.title} - {song.artist}</h2>
      <Select value={difficulty} onChange={(e) => setDifficulty(e.target.value)}>
        <MenuItem value="easy">Facile</MenuItem>
        <MenuItem value="medium">Moyen</MenuItem>
        <MenuItem value="hard">Difficile</MenuItem>
      </Select>
      <div className="mt-4 p-4 border rounded-md">
        {song.difficulty_versions[difficulty].split("\n").map((line, lineIndex) => (
          <div key={lineIndex} className="mb-2">
            {line.split(/(\s+)/).map((word, index) => // Garde les espaces et les sauts de ligne
              word === "____" ? (
                <input
                  key={`${lineIndex}-${index}`}
                  type="text"
                  className="border-b-2 border-gray-500 mx-1 w-16"
                  value={answers[`${lineIndex}-${index}`] || ""}
                  onChange={(e) => handleChange(`${lineIndex}-${index}`, e.target.value)}
                />
              ) : (
                <span key={`${lineIndex}-${index}`} className="mx-1">{word}</span>
              )
            )}
          </div>
        ))}
      </div>
      <Button variant="contained" color="primary" onClick={checkAnswers} className="mt-4">
        Vérifier
      </Button>
    </div>
  );
}
