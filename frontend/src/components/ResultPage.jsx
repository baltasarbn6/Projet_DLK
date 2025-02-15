import { useLocation, useNavigate } from "react-router-dom";

export default function ResultPage() {
  const { state } = useLocation();
  const { userAnswers, song, difficulty } = state;
  const navigate = useNavigate();

  const originalLyrics = song.lyrics.split(/\s+/); // Paroles originales mot par mot
  const maskedLyrics = song.difficulty_versions[difficulty].split("\n"); // Version masquée

  let missingWords = [];
  let originalWordIndex = 0;

  // **1. EXTRAIRE LES MOTS CACHÉS AU BON ENDROIT**
  maskedLyrics.forEach(line => {
    line.split(" ").forEach(word => {
      if (word === "____" && originalWordIndex < originalLyrics.length) {
        missingWords.push(originalLyrics[originalWordIndex].replace(/[,.;!?]/g, "")); // Supprimer la ponctuation
      }
      originalWordIndex++;
    });
  });

  let correctWords = 0;
  let wordIndex = 0;

  // **2. GÉNÉRER LA CORRECTION AVEC COULEURS ET ESPACEMENTS**
  const correctedLyrics = maskedLyrics.map((line, index) => (
    <p key={index}>
      {line.split(" ").map((word, i) => {
        if (word === "____") {
          const userInput = (userAnswers[`${index}-${i}`] || "").replace(/[,.;!?]/g, "");
          const correctWord = (missingWords[wordIndex] || "").replace(/[,.;!?]/g, ""); // Supprimer la ponctuation
          wordIndex++;

          const normalizedUserInput = userInput.trim().toLowerCase();
          const normalizedCorrectWord = correctWord.trim().toLowerCase();

          if (normalizedUserInput === normalizedCorrectWord) {
            correctWords++;
            return (
              <span key={i} style={{ color: "green", fontWeight: "bold", marginRight: "5px" }}>
                {userInput}
              </span>
            );
          } else {
            return (
              <span key={i} style={{ color: "red", fontWeight: "bold", marginRight: "5px" }}>
                {userInput || "____"} <span style={{ color: "gray" }}>({correctWord})</span>
              </span>
            );
          }
        } else {
          return <span key={i} style={{ color: "black", marginRight: "5px" }}>{word} </span>;
        }
      })}
    </p>
  ));

  // **3. CALCUL DU SCORE SUR LES SEULS MOTS CACHÉS**
  const score = missingWords.length > 0 ? Math.round((correctWords / missingWords.length) * 100) : 0;

  return (
    <div className="result-container">
      <h2>Résultat : {score}% correct</h2>
      <button onClick={() => navigate("/")}>Rejouer</button>
      <h3>Correction :</h3>
      <div>{correctedLyrics}</div>
    </div>
  );
}
