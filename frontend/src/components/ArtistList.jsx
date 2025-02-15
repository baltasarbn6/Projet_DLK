import { useState, useEffect } from "react";
import { Link, useSearchParams } from "react-router-dom";
import axios from "axios";

export default function ArtistList() {
  const [artists, setArtists] = useState([]);
  const [searchParams] = useSearchParams();
  const query = searchParams.get("search");

  useEffect(() => {
    axios.get("http://localhost:8000/curated").then((response) => {
      const filteredArtists = response.data.curated_data.filter(song =>
        song.artist.toLowerCase().includes(query.toLowerCase())
      ).map(song => song.artist);
      setArtists([...new Set(filteredArtists)]);
    });
  }, [query]);

  return (
    <div className="list-container">
      <h2>Résultats pour "{query}"</h2>
      <ul className="artist-list">
        {artists.map((artist) => (
          <li key={artist}>
            <Link to={`/songs?search=${encodeURIComponent(artist)}`}>{artist}</Link>
          </li>
        ))}
      </ul>
    </div>
  );
}
