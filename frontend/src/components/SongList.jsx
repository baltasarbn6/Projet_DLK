import React, { useState, useEffect } from "react";
import { Card, CardContent, CardMedia, Typography } from "@mui/material";

const getImageUrl = async (imageKey) => {
  try {
    const response = await fetch(`http://localhost:8000/get-s3-url/?file_key=${imageKey}`);
    const data = await response.json();
    return data.url;
  } catch (error) {
    console.error("Erreur de récupération de l'image S3:", error);
    return "https://via.placeholder.com/150"; // Image par défaut en cas d'erreur
  }
};

export default function SongList({ songs, onSelect }) {
  const [imageUrls, setImageUrls] = useState({});

  useEffect(() => {
    const fetchImages = async () => {
      const urls = {};
      for (const song of songs) {
        urls[song.image_key] = await getImageUrl(song.image_key);
      }
      setImageUrls(urls);
    };
    fetchImages();
  }, [songs]);

  return (
    <div className="grid grid-cols-3 gap-4">
      {songs.map((song) => (
        <Card key={song.title} onClick={() => onSelect(song)} style={{ cursor: "pointer" }}>
          <CardMedia component="img" height="140" image={imageUrls[song.image_key] || "https://via.placeholder.com/150"} alt={song.title} />
          <CardContent>
            <Typography variant="h6">{song.title}</Typography>
            <Typography color="textSecondary">{song.artist}</Typography>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
