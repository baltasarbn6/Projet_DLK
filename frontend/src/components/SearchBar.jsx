import React from "react";
import TextField from "@mui/material/TextField";

export default function SearchBar({ setSearch }) {
  return (
    <TextField
      label="Rechercher une chanson..."
      variant="outlined"
      fullWidth
      onChange={(e) => setSearch(e.target.value)}
    />
  );
}
