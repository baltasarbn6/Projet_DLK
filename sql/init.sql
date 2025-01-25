CREATE TABLE IF NOT EXISTS artists (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    bio TEXT,
    s3_image_key VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS songs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    artist_id INT,
    title VARCHAR(255) NOT NULL,
    url VARCHAR(255),
    s3_image_key VARCHAR(255),
    lyrics TEXT,
    FOREIGN KEY (artist_id) REFERENCES artists(id)
);
