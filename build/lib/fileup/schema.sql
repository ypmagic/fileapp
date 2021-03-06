DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS users;

CREATE TABLE files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    secure_file_name TEXT NOT NULL
);

CREATE TABLE users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL
);