package main

import (
	"database/sql"
	"io"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

const DIR = "Les-codes-en-vigueur" // https://github.com/legifrance/Les-codes-en-vigueur

func main() {
	entries, err := os.ReadDir(DIR)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite3", "./legifrance.sqlite3?_journal=WAL&_timeout=5000&_fk=true")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`
	CREATE TABLE documents (filename TEXT NOT NULL, content_text TEXT NOT NULL);

	CREATE VIRTUAL TABLE documents_fts USING fts5 (
		filename,
		content_text,
		content = documents,
		content_rowid = rowid,
		tokenize = "trigram"
	  );

	CREATE TRIGGER documents_insert
	AFTER
	INSERT
	ON documents BEGIN
	INSERT INTO
	documents_fts(rowid, filename, content_text)
	VALUES
	(new.rowid, new.filename, new.content_text);

	END;

	CREATE TRIGGER documents_delete
	AFTER
	DELETE ON documents BEGIN
	INSERT INTO
	documents_fts(documents_fts, rowid, filename, content_text)
	VALUES
	('delete', old.rowid, old.filename, old.content_text);

	END;

	CREATE TRIGGER documents_update
	AFTER
	UPDATE
	ON documents BEGIN
	INSERT INTO
	documents_fts(documents_fts, rowid, filename, content_text)
	VALUES
	('delete', new.rowid, new.filename, new.content_txt);

	INSERT INTO
	documents_fts(rowid, filename, content_text)
	VALUES
	(new.rowid, new.filename, new.content_text);

	END;
	`)
	if err != nil {
		panic(err)
	}

	for _, e := range entries {
		f, err := os.Open(filepath.Join(DIR, e.Name()))
		if err != nil {
			panic(err)
		}
		text, err := io.ReadAll(f)
		if err != nil {
			panic(err)
		}

		_, err = db.Exec("INSERT INTO documents(filename, content_text) VALUES (?, ?)", e.Name(), string(text))
		if err != nil {
			panic(err)
		}

		err = f.Close()
		if err != nil {
			panic(err)
		}
	}

	_, err = db.Exec(`INSERT INTO documents_fts(documents_fts) VALUES ('optimize');`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`pragma page_size = 65536; VACUUM INTO 'optimized.external.legifrance.65536.sqlite3'`)
	if err != nil {
		panic(err)
	}
}
