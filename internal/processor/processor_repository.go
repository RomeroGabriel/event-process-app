package processor

import (
	"context"
	"database/sql"
	"log"
	"time"
)

type ProcessorRepository struct {
	Db *sql.DB
}

var schemaClient = `
DO $$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_tables 
      WHERE schemaname = current_schema() AND tablename = 'client'
   ) THEN
      CREATE TABLE client (
		name VARCHAR(100),
		PRIMARY KEY (name)
      );
	  INSERT INTO client (name) VALUES ('client-1'), ('client-2'), ('client-3');
   END IF;
END $$;
`

var schemaMsg = `CREATE TABLE IF NOT EXISTS Message (
		message TEXT NOT NULL,
		fk_client_name VARCHAR(100),
		CONSTRAINT fk_client
			FOREIGN KEY(fk_client_name) 
			REFERENCES client(name)
		);`

func NewProcessorRepository(db *sql.DB) (*ProcessorRepository, error) {
	err := db.Ping()
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(schemaClient)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(schemaMsg)
	if err != nil {
		return nil, err
	}
	return &ProcessorRepository{
		Db: db,
	}, nil
}

var defaultSecondTimeout = 1 * time.Second

// Fake Cache
var clientCache []string

func (r *ProcessorRepository) FindAllClient() ([]string, error) {
	if clientCache != nil {
		// Could add a len validation between clientCache and table rows
		return clientCache, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultSecondTimeout)
	defer cancel()
	rows, err := r.Db.QueryContext(ctx, "SELECT name FROM client")
	if err != nil {
		return nil, err
	}
	tAll := []string{}
	for rows.Next() {
		var td string
		if err := rows.Scan(&td); err != nil {
			return nil, err
		}
		tAll = append(tAll, td)
	}
	clientCache = tAll
	select {
	case <-ctx.Done():
		log.Println("Context Canceled on FindAllClient")
		return nil, context.Canceled
	default:
		return tAll, nil
	}
}
