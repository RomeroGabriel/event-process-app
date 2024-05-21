package processor

import (
	"context"
	"database/sql"

	"github.com/RomeroGabriel/event-process-app/pkg/eventprocess"
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

var schemaMsg = `CREATE TABLE IF NOT EXISTS message (
		message_id TEXT NOT NULL,
		message TEXT NOT NULL,
		event_type TEXT NOT NULL,
		fk_client_name VARCHAR(100),
		CONSTRAINT fk_client
			FOREIGN KEY(fk_client_name) 
			REFERENCES client(name),
		PRIMARY KEY (message_id)
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

func (r *ProcessorRepository) SaveMessage(msg eventprocess.EventMessage) error {
	_, err := r.Db.ExecContext(
		context.Background(),
		"INSERT INTO message (message_id, message, event_type, fk_client_name) VALUES ($1, $2, $3, $4);",
		msg.MessageId,
		msg.Message,
		msg.EventType,
		msg.ClientId)
	return err
}
