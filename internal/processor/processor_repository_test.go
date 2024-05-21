//go:build !prod
// +build !prod

package processor

import (
	"database/sql"
	"os"
	"testing"

	"github.com/RomeroGabriel/event-process-app/pkg/eventprocess"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/suite"
)

type ProcessorRepositoryTestSuite struct {
	suite.Suite
	Db *sql.DB
}

func (suite *ProcessorRepositoryTestSuite) SetupSuite() {
	dbDriver := os.Getenv("DB_DRIVER_TEST")
	dbConnStr := os.Getenv("DB_CONNECTION_TEST")
	db, err := sql.Open(dbDriver, dbConnStr)
	if err != nil {
		panic(err)
	}
	suite.Db = db
}

func (suite *ProcessorRepositoryTestSuite) TearDownSuite() {
	suite.Db.Exec("DELETE FROM message")
	suite.Db.Close()
}

func TestSuiteProcessor(t *testing.T) {
	suite.Run(t, new(ProcessorRepositoryTestSuite))
}

func (suite *ProcessorRepositoryTestSuite) TestSaveEmptyMessage() {
	repo, err := NewProcessorRepository(suite.Db)
	suite.NoError(err)
	msgEntity := eventprocess.EventMessage{}
	err = repo.SaveMessage(msgEntity)
	suite.Error(err)
}

var eventType = "type-1"
var clientId = "client-1"
var message = "message1"
var messageId = "124141"

func (suite *ProcessorRepositoryTestSuite) TestSave() {
	repo, err := NewProcessorRepository(suite.Db)
	suite.NoError(err)
	msgEntity := eventprocess.EventMessage{
		EventType: eventType,
		ClientId:  clientId,
		Message:   message,
		MessageId: messageId,
	}
	err = repo.SaveMessage(msgEntity)
	suite.NoError(err)
}

func (suite *ProcessorRepositoryTestSuite) TestSaveInvalidClient() {
	repo, err := NewProcessorRepository(suite.Db)
	suite.NoError(err)
	msgEntity := eventprocess.EventMessage{
		EventType: eventType,
		ClientId:  "fake-client",
		Message:   message,
		MessageId: messageId,
	}
	err = repo.SaveMessage(msgEntity)
	suite.Error(err)
}

func (suite *ProcessorRepositoryTestSuite) TestSaveRepeatMessage() {
	repo, err := NewProcessorRepository(suite.Db)
	suite.NoError(err)
	msgEntity := eventprocess.EventMessage{
		EventType: eventType,
		ClientId:  "fake-client",
		Message:   message,
		MessageId: messageId,
	}
	err = repo.SaveMessage(msgEntity)
	suite.Error(err)
}
