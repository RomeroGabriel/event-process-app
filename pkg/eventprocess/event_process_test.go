//go:build !prod
// +build !prod

package eventprocess

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventMessageEmpty(t *testing.T) {
	result := EventMessage{}
	err := result.ValidateEmptyFields()
	assert.Error(t, err)
}

func TestMessageEmpty(t *testing.T) {
	msg := EventMessage{
		EventType: "user-app",
		ClientId:  "client-3",
		Message:   "Nice msg",
	}
	err := msg.ValidateEmptyFields()
	assert.NoError(t, err)
}

func TestEventEmptyMessageEmpty(t *testing.T) {
	msg := EventMessage{
		EventType: "",
		ClientId:  "client-3",
		Message:   "Nice msg",
	}
	err := msg.ValidateEmptyFields()
	assert.Error(t, err)
}

func TestClientEmptyMessageEmpty(t *testing.T) {
	msg := EventMessage{
		EventType: "user-app",
		ClientId:  "",
		Message:   "Nice msg",
	}
	err := msg.ValidateEmptyFields()
	assert.Error(t, err)
}

func TestMessageEmptyMessageEmpty(t *testing.T) {
	msg := EventMessage{
		EventType: "user-app",
		ClientId:  "client-3",
		Message:   "",
	}
	err := msg.ValidateEmptyFields()
	assert.Error(t, err)
}
