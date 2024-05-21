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
