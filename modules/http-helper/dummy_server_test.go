package http_helper

import (
	"fmt"
	"net"
	"testing"

	"github.com/gruntwork-io/terratest/modules/random"
	"github.com/stretchr/testify/assert"
)

func TestRunDummyServer(t *testing.T) {
	t.Parallel()

	uniqueId := random.UniqueId()
	text := fmt.Sprintf("dummy-server-%s", uniqueId)

	listener, port := RunDummyServer(t, text)
	defer shutDownServer(t, listener)

	url := fmt.Sprintf("http://localhost:%d", port)
	HttpGetWithValidation(t, url, 200, text)
}

func shutDownServer(t *testing.T, listener net.Listener) {
	err := listener.Close()
	assert.NoError(t, err)
}
