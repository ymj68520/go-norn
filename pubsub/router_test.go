/**
  @author: decision
  @date: 2024/1/3
  @note:
**/

package pubsub

import (
	"net/http"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestEventRouter_HandleConnect(t *testing.T) {
	var store EventStore
	router := NewRouter(nil, store)
	http.HandleFunc("/subscribe", router.HandleConnect)
	log.Fatal(http.ListenAndServe("localhost:8888", nil))
}
