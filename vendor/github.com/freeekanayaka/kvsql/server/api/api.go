package api

import (
	"net/http"

	"github.com/freeekanayaka/kvsql/db"
	"github.com/freeekanayaka/kvsql/server/membership"
)

func New(localNodeAddress string, db *db.DB, membership *membership.Membership, changes chan *db.KeyValue, subscribe SubcribeFunc) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/dqlite", dqliteHandleFunc(localNodeAddress))
	mux.HandleFunc("/watch", watchHandleFunc(db, changes, subscribe))
	mux.Handle("/cluster", clusterHandler(membership))
	return mux
}
