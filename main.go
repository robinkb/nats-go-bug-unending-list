package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Number of objects that this test will put into the store.
const numObjects = 50

func main() {
	tempDir, err := os.MkdirTemp("", "jetstream")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tempDir)
	log.Println("created tempdir:", tempDir)

	opts := &server.Options{
		JetStream: true,
		StoreDir:  tempDir,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		log.Fatal(err)
	}

	go ns.Start()
	defer ns.Shutdown()

	if !ns.ReadyForConnections(4 * time.Second) {
		log.Fatal("server not ready for connections")
	}
	log.Println("started nats server at:", ns.ClientURL())

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("initialized jetstream client")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	config := jetstream.ObjectStoreConfig{
		Bucket: "list-hangs-after-delete",
	}
	store, err := js.CreateObjectStore(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("created object store:", config.Bucket)

	rootDirectory := "/" + randomFilename(int64(8+rand.Intn(8)))
	parentDirectory := rootDirectory + "/" + randomFilename(int64(8+rand.Intn(8)))
	for i := 0; i < numObjects; i++ {
		childObject := parentDirectory + "/" + randomFilename(int64(8+rand.Intn(8)))
		_, err := store.PutBytes(ctx, childObject, randomContents(32))
		log.Println("put object:", childObject)
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Println("placed random objects in store")

	objects, err := store.List(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("listed objects and found", len(objects))
	if len(objects) != numObjects {
		log.Fatal("unexpected number of objects found:", len(objects))
	}

	log.Println("deleting objects, leaving one in the store")
	for i := 0; i < len(objects)-1; i++ {
		err := store.Delete(ctx, objects[i].Name)
		log.Println("deleted object:", objects[i].Name)
		if err != nil {
			log.Println(err)
		}
	}
	log.Println("finished deleting objects in the store, except one")

	log.Println("listing objects in the store with one left; this will return")
	objects, err = store.List(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if len(objects) != 1 {
		log.Fatal("unexpected number of objects returned")
	}

	log.Println("deleting last object")
	err = store.Delete(ctx, objects[0].Name)
	log.Println("deleted object:", objects[0].Name)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("attempting to list objects in the empty store; this won't return")
	_, err = store.List(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("unexpectedly, it DID return...")
}

var (
	filenameChars  = []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	separatorChars = []byte("._-")
	randomBytes    = make([]byte, 128<<20)
)

func randomFilename(length int64) string {
	b := make([]byte, length)
	wasSeparator := true
	for i := range b {
		if !wasSeparator && i < len(b)-1 && rand.Intn(4) == 0 {
			b[i] = separatorChars[rand.Intn(len(separatorChars))]
			wasSeparator = true
		} else {
			b[i] = filenameChars[rand.Intn(len(filenameChars))]
			wasSeparator = false
		}
	}
	return string(b)
}

func randomContents(length int64) []byte {
	return randomBytes[:length]
}
