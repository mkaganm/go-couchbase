package main

import (
	"fmt"
	"log"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
)

func main() {
	// Connect to the Couchbase cluster
	cluster, err := gocb.Connect(
		"couchbase://localhost",
		gocb.ClusterOptions{
			Username: "admin",
			Password: "password",
		},
	)
	if err != nil {
		log.Fatalf("Failed to connect to Couchbase: %v", err)
	}
	fmt.Println("Successfully connected to Couchbase!")

	// Select the bucket
	bucket := cluster.Bucket("example_bucket")
	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatalf("Bucket is not ready: %v", err)
	}

	// Select the default collection
	collection := bucket.DefaultCollection()

	// List of users to insert
	users := []map[string]interface{}{
		{"name": "Ahmet", "email": "ahmet@example.com", "age": 30, "active": true},
		{"name": "Mehmet", "email": "mehmet@example.com", "age": 28, "active": true},
		{"name": "Ayşe", "email": "ayse@example.com", "age": 25, "active": false},
	}

	// Insert multiple users with auto-generated IDs
	documentIDs := []string{}
	for _, user := range users {
		documentID := fmt.Sprintf("user::%s", uuid.New().String())
		documentIDs = append(documentIDs, documentID)

		_, err := collection.Upsert(documentID, user, nil)
		if err != nil {
			log.Fatalf("Failed to insert data (%s): %v", documentID, err)
		}
		fmt.Printf("Document inserted: %s\n", documentID)
	}

	// Update a specific user using one of the generated IDs
	documentIDToUpdate := documentIDs[0] // First inserted document
	updatedData := map[string]interface{}{
		"name":   "Ahmet Updated",
		"email":  "ahmet_updated@example.com",
		"age":    31,
		"active": true,
	}
	_, err = collection.Upsert(documentIDToUpdate, updatedData, nil)
	if err != nil {
		log.Fatalf("Failed to update document (%s): %v", documentIDToUpdate, err)
	}
	fmt.Printf("Document updated: %s\n", documentIDToUpdate)

	// Read all users using the generated IDs
	fmt.Println("Reading all documents:")
	for _, documentID := range documentIDs {
		result, err := collection.Get(documentID, nil)
		if err != nil {
			fmt.Printf("Document not found (%s): %v\n", documentID, err)
			continue
		}
		var fetchedUser map[string]interface{}
		err = result.Content(&fetchedUser)
		if err != nil {
			log.Fatalf("Failed to decode document content (%s): %v", documentID, err)
		}
		fmt.Printf("Document read (%s): %v\n", documentID, fetchedUser)
	}
}
