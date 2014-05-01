package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Stat retrieves the statistics for a bucket
func Stat(path, name string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fatal(err)
		return
	}

	db, err := bolt.Open(path, 0600)
	if err != nil {
		fatal(err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		// Find bucket.
		b := tx.Bucket([]byte(name))
		if b == nil {
			fatalf("bucket not found: %s", name)
			return nil
		}

		// Get stat
                stats := b.Stat()
                println("Branch Page Count:  ", stats.BranchPageCount)
                println("Leaf Page Count:    ", stats.LeafPageCount)
                println("Overflow Page Count:", stats.OverflowPageCount)
                println("Key Count:          ", stats.KeyCount)
                println("Max Depth:          ", stats.MaxDepth)
                return nil
	})
	if err != nil {
		fatal(err)
		return
	}
}
