package main

import (
	"os"

	"github.com/boltdb/bolt"
)

// Stat retrieves the statistics for a bucket
func Stat(path, name string, all bool) {
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
                stats := b.Stats()
                println("Key Count:          ", stats.KeyN)
                println("Depth:              ", stats.Depth)

                if all {
                    println()
                    println("Branch Pages:          ", stats.BranchPageN)
                    println("Branch Overflow Pages: ", stats.BranchOverflowN)
                    println("Leaf Pages:            ", stats.LeafPageN)
                    println("Leaf Overflow Pages:   ", stats.LeafOverflowN)
                    println()
                    println("Branch Allocated Memory: ", stats.BranchAlloc)
                    println("Branch Memory In Use   : ", stats.BranchInuse)
                    println("Leaf Allocated Memory:   ", stats.LeafAlloc)
                    println("Leaf Memory In Use     : ", stats.LeafInuse)
                }

                return nil
	})
	if err != nil {
		fatal(err)
		return
	}
}
