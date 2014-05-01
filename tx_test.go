package bolt

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that committing a closed transaction returns an error.
func TestTx_Commit_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.CreateBucket([]byte("foo"))
		assert.NoError(t, tx.Commit())
		assert.Equal(t, tx.Commit(), ErrTxClosed)
	})
}

// Ensure that rolling back a closed transaction returns an error.
func TestTx_Rollback_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		assert.NoError(t, tx.Rollback())
		assert.Equal(t, tx.Rollback(), ErrTxClosed)
	})
}

// Ensure that committing a read-only transaction returns an error.
func TestTx_Commit_ReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(false)
		assert.Equal(t, tx.Commit(), ErrTxNotWritable)
	})
}

// Ensure that a transaction can retrieve a cursor on the root bucket.
func TestTx_Cursor(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.CreateBucket([]byte("woojits"))
			c := tx.Cursor()

			k, v := c.First()
			assert.Equal(t, "widgets", string(k))
			assert.Nil(t, v)

			k, v = c.Next()
			assert.Equal(t, "woojits", string(k))
			assert.Nil(t, v)

			k, v = c.Next()
			assert.Nil(t, k)
			assert.Nil(t, v)

			return nil
		})
	})
}

// Ensure that creating a bucket with a read-only transaction returns an error.
func TestTx_CreateBucket_ReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.View(func(tx *Tx) error {
			b, err := tx.CreateBucket([]byte("foo"))
			assert.Nil(t, b)
			assert.Equal(t, ErrTxNotWritable, err)
			return nil
		})
	})
}

// Ensure that creating a bucket on a closed transaction returns an error.
func TestTx_CreateBucket_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.Commit()
		b, err := tx.CreateBucket([]byte("foo"))
		assert.Nil(t, b)
		assert.Equal(t, ErrTxClosed, err)
	})
}

// Ensure that a Tx can retrieve a bucket.
func TestTx_Bucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			b := tx.Bucket([]byte("widgets"))
			assert.NotNil(t, b)
			return nil
		})
	})
}

// Ensure that a Tx retrieving a non-existent key returns nil.
func TestTx_Get_Missing(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
			value := tx.Bucket([]byte("widgets")).Get([]byte("no_such_key"))
			assert.Nil(t, value)
			return nil
		})
	})
}

// Ensure that a bucket can be created and retrieved.
func TestTx_CreateBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket.
		db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucket([]byte("widgets"))
			assert.NotNil(t, b)
			assert.NoError(t, err)
			return nil
		})

		// Read the bucket through a separate transaction.
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			assert.NotNil(t, b)
			return nil
		})
	})
}

// Ensure that a bucket can be created if it doesn't already exist.
func TestTx_CreateBucketIfNotExists(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("widgets"))
			assert.NotNil(t, b)
			assert.NoError(t, err)

			b, err = tx.CreateBucketIfNotExists([]byte("widgets"))
			assert.NotNil(t, b)
			assert.NoError(t, err)

			b, err = tx.CreateBucketIfNotExists([]byte{})
			assert.Nil(t, b)
			assert.Equal(t, ErrBucketNameRequired, err)

			b, err = tx.CreateBucketIfNotExists(nil)
			assert.Nil(t, b)
			assert.Equal(t, ErrBucketNameRequired, err)
			return nil
		})

		// Read the bucket through a separate transaction.
		db.View(func(tx *Tx) error {
			b := tx.Bucket([]byte("widgets"))
			assert.NotNil(t, b)
			return nil
		})
	})
}

// Ensure that a bucket cannot be created twice.
func TestTx_CreateBucket_Exists(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket.
		db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucket([]byte("widgets"))
			assert.NotNil(t, b)
			assert.NoError(t, err)
			return nil
		})

		// Create the same bucket again.
		db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucket([]byte("widgets"))
			assert.Nil(t, b)
			assert.Equal(t, ErrBucketExists, err)
			return nil
		})
	})
}

// Ensure that a bucket is created with a non-blank name.
func TestTx_CreateBucket_NameRequired(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			b, err := tx.CreateBucket(nil)
			assert.Nil(t, b)
			assert.Equal(t, ErrBucketNameRequired, err)
			return nil
		})
	})
}

// Ensure that a bucket can be deleted.
func TestTx_DeleteBucket(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		// Create a bucket and add a value.
		db.Update(func(tx *Tx) error {
			tx.CreateBucket([]byte("widgets"))
			tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
			return nil
		})

		// Save root page id.
		var root pgid
		db.View(func(tx *Tx) error {
			root = tx.Bucket([]byte("widgets")).root
			return nil
		})

		// Delete the bucket and make sure we can't get the value.
		db.Update(func(tx *Tx) error {
			assert.NoError(t, tx.DeleteBucket([]byte("widgets")))
			assert.Nil(t, tx.Bucket([]byte("widgets")))
			return nil
		})

		db.Update(func(tx *Tx) error {
			// Verify that the bucket's page is free.
			assert.Equal(t, []pgid{7, 6, root, 2}, db.freelist.all())

			// Create the bucket again and make sure there's not a phantom value.
			b, err := tx.CreateBucket([]byte("widgets"))
			assert.NotNil(t, b)
			assert.NoError(t, err)
			assert.Nil(t, tx.Bucket([]byte("widgets")).Get([]byte("foo")))
			return nil
		})
	})
}

// Ensure that deleting a bucket on a closed transaction returns an error.
func TestTx_DeleteBucket_Closed(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		tx, _ := db.Begin(true)
		tx.Commit()
		assert.Equal(t, tx.DeleteBucket([]byte("foo")), ErrTxClosed)
	})
}

// Ensure that deleting a bucket with a read-only transaction returns an error.
func TestTx_DeleteBucket_ReadOnly(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.View(func(tx *Tx) error {
			assert.Equal(t, tx.DeleteBucket([]byte("foo")), ErrTxNotWritable)
			return nil
		})
	})
}

// Ensure that nothing happens when deleting a bucket that doesn't exist.
func TestTx_DeleteBucket_NotFound(t *testing.T) {
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			assert.Equal(t, ErrBucketNotFound, tx.DeleteBucket([]byte("widgets")))
			return nil
		})
	})
}

// Ensure that Tx commit handlers are called after a transaction successfully commits.
func TestTx_OnCommit(t *testing.T) {
	var x int
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.OnCommit(func() { x += 1 })
			tx.OnCommit(func() { x += 2 })
			_, err := tx.CreateBucket([]byte("widgets"))
			return err
		})
	})
	assert.Equal(t, 3, x)
}

// Ensure that Tx commit handlers are NOT called after a transaction rolls back.
func TestTx_OnCommit_Rollback(t *testing.T) {
	var x int
	withOpenDB(func(db *DB, path string) {
		db.Update(func(tx *Tx) error {
			tx.OnCommit(func() { x += 1 })
			tx.OnCommit(func() { x += 2 })
			tx.CreateBucket([]byte("widgets"))
			return errors.New("rollback this commit")
		})
	})
	assert.Equal(t, 0, x)
}

func ExampleTx_Rollback() {
	// Open the database.
	db, _ := Open(tempfile(), 0666)
	defer os.Remove(db.Path())
	defer db.Close()

	// Create a bucket.
	db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		return err
	})

	// Set a value for a key.
	db.Update(func(tx *Tx) error {
		return tx.Bucket([]byte("widgets")).Put([]byte("foo"), []byte("bar"))
	})

	// Update the key but rollback the transaction so it never saves.
	tx, _ := db.Begin(true)
	b := tx.Bucket([]byte("widgets"))
	b.Put([]byte("foo"), []byte("baz"))
	tx.Rollback()

	// Ensure that our original value is still set.
	db.View(func(tx *Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value for 'foo' is still: %s\n", value)
		return nil
	})

	// Output:
	// The value for 'foo' is still: bar
}
