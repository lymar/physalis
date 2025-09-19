package physalis

import (
	"log/slog"

	bolt "go.etcd.io/bbolt"
)

var blobStorBucket = []byte("blobs")

func blobStorApply[EV any](tx *bolt.Tx, txs []*transactionEnvelope[EV]) error {
	blobsBucket, err := tx.CreateBucketIfNotExists(blobStorBucket)

	if err != nil {
		return err
	}

	for _, tx := range txs {
		for k, v := range tx.tx.BlobStorage {
			if v == nil {
				slog.Debug("deleting blob", "key", k)
				if err := blobsBucket.Delete([]byte(k)); err != nil {
					return err
				}
			} else {
				slog.Debug("storing blob", "key", k, "size", len(v))
				if err := blobsBucket.Put([]byte(k), v); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Slice returned by BlobView is valid only during the transaction lifetime.
func BlobView(tx *bolt.Tx, key string) []byte {
	blobsBucket := tx.Bucket(blobStorBucket)

	if blobsBucket == nil {
		return nil
	}

	return blobsBucket.Get([]byte(key))
}
