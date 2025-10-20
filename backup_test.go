package physalis

import (
	"bytes"
	"testing"

	"github.com/go-softwarelab/common/pkg/seq"
)

func TestWriteBackup(t *testing.T) {
	var buf bytes.Buffer

	itemsData := []backupItem{
		{
			Event: &backupEvent{
				ID:   1,
				Data: []byte("event1"),
			},
		},
		{
			Blob: &backupBlob{
				Name: "blob1",
				Data: []byte("blobdata1"),
			},
		},
		{
			Event: &backupEvent{
				ID:   2,
				Data: []byte("event2"),
			},
		},
		{
			Blob: &backupBlob{
				Name: "blob2",
				Data: []byte("blobdata2"),
			},
		},
	}

	items := seq.FromSlice(itemsData)

	err := writeBackup(&buf, items)
	if err != nil {
		t.Fatalf("writeBackup failed: %v", err)
	}

	loadedItems := seq.Collect(loadBackup(&buf))
	if len(loadedItems) != len(itemsData) {
		t.Fatalf("expected %d items, got %d", len(itemsData), len(loadedItems))
	}

	for i, item := range loadedItems {
		origItem := itemsData[i]
		if origItem.Event != nil && item.Event != nil {
			if origItem.Event.ID != item.Event.ID || !bytes.Equal(origItem.Event.Data, item.Event.Data) {
				t.Fatalf("mismatch in event at index %d", i)
			}
		} else if origItem.Blob != nil && item.Blob != nil {
			if origItem.Blob.Name != item.Blob.Name || !bytes.Equal(origItem.Blob.Data, item.Blob.Data) {
				t.Fatalf("mismatch in blob at index %d", i)
			}
		} else {
			t.Fatalf("mismatch in item type at index %d", i)
		}
	}
}
