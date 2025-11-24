package physalis

import (
	"fmt"
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	loc, err := time.LoadLocation("America/Bogota")
	if err != nil {
		t.Fatal(err)
	}

	data := time.Date(
		2025,
		time.September,
		10,
		3, 4, 5,
		123456789,
		loc,
	)

	fmt.Println(data)

	b, err := CBORMarshal(data)
	if err != nil {
		t.Fatal(err)
	}

	var decoded time.Time
	err = CBORUnmarshal(b, &decoded)
	if err != nil {
		t.Fatal(err)
	}

	if !data.Equal(decoded) {
		t.Fatalf("decoded time does not match original: got %v, want %v", decoded, data)
	}
}

func TestNestedTime(t *testing.T) {
	type Nested struct {
		T time.Time
	}

	loc, err := time.LoadLocation("America/Bogota")
	if err != nil {
		t.Fatal(err)
	}

	original := Nested{
		T: time.Date(
			2025,
			time.September,
			10,
			3, 4, 5,
			123456789,
			loc,
		),
	}

	b, err := CBORMarshal(original)
	if err != nil {
		t.Fatal(err)
	}

	var decoded Nested
	err = CBORUnmarshal(b, &decoded)
	if err != nil {
		t.Fatal(err)
	}

	if !original.T.Equal(decoded.T) {
		t.Fatalf("decoded time does not match original: got %v, want %v", decoded.T, original.T)
	}
}
