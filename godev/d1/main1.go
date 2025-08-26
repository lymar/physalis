package main

import (
	"log"

	bolt "go.etcd.io/bbolt"
)

func main() {
	db, err := bolt.Open("qwe.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
		if err != nil {
			return err
		}

		r := b.Get([]byte("qwe"))
		log.Printf("pre: %s\n", r)

		b.Put([]byte("qwe"), []byte("asd3"))

		v := b.Get([]byte("qwe"))
		log.Printf("after: %s\n", v)

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	someChan := make(chan any, 10)
	someChan <- "qwe"
	someChan <- 123
	someChan <- struct {
		Qwe string
		Asd int
	}{
		Qwe: "qweqe asdf",
		Asd: 10,
	}
	close(someChan)

	for m := range someChan {
		log.Printf("msg: %+v", m)
	}

	// var wg sync.WaitGroup

	// wg.Go(func() {
	// 	fmt.Println("yo!")
	// })

	// wg.Wait()
	// fmt.Println("Hello, world!")
}
