package main

import (
	"fmt"
	"math/rand"

	"github.com/gokit/actorkit"
)

func main() {
	system, _, err := actorkit.System("kit", "localhost:0")
	if err != nil {
		panic(err)
	}

	books, err := system.Spawn("bookstore", &BookStore{})
	if err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		switch rand.Intn(5) {
		case 0:
			books.Send(BookCreated{}, actorkit.Header{}, nil)
		case 1:
			books.Send(BookUpvoted{}, actorkit.Header{}, nil)
		case 2:
			books.Send(BookUpvoted{}, actorkit.Header{}, nil)
		case 3:
			books.Send(BookSigned{}, actorkit.Header{}, nil)
		case 4:
			books.Send(BookEdited{}, actorkit.Header{}, nil)
		}
	}

	actorkit.Destroy(books).Wait()
}

type BookCreated struct{}
type BookUpdated struct{}
type BookEdited struct{}
type BookSigned struct{}
type BookUpvoted struct{}

// BookEventStore is a actor behaviour which focues on storing or handling
// only events related to the state of a book, that is it's creation, update
// and editing. It does not care about signing or voting of giving books.
type BookStore struct {
	Books   actorkit.Addr
	Ratings actorkit.Addr
}

// PreStart will be called when actor is starting up.
func (bm *BookStore) PreStart(parentAddr actorkit.Addr) error {
	var err error
	bm.Books, err = parentAddr.Spawn("books_events", &BookEventStore{})
	if err != nil {
		return err
	}

	bm.Ratings, err = parentAddr.Spawn("books_ratings", &BookStarStore{})
	if err != nil {
		return err
	}

	return nil
}

func (bm *BookStore) Action(addr actorkit.Addr, message actorkit.Envelope) {
	switch message.Data.(type) {
	case BookUpvoted:
		bm.Ratings.Forward(message)
	case BookSigned:
		bm.Ratings.Forward(message)
	case BookCreated:
		bm.Books.Forward(message)
	case BookUpdated:
		bm.Books.Forward(message)
	case BookEdited:
		bm.Books.Forward(message)
	}
}

// BookEventStore is a actor behaviour which focues on storing or handling
// only events related to the state of a book, that is it's creation, update
// and editing. It does not care about signing or voting of giving books.
type BookEventStore struct{}

// Action implements the actorkit.Behaviour interface and contains the logic
// related to handling different incoming book events.
func (bm *BookEventStore) Action(addr actorkit.Addr, message actorkit.Envelope) {
	switch event := message.Data.(type) {
	case BookCreated:
		fmt.Printf("Storing new created book: %#v \n", event)
	case BookUpdated:
		fmt.Printf("Updating existing book: %#v \n", event)
	case BookEdited:
		fmt.Printf("Editing existing book: %#v \n", event)
	}
}

// BookStarStore is a actor behaviour which focuses on storing or handling
// only events related to the signing and popularity voting of books.
type BookStarStore struct{}

// Action implements the actorkit.Behaviour interface and contains the logic
// related to handling different incoming book events for upvotes and signing.
func (bm *BookStarStore) Action(addr actorkit.Addr, message actorkit.Envelope) {
	switch event := message.Data.(type) {
	case BookUpvoted:
		fmt.Printf("Upvoting book popularity: %#v \n", event)
	case BookSigned:
		fmt.Printf("Signing book for purchaser: %#v \n", event)
	}
}
