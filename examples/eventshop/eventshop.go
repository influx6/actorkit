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

	for i := 0; i < 50; i++ {
		tm := rand.Intn(20)
		switch tm {
		case 0:
			if err := books.Send(BookCreated{}, actorkit.Header{}, nil); err != nil {
				panic(err)
			}
		case 1:
			if err := books.Send(BookUpdated{}, actorkit.Header{}, nil); err != nil {
				panic(err)
			}
		case 2:
			if err := books.Send(BookUpvoted{}, actorkit.Header{}, nil); err != nil {
				panic(err)
			}
		case 3:
			if err := books.Send(BookSigned{}, actorkit.Header{}, nil); err != nil {
				panic(err)
			}
		case 4:
			if err := books.Send(BookEdited{}, actorkit.Header{}, nil); err != nil {
				panic(err)
			}
		}
	}

	actorkit.Poison(system).Wait()
}

type BookCreated struct{}
type BookUpdated struct{}
type BookEdited struct{}
type BookSigned struct{}
type BookUpvoted struct{}

// BookEventStore is a actor handles processing of book related events
// It internal has two actors for book events and book ratings which it
// filters messages to based on criteria.
type BookStore struct {
	Books   actorkit.Addr
	Ratings actorkit.Addr
}

func (bm *BookStore) PostStart(addr actorkit.Addr) error {
	fmt.Println("Bookstore is ready")
	return nil
}

// PreStart will be called when actor is starting up.
func (bm *BookStore) PreStart(actor actorkit.Addr) error {
	if bm.Books == nil {
		books, err := actor.Spawn("books_events", &BookEventStore{})
		if err != nil {
			return err
		}

		bm.Books = books
	}

	if bm.Ratings == nil {
		ratings, err := actor.Spawn("books_ratings", &BookRatingStore{})
		if err != nil {
			return err
		}

		bm.Ratings = ratings
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

func (bm *BookEventStore) PreStart(addr actorkit.Addr) error {
	fmt.Println("Books Events Ready!")
	return nil
}

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

// BookRatingStore is a actor behaviour which focuses on storing or handling
// only events related to the signing and popularity voting of books.
type BookRatingStore struct{}

func (bm *BookRatingStore) PreStart(addr actorkit.Addr) error {
	fmt.Println("Books Ratings Ready!")
	return nil
}

// Action implements the actorkit.Behaviour interface and contains the logic
// related to handling different incoming book events for upvotes and signing.
func (bm *BookRatingStore) Action(addr actorkit.Addr, message actorkit.Envelope) {
	switch event := message.Data.(type) {
	case BookUpvoted:
		fmt.Printf("Upvoting book popularity: %#v \n", event)
	case BookSigned:
		fmt.Printf("Signing book for purchaser: %#v \n", event)
	}
}
