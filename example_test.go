package ftp

import (
	"fmt"
	"log"
	"time"
)

// ExampleServerConn_Walk answers the recurring "how do I use Walk?" question
// (upstream issue #293): Next advances to the next visited path — including
// descending into directories — until the tree is exhausted, Stat/Path
// describe the current entry, SkipDir prunes the current directory, and Err
// reports what (if anything) stopped the walk.
func ExampleServerConn_Walk() {
	c, err := Dial("ftp.example.org:21", DialWithTimeout(5*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := c.Quit(); err != nil {
			log.Fatal(err)
		}
	}()

	if err := c.Login("anonymous", "anonymous"); err != nil {
		log.Fatal(err)
	}

	w := c.Walk("/data")
	for w.Next() {
		entry := w.Stat()

		// Skip whole directories by name without descending into them.
		if entry.Type == EntryTypeFolder && entry.Name == ".git" {
			w.SkipDir()
			continue
		}

		fmt.Printf("%s (%v, %d bytes)\n", w.Path(), entry.Type, entry.Size)
	}
	// Err reports the failure that ended the walk, or nil after a full
	// traversal.
	if err := w.Err(); err != nil {
		log.Fatal(err)
	}
}
