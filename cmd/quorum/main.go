// Command quorum is a small interactive shell over the LSM storage engine —
// enough to kick the tires by hand before the Raft layer lands.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/mbsdeepak/quorum/internal/lsm"
)

func main() {
	dir := "quorum-data"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	db, err := lsm.Open(dir, lsm.DefaultOptions())
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Printf("quorum lsm engine — data dir %q\n", dir)
	fmt.Println(`commands: put <k> <v...> | get <k> | del <k> | flush | exit`)

	sc := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) > 0 {
			run(db, f)
		}
		fmt.Print("> ")
	}
}

func run(db *lsm.DB, f []string) {
	switch f[0] {
	case "put":
		if len(f) < 3 {
			fmt.Println("usage: put <k> <v...>")
			return
		}
		if err := db.Put([]byte(f[1]), []byte(strings.Join(f[2:], " "))); err != nil {
			fmt.Println("err:", err)
		} else {
			fmt.Println("ok")
		}
	case "get":
		if len(f) < 2 {
			fmt.Println("usage: get <k>")
			return
		}
		v, err := db.Get([]byte(f[1]))
		switch {
		case errors.Is(err, lsm.ErrNotFound):
			fmt.Println("(not found)")
		case err != nil:
			fmt.Println("err:", err)
		default:
			fmt.Printf("%s\n", v)
		}
	case "del":
		if len(f) < 2 {
			fmt.Println("usage: del <k>")
			return
		}
		if err := db.Delete([]byte(f[1])); err != nil {
			fmt.Println("err:", err)
		} else {
			fmt.Println("ok")
		}
	case "flush":
		if err := db.Flush(); err != nil {
			fmt.Println("err:", err)
		} else {
			fmt.Println("flushed")
		}
	case "exit", "quit":
		os.Exit(0)
	default:
		fmt.Println("unknown command:", f[0])
	}
}
