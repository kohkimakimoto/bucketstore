package shell

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/kohkimakimoto/bucketstore"
	"golang.org/x/crypto/ssh/terminal"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type Shell struct {
	DB      *bucketstore.DB
	Path    string
	Options *bucketstore.Options
	exit    bool
	Stdin   *os.File
	Stdout  *os.File
	Stderr  *os.File
}

func NewShell() *Shell {
	return &Shell{
		Stdin: os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

type shell struct {
	r io.Reader
	w io.Writer
}

func (sh *shell) Read(data []byte) (n int, err error) {
	return sh.r.Read(data)
}
func (sh *shell) Write(data []byte) (n int, err error) {
	return sh.w.Write(data)
}

func (sh *Shell) Run() error {
	// if the stdin is pipe, runs as a non-interactive mode
	if stat, _ := sh.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) == 0 {
		scanner := bufio.NewScanner(sh.Stdin)
		for scanner.Scan() {
			if err := sh.ExecCommand(scanner.Text()); err != nil {
				return err
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}

		return nil
	}

	// the following code runs on interactive mode

	if _, err := os.Stat(sh.Path); err != nil {
		// file not found.
		fmt.Printf("Database file '%s' was not found.\n", sh.Path)
		fmt.Print("Do you create initial database file? [y|N]: ")
		reader := bufio.NewReader(os.Stdin)
		if str, err := reader.ReadString('\n'); err == nil {
			str = strings.TrimRight(str, "\r\n")
			if str != "y" {
				return fmt.Errorf("the database has not been created.")
			}
		}

		// create new database.
		ds, err := bucketstore.Open(sh.Path, 0600, sh.Options)
		if err != nil {
			return err
		}
		ds.Close()
	}

	fmt.Println("Welcome to Bucketstore client. (hit ^D to exit)")
	fmt.Println("You can see the help by typing 'help' command.")

	fd := int(sh.Stdin.Fd())
	oldState, err := terminal.MakeRaw(fd)
	if err != nil {
		return err
	}
	defer terminal.Restore(fd, oldState)
	term := terminal.NewTerminal(&shell{r: sh.Stdin, w: sh.Stdout}, "["+filepath.Base(sh.Path)+"]> ")
	if term == nil {
		return fmt.Errorf("could not create terminal")
	}

	for {
		line, err := term.ReadLine()
		if err != nil {
			break
		}

		if err := sh.ExecCommand(line); err != nil {
			return err
		}

		if sh.exit {
			break
		}
	}

	return nil
}

func (sh *Shell) Exit() {
	sh.exit = true
}

func (sh *Shell) ExecCommand(line string) error {
	// in order to prevent locking the database, get a new datastore instance by each commands.
	ds, err := bucketstore.Open(sh.Path, 0600, sh.Options)
	if err != nil {
		return err
	}
	sh.DB = ds
	defer func() {
		ds.Close()
		sh.DB = nil
	}()

	tokens, err := Tokenize(line)
	if err != nil {
		sh.outputError(fmt.Sprintf("%v", err), false)
		return nil
	}

	if len(tokens) == 0 {
		return nil
	}

	// parse global options
	var pretty bool
	var removedIndexes = []int{}
	for i, token := range tokens {
		if token.DataType == DataTypeTerm && strings.HasPrefix(token.Buf, "-") {
			switch token.Buf {
			case "-p":
				pretty = true
				removedIndexes = append(removedIndexes, i)
			}
		}
	}

	// remove global options from tokens
	for i, idx := range removedIndexes {
		removedI := idx - i
		tokens = append(tokens[:removedI], tokens[removedI+1:]...)
	}

	defer func() {
		if err := recover(); err != nil {
			sh.outputError(fmt.Sprintf("%v", err), pretty)
		}
	}()

	if len(tokens) >= 1 {
		if tokens[0].DataType != DataTypeTerm {
			sh.outputError("syntax error: it is not a valid command.", pretty)
			return nil
		}

		if fn, ok := Cmds[tokens[0].Buf]; ok {
			res, err := fn(sh, tokens[1:])
			if err != nil {
				sh.outputError(fmt.Sprintf("%v", err), pretty)
			}

			if res != nil {
				sh.output(res, pretty)
			}
		} else {
			sh.outputError(fmt.Sprintf("unknown command: %s", tokens[0].Buf), pretty)
		}
	}

	return nil
}

func (sh *Shell) outputError(message string, pretty bool) {
	res := &Response{
		Status: "error",
		Message: message,
	}

	var b []byte
	if pretty {
		b, _ = json.MarshalIndent(res, "", "  ")
	} else {
		b, _ = json.Marshal(res)
	}

	fmt.Fprintln(sh.Stderr, string(b))
}

func (sh *Shell) output(res *Response, pretty bool) {
	var b []byte
	if pretty {
		b, _ = json.MarshalIndent(res, "", "  ")
	} else {
		b, _ = json.Marshal(res)
	}

	fmt.Fprintln(sh.Stdout, string(b))
}
