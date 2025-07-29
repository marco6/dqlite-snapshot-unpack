package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "dqlite-snapshot-unpack <snapshot>",
	Short: "Unpack dqlite snapshots",
	Long:  `Unpacks dqlite snapshots into readable databases for sqlite3 cli`,
	Args:  cobra.ExactArgs(1),
	RunE:  unpack,
}

func unpack(cmd *cobra.Command, args []string) error {
	reader, err := createReader(args[0])
	if err != nil {
		return err
	}

	if format, err := readUint64(reader); err != nil {
		return fmt.Errorf("couldn't read format number: %w", err)
	} else if format != 1 {
		return fmt.Errorf("unexpected format number: %d", format)
	}

	databases, err := readUint64(reader)
	if err != nil {
		return fmt.Errorf("couldn't read database count: %w", err)
	}

	fmt.Printf("Database count: %d\n", databases)

	for range databases {
		name, err := readPaddedString(reader)
		if err != nil {
			return fmt.Errorf("couldn't read the database name: %w", err)
		}
		fmt.Printf("Decoding database %s...\n", name)

		mainSize, err := readUint64(reader)
		if err != nil {
			return fmt.Errorf("couldn't read main size: %w", err)
		}
		walSize, err := readUint64(reader)
		if err != nil {
			return fmt.Errorf("couldn't read wal size: %w", err)
		}

		fmt.Printf("Decoding main database file (%d bytes)...\n", mainSize)
		if err := unpackFile(reader, name, int64(mainSize)); err != nil {
			return fmt.Errorf("couldn't unpack main: %w", err)
		}

		fmt.Printf("Decoding WAL database file (%d bytes)...\n", walSize)
		if err := unpackFile(reader, name+"-wal", int64(walSize)); err != nil {
			return fmt.Errorf("couldn't unpack wal: %w", err)
		}
		fmt.Println("Done!\n")
	}

	var extra [1]byte
	_, err = reader.Read(extra[:])
	if err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("checking for EOF: %w", err)
	} else {
		return fmt.Errorf("expected EOF but found extra data")
	}
}

func readUint64(r io.Reader) (uint64, error) {
	var buf [8]byte
	_, err := io.ReadFull(r, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}

// readPaddedString reads a null-terminated string from r,
// consuming 8-byte blocks, stopping at the first null, and discarding remaining padding.
func readPaddedString(r io.Reader) (string, error) {
	var buf bytes.Buffer
	block := make([]byte, 8)

	for {
		_, err := io.ReadFull(r, block)
		if err != nil {
			return "", fmt.Errorf("reading block: %w", err)
		}

		// Efficient null scan
		i := bytes.IndexByte(block, 0)
		if i >= 0 {
			// Null found: write up to it and stop
			buf.Write(block[:i])
			break
		}

		// No null: write whole block
		buf.Write(block)
	}

	return buf.String(), nil
}

func unpackFile(reader io.Reader, name string, length int64) error {
	main, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0766)
	if err != nil {
		return err
	}
	defer main.Close()

	_, err = io.Copy(main, io.LimitReader(reader, int64(length)))
	return err
}

func createReader(path string) (io.Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(file)
	compressed, err := isCompressed(reader)
	if err != nil {
		return nil, err
	}
	if compressed {
		return NewLZ4Reader(reader)
	}
	return reader, nil
}

func isCompressed(reader *bufio.Reader) (bool, error) {
	const lz4magic = 0x184D2204
	lz4Header, err := reader.Peek(4)
	if err != nil {
		return false, err
	}

	compressed := binary.LittleEndian.Uint32(lz4Header) == lz4magic
	return compressed, nil
}

func main() {
	rootCmd.Execute()
}
