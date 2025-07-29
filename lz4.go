package main

/*
#cgo LDFLAGS: -llz4
#include <stdlib.h>
#include <lz4frame.h>
*/
import "C"
import (
	"errors"
	"io"
	"unsafe"
)

const (
	bufferSize = 64 * 1024
)

// LZ4Reader is a reader that wraps LZ4 interface in a simple way and implements io.Reader.
// It became necessary as dqlite is using block dependency to yield supposedly better compression,
// and that is not supported by the usual Go lz4 package (github.com/pierrec/lz4)
type LZ4Reader struct {
	r            io.Reader
	ctx          *C.LZ4F_dctx
	inputBuf     []byte // holds compressed input
	inputStart   int    // offset of next byte to read from inputBuf
	inputEnd     int    // end of valid data in inputBuf
	outputBuf    []byte // holds decompressed output
	outputOffset int    // offset of next byte to read from outputBuf
	outputSize   int    // number of decompressed bytes in outputBuf
	eof          bool
	err          error
	frameStarted bool
}

type LZ4Error uint64

// Error implements error.
func (l LZ4Error) Error() string {
	return C.GoString(C.LZ4F_getErrorName(C.LZ4F_errorCode_t(l)))
}

var _ error = LZ4Error(0)

// NewReader wraps an io.Reader that provides compressed LZ4 (frame) data.
func NewLZ4Reader(r io.Reader) (*LZ4Reader, error) {
	var ctx *C.LZ4F_dctx
	if errCode := C.LZ4F_createDecompressionContext(&ctx, C.LZ4F_VERSION); C.LZ4F_isError(errCode) != 0 {
		return nil, errors.New("failed to create LZ4 decompression context")
	}

	return &LZ4Reader{
		r:         r,
		ctx:       ctx,
		inputBuf:  make([]byte, bufferSize),
		outputBuf: make([]byte, bufferSize),
	}, nil
}

func (lr *LZ4Reader) Read(p []byte) (int, error) {
	if lr.err != nil {
		return 0, lr.err
	}
	// Serve leftover output
	if lr.outputOffset < lr.outputSize {
		n := copy(p, lr.outputBuf[lr.outputOffset:lr.outputSize])
		lr.outputOffset += n
		return n, nil
	}
	if lr.eof {
		return 0, io.EOF
	}

	// Prepare output
	lr.outputOffset = 0
	lr.outputSize = 0

	// Fill input buffer if needed
	if lr.inputStart == lr.inputEnd {
		n, err := lr.r.Read(lr.inputBuf)
		if n == 0 && err != nil {
			if err == io.EOF {
				lr.eof = true
				return 0, io.EOF
			}
			lr.err = err
			return 0, err
		}
		lr.inputStart = 0
		lr.inputEnd = n
	}

	// Set up pointers
	srcPtr := unsafe.Pointer(&lr.inputBuf[lr.inputStart])
	srcSize := C.size_t(lr.inputEnd - lr.inputStart)
	dstPtr := unsafe.Pointer(&lr.outputBuf[0])
	dstSize := C.size_t(len(lr.outputBuf))

	// Decompress
	res := C.LZ4F_decompress(lr.ctx, dstPtr, &dstSize, srcPtr, &srcSize, nil)
	if C.LZ4F_isError(res) != 0 {
		lr.err = LZ4Error(res)
		return 0, lr.err
	}

	// Update input buffer position
	lr.inputStart += int(srcSize)
	// Update output buffer size
	lr.outputSize = int(dstSize)
	lr.outputOffset = 0

	// Try again to serve data
	return lr.Read(p)
}

func (lr *LZ4Reader) Close() error {
	if lr.ctx != nil {
		C.LZ4F_freeDecompressionContext(lr.ctx)
		lr.ctx = nil
	}
	return nil
}
