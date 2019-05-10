// +build !windows

package main

import (
	"os"
	"syscall"

	"github.com/billziss-gh/cgofuse/fuse"
)

const (
	invalidHandle     = -1
	maxReadChunkSize  = 64 * 1024 * 1024 // 64 MiB
	maxWriteChunkSize = 64 * 1024 * 1024 // 64 MiB
)

type diskFile struct {
	fileSystemNodeBase
	path   string
	handle int
}

// IsOpen returns true if the file is already open, otherwise false.
func (f *diskFile) IsOpen() (isOpen bool) {
	return f.handle != invalidHandle
}

// Opens the file to prepare for io tasks.
func (f *diskFile) Open(create bool) (errno int) {
	if f.IsOpen() {
		return -fuse.EBADF
	}
	var flags int
	if create {
		flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
	} else {
		flags = os.O_RDWR
	}
	fh, err := syscall.Open(f.path, flags, 0644)
	if err != nil {
		return fuseErrorFromSysErrno(err)
	}
	f.handle = fh
	return 0
}

// Read reads data from the file.
func (f *diskFile) Read(buffer []byte, offset int64) (totalBytesRead int, errno int) {
	totalBytesRead, err := syscall.Pread(f.handle, buffer, offset)
	if err != nil {
		errno = fuseErrorFromSysErrno(err)
	} else {
		errno = 0
	}
	return totalBytesRead, errno
}

// Write writes data to the file.
func (f *diskFile) Write(buffer []byte, offset int64) (totalBytesWritten int, errno int) {
	totalBytesWritten, err := syscall.Pwrite(f.handle, buffer, offset)
	if err != nil {
		errno = fuseErrorFromSysErrno(err)
	} else {
		errno = 0
	}
	return totalBytesWritten, errno
}

// Truncate changes the size of the file.
func (f *diskFile) Truncate(size int64) (errno int) {
	return fuseErrorFromSysErrno(syscall.Ftruncate(f.handle, size))
}

// Flush flushes cached file data.
func (f *diskFile) Flush() (errno int) {
	return fuseErrorFromSysErrno(syscall.Fsync(f.handle))
}

// Close closes the file.
func (f *diskFile) Close() (errno int) {
	errno = fuseErrorFromSysErrno(syscall.Close(f.handle))
	if errno != 0 {
		return errno
	}
	f.handle = invalidHandle
	return 0
}

// Unlink removes the file from the storage.
func (f *diskFile) Unlink() (errno int) {
	err := syscall.Unlink(f.path)
	return fuseErrorFromSysErrno(err)
}

var _ FileNode = (*diskFile)(nil)

func newDiskFile(path string) FileNode {
	return &diskFile{
		path:   path,
		handle: invalidHandle,
	}
}
