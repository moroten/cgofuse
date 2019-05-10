// +build windows

package main

import (
	"path/filepath"
	"strings"
	"syscall"

	"github.com/billziss-gh/cgofuse/fuse"
)

const (
	maxReadChunkSize  = 64 * 1024 * 1024 // 64 MiB
	maxWriteChunkSize = 64 * 1024 * 1024 // 64 MiB
)

type diskFile struct {
	fileSystemNodeBase
	path   string
	handle syscall.Handle
}

// IsOpen returns true if the file is already open, otherwise false.
func (f *diskFile) IsOpen() (isOpen bool) {
	return f.handle != syscall.InvalidHandle
}

// Opens the file to prepare for io tasks.
func (f *diskFile) Open(create bool) (errno int) {
	if f.IsOpen() {
		return -fuse.EBADF
	}
	utf16path, err := syscall.UTF16PtrFromString(fixLongPath(f.path))
	if err != nil {
		return fuseErrorFromSysErrno(err)
	}
	var creationDisposition uint32
	if !create {
		creationDisposition = syscall.OPEN_EXISTING
	} else {
		creationDisposition = syscall.OPEN_ALWAYS
	}
	// fuse.Unlink() is called while the file is still open, so use FILE_SHARE_DELETE
	f.handle, err = syscall.CreateFile(
		utf16path,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, // lpSecurityAttributes
		creationDisposition,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0) // hTemplateFile
	if err != nil {
		return fuseErrorFromSysErrno(err)
	}
	return 0
}

// Read reads data from the file.
func (f *diskFile) Read(buffer []byte, offset int64) (totalBytesRead int, errno int) {
	totalBytesRead = 0
	for totalBytesRead < len(buffer) {
		bytesToRead := len(buffer) - totalBytesRead
		if bytesToRead > maxReadChunkSize {
			bytesToRead = maxReadChunkSize
		}
		readOffset := offset + int64(totalBytesRead)
		var bytesRead uint32
		overlapped := syscall.Overlapped{
			OffsetHigh: uint32(readOffset >> 32),
			Offset:     uint32(readOffset),
		}
		err := syscall.ReadFile(f.handle, buffer[totalBytesRead:totalBytesRead+bytesToRead], &bytesRead, &overlapped)
		if err != nil {
			if err.(syscall.Errno) == syscall.ERROR_HANDLE_EOF && bytesRead == 0 {
				// End of file
				break
			}
			return totalBytesRead, fuseErrorFromSysErrno(err)
		}
		if bytesRead == 0 {
			// TODO: What happened? Nothing read but still not EOF?
			break
		}
		totalBytesRead += int(bytesRead)
	}
	return totalBytesRead, 0
}

// Write writes data to the file.
func (f *diskFile) Write(buffer []byte, offset int64) (totalBytesWritten int, errno int) {
	totalBytesWritten = 0
	for totalBytesWritten < len(buffer) {
		chunk := buffer[totalBytesWritten:]
		if len(chunk) > maxWriteChunkSize {
			chunk = chunk[:maxWriteChunkSize]
		}
		writeOffset := offset + int64(totalBytesWritten)
		var bytesWritten uint32
		overlapped := syscall.Overlapped{
			OffsetHigh: uint32(writeOffset >> 32),
			Offset:     uint32(writeOffset),
		}
		err := syscall.WriteFile(f.handle, chunk, &bytesWritten, &overlapped)
		if err != nil {
			return totalBytesWritten, fuseErrorFromSysErrno(err)
		}
		if bytesWritten == 0 {
			// TODO: What happened? Nothing written?
			break
		}
		totalBytesWritten += int(bytesWritten)
	}
	return totalBytesWritten, 0
}

// Truncate changes the size of the file.
func (f *diskFile) Truncate(size int64) (errno int) {
	return fuseErrorFromSysErrno(syscall.Ftruncate(f.handle, size))
}

// Flush flushes cached file data.
func (f *diskFile) Flush() (errno int) {
	return fuseErrorFromSysErrno(syscall.FlushFileBuffers(f.handle))
}

// Close closes the file.
func (f *diskFile) Close() (errno int) {
	errno = fuseErrorFromSysErrno(syscall.Close(f.handle))
	if errno != 0 {
		return errno
	}
	f.handle = syscall.InvalidHandle
	return 0
}

// Unlink removes the file from the storage.
func (f *diskFile) Unlink() (errno int) {
	err := syscall.Unlink(f.path)
	return fuseErrorFromSysErrno(err)
}

var _ FileNode = (*diskFile)(nil)

func fixLongPath(shortPath string) string {
	if len(shortPath) < 248 {
		return shortPath
	}
	if strings.HasPrefix(shortPath, `\\`) {
		return shortPath
	}
	absPath, err := filepath.Abs(shortPath)
	if err != nil {
		return shortPath
	}
	return `\\?\` + absPath
}

func newDiskFile(path string) FileNode {
	return &diskFile{
		path:   path,
		handle: syscall.InvalidHandle,
	}
}
