package main

import (
	"github.com/billziss-gh/cgofuse/fuse"
)

// FileSystemNode represents a basic node in a file system, e.g. directory,
// file or symlink.
type FileSystemNode interface {
	Stat() *fuse.Stat_t
}

// DirectoryNode represents a directory in a file system. The directory can be
// populated on demand.
type DirectoryNode interface {
	FileSystemNode
	Children() map[string]FileSystemNode
	// Populates the directory. Will only be called once.
	Populate()
}

// SymlinkNode represents a symlink in a file system.
type SymlinkNode interface {
	FileSystemNode
	Target() string
}

// FileNode represents a file in the file system.
type FileNode interface {
	FileSystemNode

	// IsOpen returns true if the file is already open, otherwise false.
	IsOpen() (isOpen bool)

	// Open opens the file to allow io operations. A call to Close must be done
	// before calling Open again.
	Open(create bool) (errno int)

	// Read reads data from the file.
	Read(buffer []byte, offset int64) (bytesRead int, errno int)

	// Write writes data to the file.
	Write(buffer []byte, offset int64) (bytesWritten int, errno int)

	// Truncate changes the size of the file.
	Truncate(size int64) (errno int)

	// Flush flushes cached file data.
	Flush() (errno int)

	// Close closes the file.
	Close() (errno int)

	// Unlink removes the file from the storage.
	Unlink() (errno int)
}

type fileSystemNodeBase struct {
	stat fuse.Stat_t
}

func (node *fileSystemNodeBase) Stat() *fuse.Stat_t {
	return &node.stat
}

var _ FileSystemNode = (*fileSystemNodeBase)(nil)

type directoryNodeBase struct {
	fileSystemNodeBase
	children map[string]FileSystemNode
}

func (directory *directoryNodeBase) Children() map[string]FileSystemNode {
	return directory.children
}

func (directory *directoryNodeBase) Populate() {
}

var _ DirectoryNode = (*directoryNodeBase)(nil)

type symlinkNodeBase struct {
	fileSystemNodeBase
	target string
}

func (symlink *symlinkNodeBase) Target() string {
	return symlink.target
}

var _ SymlinkNode = (*symlinkNodeBase)(nil)
