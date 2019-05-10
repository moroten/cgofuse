package main

import (
	"log"
	"os"
	posixpath "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/billziss-gh/cgofuse/fuse"
)

const (
	// invalidFileHandle is defined to match cgofuse.
	invalidFileHandle = ^uint64(0)
)

// FullCgoFuseInterface groups all the fuse.FileSystem* interfaces together.
type FullCgoFuseInterface interface {
	fuse.FileSystemInterface
	fuse.FileSystemChflags
	fuse.FileSystemSetcrtime
	fuse.FileSystemSetchgtime
}

func fuseErrorFromSysErrno(err error) int {
	if nil != err {
		errno := err.(syscall.Errno)
		return -int(errno) // FUSE wants regative error numbers
	}
	return 0
}

type openFileInfo struct {
	file      FileNode
	opencount int
}

type buildBarnFuseFileSystem struct {
	fuse.FileSystemBase
	lock         sync.Mutex
	inodecounter uint64
	// rootNode is the root directory of the file system.
	rootNode DirectoryNode
	// openFiles maps inode to openFileInfo with a FileNode. Lookup using this
	// map is faster than iterating through the directory tree.
	openFiles map[uint64]*openFileInfo
	// Path for storage of new files are written.
	diskroot string
}

func (bbfs *buildBarnFuseFileSystem) synchronize() func() {
	bbfs.lock.Lock()
	return func() {
		bbfs.lock.Unlock()
	}
}

func (bbfs *buildBarnFuseFileSystem) pathOnDisk(stat *fuse.Stat_t) string {
	return filepath.Join(bbfs.diskroot, strconv.FormatUint(stat.Ino, 10))
}

func (bbfs *buildBarnFuseFileSystem) fillInNewStat(stat *fuse.Stat_t, mode uint32) (now fuse.Timespec) {
	bbfs.inodecounter++
	uid, gid, _ := fuse.Getcontext()
	now = fuse.Now()
	*stat = fuse.Stat_t{
		Dev:      0,
		Ino:      bbfs.inodecounter,
		Mode:     mode,
		Nlink:    1,
		Uid:      uid,
		Gid:      gid,
		Atim:     now,
		Mtim:     now,
		Ctim:     now,
		Birthtim: now,
		Flags:    0,
	}
	return now
}

func (bbfs *buildBarnFuseFileSystem) populateDirectory(directory DirectoryNode) (errno int) {
	// TODO: Implement
	directory.Populate()
	return 0
}

// lookupParent finds the parent directory for a path. If the path is "/", the
// return value will be (nil, 0), so check the returned errno, not the
// directory pointer.
func (bbfs *buildBarnFuseFileSystem) lookupParent(path string) (parent DirectoryNode, filename string, errno int) {
	var node FileSystemNode = bbfs.rootNode
	parent = nil
	for _, component := range strings.Split(path, "/") {
		// Double slash is allowed in paths, but probably not withing FUSE.
		// Anyway, this check sorts out the initial slash and any potential
		// trailing slash.
		if component != "" {
			if len(component) > 255 {
				return nil, "", -fuse.ENAMETOOLONG
			}
			var ok bool
			if parent != nil {
				node, ok = parent.Children()[filename]
				if !ok {
					return nil, "", -fuse.ENOENT
				}
			}
			parent, ok = node.(DirectoryNode)
			if !ok {
				return nil, "", -fuse.ENOTDIR
			}
			errno = bbfs.populateDirectory(parent)
			if errno != 0 {
				return nil, "", errno
			}
			filename = component
		}
	}
	return parent, filename, 0
}

func (bbfs *buildBarnFuseFileSystem) lookupNode(path string) (node FileSystemNode, parent DirectoryNode, errno int) {
	if path == "/" {
		return bbfs.rootNode, nil, 0
	}
	parent, filename, errno := bbfs.lookupParent(path)
	if errno != 0 {
		return nil, nil, errno
	}
	node, ok := parent.Children()[filename]
	if !ok {
		return nil, nil, -fuse.ENOENT
	}
	return node, parent, 0
}

func (bbfs *buildBarnFuseFileSystem) lookupNonexistentNode(path string) (filename string, parent DirectoryNode, errno int) {
	if path == "/" {
		return "", nil, -fuse.EEXIST
	}
	parent, filename, errno = bbfs.lookupParent(path)
	if errno != 0 {
		return "", nil, errno
	}
	if _, ok := parent.Children()[filename]; ok {
		return "", nil, -fuse.EEXIST
	}
	return filename, parent, 0
}

func (bbfs *buildBarnFuseFileSystem) lookupDirectoryNode(path string) (directory DirectoryNode, parent DirectoryNode, errno int) {
	node, parent, errno := bbfs.lookupNode(path)
	if errno != 0 {
		directory = nil
		return
	}
	directory, ok := node.(DirectoryNode)
	if !ok {
		errno = -fuse.ENOTDIR
		return
	}
	return
}

func (bbfs *buildBarnFuseFileSystem) lookupFile(path string) (file FileNode, parent DirectoryNode, errno int) {
	node, parent, errno := bbfs.lookupNode(path)
	if errno != 0 {
		file = nil
		return
	}
	file, ok := node.(FileNode)
	if !ok {
		errno = -fuse.EINVAL
		return
	}
	return
}

func (bbfs *buildBarnFuseFileSystem) lookupSymlinkNode(path string) (symlink SymlinkNode, parent DirectoryNode, errno int) {
	node, parent, errno := bbfs.lookupNode(path)
	if errno != 0 {
		symlink = nil
		return
	}
	symlink, ok := node.(SymlinkNode)
	if !ok {
		errno = -fuse.EINVAL
		return
	}
	return
}

func (bbfs *buildBarnFuseFileSystem) lookupFileHandleInfo(fh uint64) (fi *openFileInfo, errno int) {
	fi, ok := bbfs.openFiles[fh]
	if !ok {
		return nil, -fuse.EBADF
	}
	return fi, 0
}

func (bbfs *buildBarnFuseFileSystem) lookupFileHandle(fh uint64) (file FileNode, errno int) {
	fi, errno := bbfs.lookupFileHandleInfo(fh)
	if fi == nil {
		return nil, errno
	}
	return fi.file, errno
}

func (bbfs *buildBarnFuseFileSystem) lookupFileHandleSynchronized(fh uint64) (file FileNode, errno int) {
	defer bbfs.synchronize()()
	return bbfs.lookupFileHandle(fh)
}

// NewBuildBarnFuseFileSystem creates a FUSE file system which is populated
// with input files on demand and writes output files to a disk.
func NewBuildBarnFuseFileSystem(diskroot string) FullCgoFuseInterface {
	bbfs := &buildBarnFuseFileSystem{
		diskroot: diskroot,
	}
	return bbfs
}

func (bbfs *buildBarnFuseFileSystem) Init() {
	bbfs.openFiles = make(map[uint64]*openFileInfo)

	bbfs.rootNode = &directoryNodeBase{
		children: make(map[string]FileSystemNode),
	}
	_ = bbfs.fillInNewStat(bbfs.rootNode.Stat(), fuse.S_IFDIR|0777)
}

// Statfs gets file system statistics.
// Implement using GetVolumeInformation() if needed on Windows.
// func (bbfs *buildBarnFuseFileSystem) Statfs(path string, stat *Statfs_t) int

// Mknod creates a file node.
// Not needed on Windows, only used for IO_REPARSE_TAG_NFS.
func (bbfs *buildBarnFuseFileSystem) Mknod(path string, mode uint32, dev uint64) (errno int) {
	defer bbfs.synchronize()()
	filename, parent, errno := bbfs.lookupNonexistentNode(path)
	if errno != 0 {
		return errno
	}
	var stat fuse.Stat_t
	now := bbfs.fillInNewStat(&stat, mode)
	newFile := newDiskFile(bbfs.pathOnDisk(&stat))
	*newFile.Stat() = stat
	// Try to open the file before inserting it into the directory tree.
	errno = newFile.Open(true)
	if errno != 0 {
		return errno
	}
	errno = newFile.Close()
	if errno != 0 {
		return errno
	}
	// Success, insert the file.
	parent.Children()[filename] = newFile
	parent.Stat().Ctim = now
	parent.Stat().Mtim = now
	return 0
}

// Mkdir creates a directory.
func (bbfs *buildBarnFuseFileSystem) Mkdir(path string, mode uint32) (errno int) {
	defer bbfs.synchronize()()
	newDirName, parentDirectoryNode, errno := bbfs.lookupNonexistentNode(path)
	if errno != 0 {
		return errno
	}
	newDirectoryNode := &directoryNodeBase{
		children: make(map[string]FileSystemNode),
	}
	now := bbfs.fillInNewStat(newDirectoryNode.Stat(), fuse.S_IFDIR|(mode&07777))
	parentDirectoryNode.Children()[newDirName] = newDirectoryNode
	parentDirectoryNode.Stat().Ctim = now
	parentDirectoryNode.Stat().Mtim = now
	return 0
}

// Unlink removes a file.
func (bbfs *buildBarnFuseFileSystem) Unlink(path string) (errno int) {
	defer bbfs.synchronize()()
	node, parent, errno := bbfs.lookupNode(path)
	if errno != 0 {
		return errno
	}
	switch leaf := node.(type) {
	case DirectoryNode:
		errno = -fuse.EISDIR
	case FileNode:
		if node.Stat().Nlink == 1 {
			errno = leaf.Unlink()
			if errno != 0 {
				return errno
			}
		}
	case SymlinkNode:
		// noop
	default:
		log.Print("Unlink expected leaf at ", path, ", got ", node)
		return -fuse.EINVAL
	}
	delete(parent.Children(), posixpath.Base(path))
	node.Stat().Nlink--
	now := fuse.Now()
	node.Stat().Ctim = now
	node.Stat().Mtim = now
	parent.Stat().Ctim = now
	parent.Stat().Mtim = now
	return 0
}

// Rmdir removes a directory.
func (bbfs *buildBarnFuseFileSystem) Rmdir(path string) (errno int) {
	defer bbfs.synchronize()()
	if path == "/" {
		log.Print("Rmdir cannot remove the root")
		return -fuse.EACCES
	}
	directory, parent, errno := bbfs.lookupDirectoryNode(path)
	if errno != 0 {
		return errno
	}
	if len(directory.Children()) != 0 {
		return -fuse.ENOTEMPTY
	}
	delete(parent.Children(), posixpath.Base(path))
	// Well, a directory cannot be hard linked, but why not.
	directory.Stat().Nlink--
	now := fuse.Now()
	directory.Stat().Ctim = now
	directory.Stat().Mtim = now
	parent.Stat().Ctim = now
	parent.Stat().Mtim = now
	return 0
}

// Link creates a hard link to a file.
func (bbfs *buildBarnFuseFileSystem) Link(oldpath string, newpath string) (errno int) {
	defer bbfs.synchronize()()
	node, _, errno := bbfs.lookupNode(oldpath)
	if errno != 0 {
		return errno
	}
	if _, ok := node.(DirectoryNode); ok {
		// Directories are not allowed to be linked according to link(2).
		return -fuse.EPERM
	}
	newName, newParent, errno := bbfs.lookupNonexistentNode(newpath)
	if errno != 0 {
		return errno
	}
	node.Stat().Nlink++
	newParent.Children()[newName] = node
	now := fuse.Now()
	node.Stat().Ctim = now
	newParent.Stat().Ctim = now
	newParent.Stat().Mtim = now
	return 0
}

// SymlinkNode creates a symbolic link.
func (bbfs *buildBarnFuseFileSystem) Symlink(target string, newpath string) (errno int) {
	defer bbfs.synchronize()()
	newSymlinkNodeName, parent, errno := bbfs.lookupNonexistentNode(newpath)
	if errno != 0 {
		return errno
	}
	newSymlinkNode := &symlinkNodeBase{
		target: target,
	}
	now := bbfs.fillInNewStat(newSymlinkNode.Stat(), fuse.S_IFLNK|0777)
	newSymlinkNode.Stat().Size = int64(len([]byte(target)))
	parent.Children()[newSymlinkNodeName] = newSymlinkNode
	parent.Stat().Ctim = now
	parent.Stat().Mtim = now
	return 0
}

// Readlink reads the target of a symbolic link.
// The target is returned with slash as path separator.
func (bbfs *buildBarnFuseFileSystem) Readlink(path string) (errno int, target string) {
	defer bbfs.synchronize()()
	symlink, _, errno := bbfs.lookupSymlinkNode(path)
	if errno != 0 {
		return errno, ""
	}
	return 0, symlink.Target()
}

// Rename renames a file.
// If the destination already exists, it is replaced.
func (bbfs *buildBarnFuseFileSystem) Rename(oldpath string, newpath string) (errno int) {
	defer bbfs.synchronize()()
	node, oldParent, errno := bbfs.lookupNode(oldpath)
	if errno != 0 {
		return errno
	}
	newParent, newName, errno := bbfs.lookupParent(newpath)
	if errno != 0 {
		return errno
	}
	if newParent == nil {
		// newpath = "/" is not allowed.
		return -fuse.EINVAL
	}
	if newpath == oldpath {
		return 0
	}
	if strings.HasPrefix(newpath, oldpath+"/") {
		// newpath is inside oldpath, which is not allowed.
		return -fuse.EINVAL
	}

	now := fuse.Now()
	// Unlink the old node.
	nodeToRemove := newParent.Children()[newName]
	if nodeToRemove != nil {
		if directoryToRemove, ok := nodeToRemove.(DirectoryNode); ok {
			if len(directoryToRemove.Children()) != 0 {
				// Cannot replace non-empty directories.
				return -fuse.ENOTEMPTY
			}
		}
		nodeToRemove.Stat().Nlink--
		nodeToRemove.Stat().Ctim = now
	}
	// Move it.
	delete(oldParent.Children(), posixpath.Base(oldpath))
	oldParent.Stat().Ctim = now
	oldParent.Stat().Mtim = now
	newParent.Children()[newName] = node
	newParent.Stat().Ctim = now
	newParent.Stat().Mtim = now
	return 0
}

// Chmod changes the permission bits of a file.
func (bbfs *buildBarnFuseFileSystem) Chmod(path string, mode uint32) (errno int) {
	defer bbfs.synchronize()()
	node, _, errno := bbfs.lookupNode(path)
	if errno != 0 {
		return errno
	}
	node.Stat().Mode = (node.Stat().Mode & fuse.S_IFMT) | (mode & 07777)
	node.Stat().Ctim = fuse.Now()
	return 0
}

// Chown changes the owner and group of a file.
func (bbfs *buildBarnFuseFileSystem) Chown(path string, uid uint32, gid uint32) (errno int) {
	defer bbfs.synchronize()()
	node, _, errno := bbfs.lookupNode(path)
	if errno != 0 {
		return errno
	}
	if uid != ^uint32(0) {
		node.Stat().Uid = uid
	}
	if gid != ^uint32(0) {
		node.Stat().Gid = gid
	}
	node.Stat().Ctim = fuse.Now()
	return 0
}

// Utimens changes the access and modification times of a file.
func (bbfs *buildBarnFuseFileSystem) Utimens(path string, tmsp []fuse.Timespec) (errno int) {
	defer bbfs.synchronize()()
	node, _, errno := bbfs.lookupNode(path)
	if errno != 0 {
		return errno
	}
	now := fuse.Now()
	if tmsp == nil {
		node.Stat().Atim = now
		node.Stat().Mtim = now
	} else {
		if len(tmsp) != 2 {
			return -fuse.EINVAL
		}
		node.Stat().Atim = tmsp[0]
		node.Stat().Mtim = tmsp[1]
	}
	node.Stat().Ctim = now
	return 0
}

// Access checks file access permissions.
// TODO: Needed?

// Create creates and opens a file.
// Not implemented. Use Mknod() instead as that is required anyway for `mkfifo` to work.
// The flags are a combination of the fuse.O_* constants.
// func (bbfs *buildBarnFuseFileSystem) Create2(path string, flags int, mode uint32) (errno int, fh uint64)

// Open opens a file.
// The flags are a combination of the fuse.O_* constants.
func (bbfs *buildBarnFuseFileSystem) Open(path string, flags int) (errno int, fh uint64) {
	defer bbfs.synchronize()()
	file, _, errno := bbfs.lookupFile(path)
	if errno != 0 {
		return errno, invalidFileHandle
	}
	fi, ok := bbfs.openFiles[file.Stat().Ino]
	if !ok {
		errno = file.Open(false)
		if errno != 0 {
			return errno, invalidFileHandle
		}
		fi = &openFileInfo{
			file:      file,
			opencount: 0,
		}
		bbfs.openFiles[file.Stat().Ino] = fi
	}
	fi.opencount++
	return 0, fi.file.Stat().Ino
}

// Getattr gets file attributes.
func (bbfs *buildBarnFuseFileSystem) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errno int) {
	defer bbfs.synchronize()()
	node, _, errno := bbfs.lookupNode(path)
	if errno != 0 {
		return errno
	}
	*stat = *node.Stat()
	return 0
}

// Truncate changes the size of a file.
func (bbfs *buildBarnFuseFileSystem) Truncate(path string, size int64, fh uint64) (errno int) {
	defer bbfs.synchronize()()
	var file FileNode
	shouldClose := false
	closeErrno := int(0)
	if fh != invalidFileHandle {
		file, errno = bbfs.lookupFileHandle(fh)
	} else {
		file, _, errno = bbfs.lookupFile(path)
		if !file.IsOpen() {
			// TODO: Use `defer Close()` in case Truncate() panics?
			errno = file.Open(false)
			if errno != 0 {
				return errno
			}
			shouldClose = true
		}
	}
	errno = file.Truncate(size)
	if errno == 0 {
		file.Stat().Size = size
		now := fuse.Now()
		file.Stat().Ctim = now
		file.Stat().Mtim = now
	}
	if shouldClose {
		closeErrno = file.Close()
		if closeErrno != 0 {
			log.Printf("Truncate(%d, \"%s\") failed closing the file (error %d, close error %d)", fh, path, errno, closeErrno)
			if errno == 0 {
				errno = closeErrno
			}
		}
	}
	return errno
}

// Read reads data from a file.
func (bbfs *buildBarnFuseFileSystem) Read(path string, buff []byte, ofst int64, fh uint64) (bytesReadOrErrno int) {
	file, errno := bbfs.lookupFileHandleSynchronized(fh)
	if errno != 0 {
		log.Printf("Read(%d, \"%s\") called for a closed file (error %d)", fh, path, errno)
		return errno
	}
	bytesRead, errno := file.Read(buff, ofst)
	if errno != 0 {
		return errno
	}
	file.Stat().Atim = fuse.Now()
	return bytesRead
}

// Write writes data to a file.
func (bbfs *buildBarnFuseFileSystem) Write(path string, buff []byte, ofst int64, fh uint64) (bytesWritten int) {
	file, errno := bbfs.lookupFileHandleSynchronized(fh)
	if errno != 0 {
		log.Printf("Write(%d, \"%s\") called for a closed file (error %d)", fh, path, errno)
		return errno
	}
	bytesWritten, errno = file.Write(buff, ofst)
	if bytesWritten != 0 {
		now := fuse.Now()
		file.Stat().Ctim = now
		file.Stat().Mtim = now
		endOffset := ofst + int64(bytesWritten)
		if endOffset > file.Stat().Size {
			file.Stat().Size = endOffset
		}
	}
	if errno != 0 {
		return errno
	}
	return bytesWritten
}

// Flush flushes cached file data.
func (bbfs *buildBarnFuseFileSystem) Flush(path string, fh uint64) (errno int) {
	file, errno := bbfs.lookupFileHandleSynchronized(fh)
	if errno != 0 {
		log.Printf("Flush(%d, \"%s\") called for a closed file (error %d)", fh, path, errno)
		return errno
	}
	errno = file.Flush()
	return errno
}

// Release closes an open file.
func (bbfs *buildBarnFuseFileSystem) Release(path string, fh uint64) (errno int) {
	defer bbfs.synchronize()()
	fi, errno := bbfs.lookupFileHandleInfo(fh)
	if errno != 0 {
		log.Printf("Close(%d, \"%s\") called for a closed file (error %d)", fh, path, errno)
		return errno
	}
	if fi.opencount == 1 {
		errno = fi.file.Close()
		if errno != 0 {
			return errno
		}
		delete(bbfs.openFiles, fi.file.Stat().Ino)
	}
	fi.opencount--
	return errno
}

// Fsync synchronizes file contents.
func (bbfs *buildBarnFuseFileSystem) Fsync(path string, datasync bool, fh uint64) (errno int) {
	file, errno := bbfs.lookupFileHandleSynchronized(fh)
	if errno != 0 {
		log.Printf("Fsync(%d, \"%s\") called for a closed file (error %d)", fh, path, errno)
		return errno
	}
	errno = file.Flush()
	return errno
}

// Opendir opens a directory.
// func (bbfs *buildBarnFuseFileSystem) Opendir(path string) (errno int, fh uint64)

// Readdir reads a directory.
func (bbfs *buildBarnFuseFileSystem) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errno int) {
	defer bbfs.synchronize()()
	directory, _, errno := bbfs.lookupDirectoryNode(path)
	if errno != 0 {
		return errno
	}
	if !fill(".", nil, 0) {
		return 0
	}
	if !fill("..", nil, 0) {
		return 0
	}
	for name, node := range directory.Children() {
		if !fill(name, node.Stat(), 0) {
			return 0
		}
	}
	return 0
}

// Releasedir closes an open directory.
// func (bbfs *buildBarnFuseFileSystem) Releasedir(path string, fh uint64) (errno int)

// Fsyncdir synchronizes directory contents.
// func (bbfs *buildBarnFuseFileSystem) Fsyncdir(path string, datasync bool, fh uint64) (errno int)

// Setxattr sets extended attributes.
// func (bbfs *buildBarnFuseFileSystem) Setxattr(path string, name string, value []byte, flags int) (errno int)

// Getxattr gets extended attributes.
// func (bbfs *buildBarnFuseFileSystem) Getxattr(path string, name string) (errno int, attr []byte)

// Removexattr removes extended attributes.
// func (bbfs *buildBarnFuseFileSystem) Removexattr(path string, name string) (errno int)

// Listxattr lists extended attributes.
// func (bbfs *buildBarnFuseFileSystem) Listxattr(path string, fill func(name string) bool) (errno int)

// FileSystemChflags is the interface that wraps the Chflags method.
//
// Chflags changes the BSD file flags (Windows file attributes). [OSX and Windows only]
func (bbfs *buildBarnFuseFileSystem) Chflags(path string, flags uint32) (errno int) {
	defer bbfs.synchronize()()
	node, _, errno := bbfs.lookupNode(path)
	if errno != 0 {
		return errno
	}
	node.Stat().Flags = flags
	node.Stat().Ctim = fuse.Now()
	return 0
}

// FileSystemSetcrtime is the interface that wraps the Setcrtime method.
//
// Setcrtime changes the file creation (birth) time. [OSX and Windows only]
func (bbfs *buildBarnFuseFileSystem) Setcrtime(path string, tmsp fuse.Timespec) (errno int) {
	defer bbfs.synchronize()()
	node, _, errno := bbfs.lookupNode(path)
	if errno != 0 {
		return errno
	}
	node.Stat().Birthtim = tmsp
	node.Stat().Ctim = fuse.Now()
	return 0
}

// FileSystemSetchgtime is the interface that wraps the Setchgtime method.
//
// Setchgtime changes the file change (ctime) time. [OSX and Windows only]
func (bbfs *buildBarnFuseFileSystem) Setchgtime(path string, tmsp fuse.Timespec) (errno int) {
	defer bbfs.synchronize()()
	node, _, errno := bbfs.lookupNode(path)
	if errno != 0 {
		return errno
	}
	node.Stat().Ctim = tmsp
	return 0
}

func main() {
	/*
		bbfs := NewBuildBarnFileSystem(
			&Hellofs{
				&fuse.FileSystemBase{},
			},
			os.Args[1])
	*/
	bbfs := NewBuildBarnFuseFileSystem(os.Args[1])
	host := fuse.NewFileSystemHost(bbfs)
	host.SetCapReaddirPlus(true) // Readdir returns stat information.
	host.Mount(os.Args[2], os.Args[3:])
	/*
		[]string{
			`--VolumePrefix=\LocalFuseSystem\BuildBarnWorker`,
			"-d", // For debugging
		}
	*/
	log.Print("Hej")
}
