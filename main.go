package main

import (
	"fmt"
	"os"
	Path "path"
	"strings"
	"syscall"
	"time"
	"unsafe"
	"strconv"
	"sync"

	"github.com/hanwen/go-fuse/fuse"
)

const IDXATTR = "trusted.constor.id"
const DELXATTR = "trusted.constor.deleted"
const LINKSXATTR = "trusted.constor.links"
const ROOTID = "00000000000000000000000000000001"

type Constor struct {
	sync.Mutex
	logf	  *os.File
	inodemap  *Inodemap
	fdmap     map[uintptr]*FD
	layers    []string
}

func (constor *Constor) Lookup(header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	constor.log("%d %s", header.NodeId, name)
	var stat syscall.Stat_t
	if len(name) > 255 {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	li := -1
	parent := constor.inodemap.findInodePtr(header.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}
	id, err := constor.getid(-1, parent.id, name)
	if err != nil {
		return fuse.ToStatus(err)
	}
	inode := constor.inodemap.findInodeId(id)
	if inode != nil {
		li = inode.layer
	} else {
		li = constor.getLayer(id)
	}
	if li == -1 {
		constor.error("%s (%s)", id, name)
		return fuse.ENOENT
	}
	err = constor.Lstat(li, id, &stat)
	if err != nil {
		constor.error("%s (%s) : %s", id, name, err)
		return fuse.ToStatus(err)
	}
	if inode == nil {
		inode = NewInode(constor, id)
		inode.layer = li
		constor.inodemap.hashInode(inode)
	} else {
		inode.lookup()
	}
	attr := (*fuse.Attr)(&out.Attr)
	attr.FromStat(&stat)
	out.NodeId = uint64(uintptr(unsafe.Pointer(inode)))
	out.Ino = attr.Ino
	constor.log("%d", out.NodeId)
	return fuse.OK
}

func (constor *Constor) Forget(nodeID uint64, nlookup uint64) {
	constor.log("%d %d", nodeID, nlookup)
	if inode := constor.inodemap.findInodePtr(nodeID); inode != nil {
		inode.forget(nlookup)
	}
}

func (constor *Constor) GetAttr(input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	constor.log("%d", input.NodeId)
	stat := syscall.Stat_t{}
	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		constor.error("%d not in inodemap", input.NodeId)
		return fuse.ENOENT
	}
	if inode.id == ROOTID && inode.layer == -1 {
		inode.layer = constor.getLayer(inode.id)
	}

	err := constor.Lstat(inode.layer, inode.id, &stat)
	if err != nil {
		constor.error("%s: %s", inode.id, err)
		return fuse.ToStatus(err)
	}
	attr := (*fuse.Attr)(&out.Attr)
	attr.FromStat(&stat)
	constor.log("%d", attr.Ino)
	return fuse.OK
}

func (constor *Constor) OpenDir(input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	constor.log("%d", input.NodeId)
	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		return fuse.ENOENT
	}
	entries := map[string]DirEntry{}
	for li, _ := range constor.layers {
		path := constor.getPath(li, inode.id)
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		infos, _ := f.Readdir(0)
		for i := range infos {
			// workaround forhttps://code.google.com/p/go/issues/detail?id=5960
			if infos[i] == nil {
				continue
			}
			name := infos[i].Name()
			if _, ok := entries[name]; ok {
				// skip if the file was in upper layer
				continue
			}
			d := DirEntry {
				Name: name,
				Mode: uint32(infos[i].Mode()),
			}
			if constor.isdeleted(Path.Join(path, name)) {
				d.Deleted = true
			}
			id, err := constor.getid(li, inode.id, name)
			constor.log("%s", id)
			if err != nil {
				continue;
			}
			d.Ino = idtoino(id)
			entries[name] = d
		}
		f.Close()
	}
	fmt.Println(entries)
	output := make([]DirEntry, 0, 500)

	for _, d := range entries {
		if d.Deleted {
			continue
		}
		output = append(output, d)
	}
	d := DirEntry{
		Name: ".",
		Mode: syscall.S_IFDIR,
		Ino: idtoino(inode.id),
	}
	output = append(output, d)

	// FIXME: take care of ".." entry
	// err = constor.Lstat(Path.Join(path, ".."), &stat)
	// d = DirEntry{
	// 	Name: "..",
	// 	Mode: syscall.S_IFDIR,
	// }
	// output = append(output, d)

	for i, _ := range output {
		output[i].Offset = uint64(i) + 1
	}
	fmt.Println(output)
	F := new(FD)
	F.stream = output
	constor.putfd(F)
	out.Fh = uint64(uintptr(unsafe.Pointer(F)))
	out.OpenFlags = 0
	return fuse.OK
}


func (constor *Constor) ReadDir(input *fuse.ReadIn, fuseout *fuse.DirEntryList) fuse.Status {
	constor.log("%d", input.Offset)
	ptr := uintptr(input.Fh)
	offset := input.Offset
	out := (*DirEntryList)(unsafe.Pointer(fuseout))

	F := constor.getfd(ptr)
	stream := F.stream
	if stream == nil {
		return fuse.EIO
	}
	if offset > uint64(len(stream)) {
		return fuse.EINVAL
	}
	todo := F.stream[offset:]
	for _, e := range todo {
		if e.Name == "" {
			continue
		}
		ok, _ := out.AddDirEntry(e)
		if !ok {
			break
		}
	}
	return fuse.OK
}

func (constor *Constor) ReleaseDir(input *fuse.ReleaseIn) {
	constor.log("")
	ptr := uintptr(input.Fh)
	constor.deletefd(ptr)
}

func (constor *Constor) Init(*fuse.Server) {
}

func (constor *Constor) String() string {
	return os.Args[0]
}

func (constor *Constor) SetDebug(dbg bool) {
}

func (constor *Constor) StatFs(header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	constor.log("%d", header.NodeId)
	path := constor.layers[0]
	s := syscall.Statfs_t{}
	err := syscall.Statfs(path, &s)
	if err == nil {
		out.Blocks = s.Blocks
		out.Bsize = uint32(s.Bsize)
		out.Bfree = s.Bfree
		out.Bavail = s.Bavail
		out.Files = s.Files
		out.Ffree = s.Ffree
		out.Frsize = uint32(s.Frsize)
		out.NameLen = uint32(s.Namelen)
		return fuse.OK
	} else {
		return fuse.ToStatus(err)
	}
}

func (constor *Constor) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	constor.log("%d %d", input.NodeId, input.Valid)
	var err error
	uid := -1
	gid := -1

	inode :=  constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		constor.error("inode nil")
		return fuse.EIO
	}

	// if ((input.Valid & fuse.FATTR_FH) !=0) && ((input.Valid & (fuse.FATTR_ATIME | fuse.FATTR_MTIME)) == 0) {
	if ((input.Valid & fuse.FATTR_FH) !=0) && ((input.Valid & fuse.FATTR_SIZE) != 0) {
		ptr := uintptr(input.Fh)
		F := constor.getfd(ptr)
		if F == nil {
			constor.error("F == nil")
			return fuse.EIO
		}
		if F.layer != 0 {
			constor.error("layer not 0")
			return fuse.EIO
		}
		constor.log("Ftruncate %d", ptr)
		err := syscall.Ftruncate(F.fd, int64(input.Size))
		if err != nil {
			constor.error("%s", err)
			return fuse.ToStatus(err)
		}
		stat := syscall.Stat_t{}
		err = syscall.Fstat(F.fd, &stat)
		if err != nil {
			constor.error("%s", err)
			return fuse.ToStatus(err)
		}
		attr := (*fuse.Attr)(&out.Attr)
		attr.FromStat(&stat)
		attr.Ino = idtoino(inode.id)
		return fuse.OK
	}

	if inode.layer == -1 {
		return fuse.ENOENT
	}

	if inode.layer != 0 {
		err = constor.copyup(inode)
		if err != nil {
			constor.error("%s", err)
			return fuse.ToStatus(err)
		}
	}

	stat := syscall.Stat_t{}
	path := constor.getPath(0, inode.id)

	// just to satisfy PJD tests
	if input.Valid == 0 {
		err = syscall.Lchown(path, uid, gid)
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	if input.Valid&fuse.FATTR_MODE != 0 {
		permissions := uint32(07777) & input.Mode
		err = syscall.Chmod(path, permissions)
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	if input.Valid&(fuse.FATTR_UID) != 0 {
		uid = int(input.Uid)
	}
	if input.Valid&(fuse.FATTR_GID) != 0 {
		gid = int(input.Gid)
	}

	if input.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		constor.log("%s %d %d", path, uid, gid)
		err = syscall.Lchown(path, uid, gid)
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	if input.Valid&fuse.FATTR_SIZE != 0 {
		err = syscall.Truncate(path, int64(input.Size))
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	if input.Valid&(fuse.FATTR_ATIME|fuse.FATTR_MTIME|fuse.FATTR_ATIME_NOW|fuse.FATTR_MTIME_NOW) != 0 {
		now := time.Now()
		var atime *time.Time
		var mtime *time.Time

		if input.Valid&fuse.FATTR_ATIME_NOW != 0 {
			atime = &now
		} else {
			t := time.Unix(int64(input.Atime), int64(input.Atimensec))
			atime = &t
		}

		if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
			mtime = &now
		} else {
			t := time.Unix(int64(input.Mtime), int64(input.Mtimensec))
			mtime = &t
		}
		fi, err := os.Lstat(path)
		if err != nil {
			return fuse.ToStatus(err)
		}
		if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
			// FIXME: there is no Lchtimes
			err = os.Chtimes(path, *atime, *mtime)
			if err != nil {
				constor.log("%s", err)
				return fuse.ToStatus(err)
			}
		} else {
			constor.error("Chtimes on Symlink not supported")
		}
	}
	attr := (*fuse.Attr)(&out.Attr)

	err = constor.Lstat(inode.layer, inode.id, &stat)
	if err != nil {
		return fuse.ToStatus(err)
	}
	attr.FromStat(&stat)
	attr.Ino = stat.Ino
	return fuse.ToStatus(err)
}

func (constor *Constor) Readlink(header *fuse.InHeader) (out []byte, code fuse.Status) {
	constor.log("%d", header.NodeId)
	inode := constor.inodemap.findInodePtr(header.NodeId)
	if inode == nil {
		return nil, fuse.ENOENT
	}
	path := constor.getPath(inode.layer, inode.id)
	link, err := os.Readlink(path)
	if err != nil {
		return []byte{}, fuse.ToStatus(err)
	}
	return []byte(link), fuse.OK
}

func (constor *Constor) Mknod(input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	constor.log("%d %s", input.NodeId, name)

	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		return fuse.ENOENT
	}
	err := constor.copyup(inode)
	if err != nil {
		return fuse.ToStatus(err)
	}
	dirpath := constor.getPath(0, inode.id)
	entrypath := Path.Join(dirpath, name)
	syscall.Unlink(entrypath) // remove a deleted entry
	err = syscall.Mknod(entrypath, input.Mode, int(input.Rdev))
	if err != nil {
		return fuse.ToStatus(err)
	}
	id := constor.setid(entrypath, "")
	if id == "" {
		return fuse.ENOENT
	}
	if err :=  constor.createPath(id); err != nil {
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, id)
	err = syscall.Mknod(path, input.Mode, int(input.Rdev))
	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Chown(path, int(input.Uid), int(input.Gid))
	if err != nil {
		return fuse.ToStatus(err)
	}
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, out)
}

func (constor *Constor) Mkdir(input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	constor.log("%d %s", input.NodeId, name)

	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		return fuse.ENOENT
	}
	err := constor.copyup(inode)
	if err != nil {
		return fuse.ToStatus(err)
	}
	dirpath := constor.getPath(0, inode.id)
	entrypath := Path.Join(dirpath, name)
	syscall.Unlink(entrypath) // remove a deleted entry
	err = syscall.Mkdir(entrypath, input.Mode)
	if err != nil {
		return fuse.ToStatus(err)
	}
	id := constor.setid(entrypath, "")
	if id == "" {
		return fuse.ENOENT
	}
	if err :=  constor.createPath(id); err != nil {
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, id)
	err = syscall.Mkdir(path, input.Mode)
	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Chown(path, int(input.Uid), int(input.Gid))
	if err != nil {
		return fuse.ToStatus(err)
	}
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, out)
}

func (constor *Constor) Unlink(header *fuse.InHeader, name string) (code fuse.Status) {
	constor.log("%d %s", header.NodeId, name)
	var stat syscall.Stat_t

	parent := constor.inodemap.findInodePtr(header.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}
	id, err := constor.getid(-1, parent.id, name)
	if err != nil {
		return fuse.ToStatus(err)
	}
	inode := constor.inodemap.findInodeId(id)
	if inode == nil {
		return fuse.ENOENT
	}
	if inode.layer == 0 {
		linkcnt, err := constor.declinkscnt(inode.id)
		if err != nil {
			return fuse.ToStatus(err)
		}
		if linkcnt == 0 {
			path := constor.getPath(0, inode.id)
			if err := syscall.Unlink(path); err != nil {
				return fuse.ToStatus(err)
			}
			inode.layer = -1
		}
	}
	// if there is an entry path, delete it
	entrypath := Path.Join(constor.getPath(0, parent.id), name)
	if err := syscall.Lstat(entrypath, &stat); err == nil {
		if err := syscall.Unlink(entrypath); err != nil {
			return fuse.ToStatus(err)
		}
	}
	// if the file is in a lower layer then create a deleted place holder file
	if _, err := constor.getid(-1, parent.id, name); err == nil {
		constor.setdeleted(entrypath)
	}
	return fuse.OK
}

func (constor *Constor) Rmdir(header *fuse.InHeader, name string) (code fuse.Status) {
	constor.log("%d %s", header.NodeId, name)
	var stat syscall.Stat_t
	parent := constor.inodemap.findInodePtr(header.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}
	id, err := constor.getid(-1, parent.id, name)
	if err != nil {
		return fuse.ToStatus(err)
	}
	inode := constor.inodemap.findInodeId(id)
	if inode == nil {
		return fuse.ENOENT
	}

	entries := map[string]DirEntry{}
	for li, _ := range constor.layers {
		path := constor.getPath(li, inode.id)
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		infos, _ := f.Readdir(0)
		for i := range infos {
			// workaround forhttps://code.google.com/p/go/issues/detail?id=5960
			if infos[i] == nil {
				continue
			}
			name := infos[i].Name()
			if _, ok := entries[name]; ok {
				// skip if the file was in upper layer
				continue
			}
			d := DirEntry {
				Name: name,
				Mode: uint32(infos[i].Mode()),
			}
			if constor.isdeleted(Path.Join(path, name)) {
				d.Deleted = true
			}
			id, err := constor.getid(li, inode.id, name)
			constor.log("%s", id)
			if err != nil {
				continue;
			}
			d.Ino = idtoino(id)
			entries[name] = d
		}
		f.Close()
	}
	fmt.Println(entries)
	output := make([]DirEntry, 0, 500)

	for _, d := range entries {
		if d.Deleted {
			continue
		}
		output = append(output, d)
	}

	if len(output) > 0 {
		return fuse.Status(syscall.ENOTEMPTY)
	}

	if inode.layer == 0 {
		path := constor.getPath(0, inode.id)
		if err := os.RemoveAll(path); err != nil {
			return fuse.ToStatus(err)
		}
	}
	entrypath := Path.Join(constor.getPath(0, parent.id), name)
	if err := syscall.Lstat(entrypath, &stat); err == nil {
		if err := syscall.Rmdir(entrypath); err != nil {
			return fuse.ToStatus(err)
		}
	}
	// if the file is in a lower layer then create a deleted place holder file
	if _, err := constor.getid(-1, parent.id, name); err == nil {
		constor.setdeleted(entrypath)
	}
	inode.layer = -1
	return fuse.OK
}

func (constor *Constor) Symlink(header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	constor.log("%d %s <- %s, uid: %d, gid: %d", header.NodeId, pointedTo, linkName, header.Uid, header.Gid)

	inode := constor.inodemap.findInodePtr(header.NodeId)
	if inode == nil {
		return fuse.ENOENT
	}
	err := constor.copyup(inode)
	if err != nil {
		return fuse.ToStatus(err)
	}
	dirpath := constor.getPath(0, inode.id)
	entrypath := Path.Join(dirpath, linkName)
	syscall.Unlink(entrypath) // remove a deleted entry
	err = syscall.Symlink(pointedTo, entrypath)
	if err != nil {
		return fuse.ToStatus(err)
	}
	id := constor.setid(entrypath, "")
	if id == "" {
		return fuse.ENOENT
	}
	if err :=  constor.createPath(id); err != nil {
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, id)
	err = syscall.Symlink(pointedTo, path)
	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Lchown(path, int(header.Uid), int(header.Gid))
	if err != nil {
		return fuse.ToStatus(err)
	}
	return constor.Lookup(header, linkName, out)
}

func (constor *Constor) Rename(input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	oldParent := constor.inodemap.findInodePtr(input.NodeId)
	if oldParent == nil {
		return fuse.ENOENT
	}
	newParent := constor.inodemap.findInodePtr(input.Newdir)
	if newParent == nil {
		return fuse.ENOENT
	}
	if err := constor.copyup(newParent); err != nil {
		return fuse.EIO
	}
	// remove any entry that existed in the newName's place
	if iddel, err := constor.getid(-1, newParent.id, newName); err == nil {
		if inodedel := constor.inodemap.findInodeId(iddel); inodedel != nil {
			if inodedel.layer == 0 {
				path := constor.getPath(0, iddel)
				fi, err := os.Lstat(path)
				if err != nil {
					return fuse.ToStatus(err)
				}
				if fi.IsDir() {
					return fuse.Status(syscall.EEXIST)
				}
				if err := syscall.Unlink(path); err != nil {
					return fuse.ToStatus(err)
				}
			}
			path := Path.Join(constor.getPath(0, newParent.id), newName)
			syscall.Unlink(path)
			inodedel.layer = -1
		} else {
			return fuse.EIO
		}
	}
	oldid, err := constor.getid(-1, oldParent.id, oldName)
	if err != nil {
		return fuse.ToStatus(err)
	}
	oldinode := constor.inodemap.findInodeId(oldid)
	if oldinode == nil {
		return fuse.ENOENT
	}
	path := constor.getPath(oldinode.layer, oldid)
	fi, err := os.Lstat(path)
	if err != nil {
		return fuse.ToStatus(err)
	}
	oldParentPath := constor.getPath(0, oldParent.id)
	oldentrypath := Path.Join(oldParentPath, oldName)
	oldstat := syscall.Stat_t{}
	if err := syscall.Lstat(oldentrypath, &oldstat); err == nil {
		if fi.IsDir() {
			if err := syscall.Rmdir(oldentrypath); err != nil {
				return fuse.ToStatus(err)
			}
		} else {
			if err := syscall.Unlink(oldentrypath); err != nil {
				return fuse.ToStatus(err)
			}
		}
	}
	if _, err := constor.getid(-1, oldParent.id, oldName); err == nil {
		constor.setdeleted(oldentrypath)
	}
	newParentPath := constor.getPath(0, newParent.id)
	newentrypath := Path.Join(newParentPath, newName)
	if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		err = os.Symlink("placeholder", newentrypath)
		if err != nil {
			return fuse.ToStatus(err)
		}
	} else if fi.Mode()&os.ModeDir == os.ModeDir {
		err := os.Mkdir(newentrypath, fi.Mode())
		if err != nil {
			return fuse.ToStatus(err)
		}
	} else {
		fd, err := syscall.Creat(newentrypath, uint32(fi.Mode()))
		if err != nil {
			return fuse.ToStatus(err)
		}
		syscall.Close(fd)
	}
	id := constor.setid(newentrypath, oldid)
	if id == "" {
		return fuse.EIO
	}
	return fuse.OK
}


func (constor *Constor) Link(input *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	constor.log("%d %d %s", input.Oldnodeid, input.NodeId, name)

	inodeold := constor.inodemap.findInodePtr(input.Oldnodeid)
	if inodeold == nil {
		return fuse.ENOENT
	}
	parent := constor.inodemap.findInodePtr(input.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}
	if err := constor.copyup(inodeold); err != nil {
		return fuse.ToStatus(err)
	}
	if err := constor.copyup(parent); err != nil {
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, parent.id)
	entrypath := Path.Join(path, name)

	if _, err := os.Lstat(entrypath); err == nil {
		//remove the deleted place holder file
		syscall.Unlink(entrypath)
	}
	if fd, err := syscall.Creat(entrypath, 0); err != nil {
		return fuse.ToStatus(err)
	} else {
		syscall.Close(fd)
	}
	id := constor.setid(entrypath, inodeold.id)
	if id == "" {
		return fuse.EIO
	}
	if err := constor.inclinkscnt(inodeold.id); err != nil {
		return fuse.ToStatus(err)
	}
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, out)
}

func (constor *Constor) GetXAttrSize(header *fuse.InHeader, attr string) (size int, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (constor *Constor) GetXAttrData(header *fuse.InHeader, attr string) (data []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (constor *Constor) SetXAttr(input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	return fuse.ENOSYS
}

func (constor *Constor) ListXAttr(header *fuse.InHeader) (data []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (constor *Constor) RemoveXAttr(header *fuse.InHeader, attr string) fuse.Status {
	if attr == "inodemap" {
		fmt.Println(constor.inodemap)
	}
	return fuse.OK
}

func (constor *Constor) Access(input *fuse.AccessIn) (code fuse.Status) {
	constor.error("%d", input.NodeId)
	return fuse.ENOSYS
	// FIXME: oops fix this
	// path, err := constor.getPath(input.NodeId)
	// if err != nil {
	// 	return fuse.ToStatus(err)
	// }
	// return fuse.ToStatus(syscall.Access(path, input.Mask))
}

func (constor *Constor) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {


	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		return fuse.ENOENT
	}
	err := constor.copyup(inode)
	if err != nil {
		return fuse.ToStatus(err)
	}
	dirpath := constor.getPath(0, inode.id)
	entrypath :=  Path.Join(dirpath, name)
	syscall.Unlink(entrypath) // remove a deleted entry
	fd, err := syscall.Creat(entrypath, input.Mode)
	if err != nil {
		return fuse.ToStatus(err)
	}
	syscall.Close(fd)
	id := constor.setid(entrypath, "")
	if id == "" {
		return fuse.EIO
	}
	if err :=  constor.createPath(id); err != nil {
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, id)
	fd, err = syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR, input.Mode)
	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Chown(path, int(input.Uid), int(input.Gid))
	if err != nil {
		return fuse.ToStatus(err)
	}
	F := new(FD)
	F.fd = fd
	F.layer = 0
	constor.putfd(F)
	out.Fh = uint64(uintptr(unsafe.Pointer(F)))
	constor.log("%d", out.Fh)
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, &out.EntryOut)
}

func (constor *Constor) Open(input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	inode :=  constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		return fuse.ENOENT
	}
	path :=  constor.getPath(inode.layer, inode.id)
	fd, err := syscall.Open(path, int(input.Flags), 0)
	if err != nil {
		constor.error("%s", err)
		return fuse.ToStatus(err)
	}
	F := new(FD)
	F.fd = fd
	F.flags = int(input.Flags)
	F.layer = inode.layer
	constor.putfd(F)
	out.Fh = uint64(uintptr(unsafe.Pointer(F)))
	out.OpenFlags = 0
	constor.log("%d", out.Fh)
	return fuse.OK
}

func (constor *Constor) Read(input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	constor.log("%d", input.Fh)
	ptr := uintptr(input.Fh)
	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		return nil, fuse.ENOENT
	}
	offset := input.Offset

	F := constor.getfd(ptr)

	if F == nil {
		constor.error("F == nil")
		return nil, fuse.EIO
	}

	if (F.layer != inode.layer) && (inode.layer == 0) {
		syscall.Close(F.fd)
		path := constor.getPath(0, inode.id)
		fd, err := syscall.Open(path, F.flags, 0)
		if err != nil {
			constor.error("%s", err)
			return nil, fuse.ToStatus(err)
		}
		F.fd = fd
		F.layer = 0
		constor.log("reset fd for %s", path)
	}
	if (F.layer != inode.layer) && (inode.layer >= 0) {
		return nil, fuse.EBADF
	}
	fd := F.fd
	_, err := syscall.Pread(fd, buf, int64(offset))
	if err != nil {
		constor.error("%s", err)
		return nil, fuse.ToStatus(err)
	}
	return fuse.ReadResultData(buf), fuse.OK
}

func (constor *Constor) Release(input *fuse.ReleaseIn) {
	constor.log("%d", input.Fh)
	ptr := uintptr(input.Fh)
	F := constor.getfd(ptr)
	if F == nil {
		return
	}
	fd := F.fd
	constor.deletefd(ptr)
	syscall.Close(fd)
}

func (constor *Constor) Write(input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	constor.log("%d", input.Fh)
	ptr := uintptr(input.Fh)
	offset := input.Offset

	F := constor.getfd(ptr)
	if F == nil {
		constor.error("F == nil")
		return 0, fuse.EIO
	}
	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		return 0, fuse.ENOENT
	}
	if F.layer != 0 && inode.layer != 0 {
		err := constor.copyup(inode)
		if err != nil {
			constor.error("%s", err)
			return 0, fuse.ToStatus(err)
		}
		path := constor.getPath(0, inode.id)
		syscall.Close(F.fd)
		fd, err := syscall.Open(path, F.flags, 0)
		if err != nil {
			constor.error("%s", err)
			return 0, fuse.ToStatus(err)
		}
		F.fd = fd
		F.layer = 0
		constor.log("reset fd for %s", path)
	} else if F.layer != 0 && inode.layer == 0 {
		syscall.Close(F.fd)
		path := constor.getPath(0, inode.id)
		fd, err := syscall.Open(path, F.flags, 0)
		if err != nil {
			constor.error("%s", err)
			return 0, fuse.ToStatus(err)
		}
		F.fd = fd
		F.layer = 0
		constor.log("reset fd for %s", path)
	}

	fd := F.fd
	n, err := syscall.Pwrite(fd, data, int64(offset))
	return uint32(n), fuse.ToStatus(err)
}

func (constor *Constor) Flush(input *fuse.FlushIn) fuse.Status {
	constor.log("")
	return fuse.OK
}

func (constor *Constor) Fsync(input *fuse.FsyncIn) (code fuse.Status) {
	constor.log("")
	return fuse.OK
}

func (constor *Constor) ReadDirPlus(input *fuse.ReadIn, fuseout *fuse.DirEntryList) fuse.Status {
	return fuse.ENOSYS
// 	constor.log("")
// 	constor.log("%d", input.Offset)
// 	ptr := uintptr(input.Fh)
// 	offset := input.Offset
// 	entryOut := fuse.EntryOut{}
// 	out := (*DirEntryList)(unsafe.Pointer(fuseout))

// 	F := constor.getfd(ptr)
// 	stream := F.stream
// 	if stream == nil {
// 		return fuse.EIO
// 	}
// 	if offset > uint64(len(stream)) {
// 		return fuse.EINVAL
// 	}
// 	todo := F.stream[offset:]
// 	for _, e := range todo {
// 		if e.Name == "" {
// 			continue
// 		}
// 		attr := (*fuse.Attr)(&entryOut.Attr)
// 		attr.FromStat(&e.Stat)
// 		entryOut.NodeId = attr.Ino
// 		entryOut.Ino = attr.Ino
// 		ok, _ := out.AddDirLookupEntry(e, &entryOut)
// 		if !ok {
// 			break
// 		}
// 	}
// 	return fuse.OK
}

func (constor *Constor) FsyncDir(input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

func (constor *Constor) Fallocate(in *fuse.FallocateIn) (code fuse.Status) {
	return fuse.ENOSYS
}

// var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// func randSeq(n int) string {
//     b := make([]rune, n)
// 	rand.Seed(time.Now().UTC().UnixNano())
//     for i := range b {
//         b[i] = letters[rand.Intn(len(letters))]
//     }
//     return string(b)
// }

func main() {
	// godaemon.MakeDaemon(&godaemon.DaemonAttr{})
	// log.SetFlags(log.Lshortfile)
	layers := os.Args[1]
	mountPoint := os.Args[2]

	// F, err := os.OpenFile("/tmp/constor", os.O_APPEND|os.O_WRONLY, 0)
	// F.Write([]byte("START\n"))
	// F.Write([]byte(layers))
	// F.Write([]byte(" "))
	// F.Write([]byte(mountPoint))
	// F.Write([]byte("\n"))

	if len(os.Args) != 3 {
		fmt.Println("Usage: constor /layer0:/layer1:....:/layerN /mnt/point")
		os.Exit(1)
	}

	pid := os.Getpid()
	pidstr := strconv.Itoa(pid)
	logf, err := os.Create("/tmp/constor.log." + pidstr)
	logfd := logf.Fd()
	syscall.Dup2(int(logfd), 1)
	syscall.Dup2(int(logfd), 2)


	constor := new(Constor)
	constor.inodemap = NewInodemap(constor)
	constor.fdmap = make(map[uintptr]*FD)
	constor.logf = logf
	constor.layers = strings.Split(layers, ":")

	err = os.MkdirAll(Path.Join(constor.layers[0], ROOTID), 0777)
	if err != nil && err != os.ErrExist {
		constor.error("Unable to mkdir %s", ROOTID)
		os.Exit(1)
	}

	constor.log("%s %s", layers, mountPoint)

	mOpts := &fuse.MountOptions{
		Name:    "constor",
		// SingleThreaded: true,
		Options: []string{"nonempty", "allow_other", "default_permissions", "user_id=0", "group_id=0", "fsname=" + constor.layers[0]},
	}
	_ = syscall.Umask(000)
	state, err := fuse.NewServer(constor, mountPoint, mOpts)
	if err != nil {
		// fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}
	// fmt.Println("Mounted!")
	state.Serve()
}
