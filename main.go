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
	"io"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/ncw/directio"
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
	ms 		  *fuse.Server
}

func (constor *Constor) Lookup(header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	var stat syscall.Stat_t
	if len(name) > 255 {
		constor.error("name too long : %s", name)
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	li := -1
	parent := constor.inodemap.findInodePtr(header.NodeId)
	if parent == nil {
		constor.error("Unable to find parent inode : %d", header.NodeId)
		return fuse.ENOENT
	}
	constor.log("%s(%s)", parent.id, name)
	id, err := constor.getid(-1, parent.id, name)
	if err != nil {
		// logging this error will produce too many logs as there will be too many
		// lookps on non-existant files
		return fuse.ToStatus(err)
	}
	inode := constor.inodemap.findInodeId(id)
	if inode != nil {
		li = inode.layer
	} else {
		li = constor.getLayer(id)
	}
	if li == -1 {
		constor.error("Unable to find inode for %s(%s) id %s", parent.id, name, id)
		return fuse.ENOENT
	}
	err = constor.Lstat(li, id, &stat)
	if err != nil {
		constor.error("Unable to Lstat inode for %s(%s) id %s", parent.id, name, id)
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
	out.EntryValid = 1000
	out.AttrValid = 1000
	constor.log("%s", id)
	return fuse.OK
}

func (constor *Constor) Forget(nodeID uint64, nlookup uint64) {
	if inode := constor.inodemap.findInodePtr(nodeID); inode != nil {
		constor.log("%s %d", inode.id, nlookup)
		inode.forget(nlookup)
	}
}

func (constor *Constor) GetAttr(input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	stat := syscall.Stat_t{}
	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		constor.error("%d not in inodemap", input.NodeId)
		return fuse.ENOENT
	}
	if inode.id == ROOTID && inode.layer == -1 {
		inode.layer = constor.getLayer(inode.id)
		if inode.layer == -1 {
			constor.error("Unable to find root inode")
			return fuse.ENOENT
		}
	}
	F := constor.fdlookup(inode.id, input.Pid)
	var err error
	// FIXME check to see if F.layer needs to be changed
	if F == nil  {
		if inode.layer == -1 {
			constor.error("layer is -1 for %s", inode.id)
			return fuse.ENOENT
		}
		constor.log("Lstat on %s", inode.id)
		err = constor.Lstat(inode.layer, inode.id, &stat)
	} else {
		constor.log("Fstat on %s", inode.id)
		err = syscall.Fstat(F.fd, &stat)
		stat.Ino = idtoino(F.id)
		// FIXME take care of hard links too
	}
	if err != nil {
		constor.error("%s: %s", inode.id, err)
		return fuse.ToStatus(err)
	}
	attr := (*fuse.Attr)(&out.Attr)
	attr.FromStat(&stat)
	out.AttrValid = 1000
	constor.log("%s", inode.id)
	return fuse.OK
}

func (constor *Constor) OpenDir(input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		constor.log("inode == nil for %d", input.NodeId)
		return fuse.ENOENT
	}
	constor.log("%s", inode.id)
	entries := map[string]DirEntry{}
	for li, _ := range constor.layers {
		path := constor.getPath(li, inode.id)
		stat := syscall.Stat_t{}
		err := syscall.Lstat(path, &stat)
		if err != nil {
			// continue aggregating upper layers
			continue
		}
		if (stat.Mode & syscall.S_IFMT) != syscall.S_IFDIR {
			constor.error("Not a dir: %s", path)
			break
		}

		f, err := os.Open(path)
		if err != nil {
			constor.error("Open failed on %s", path)
			break
		}
		infos, _ := f.Readdir(0) // reads all entries except "." and ".."
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
			if constor.isdeleted(Path.Join(path, name), infos[i].Sys().(*syscall.Stat_t)) {
				d.Deleted = true
			} else {
				id, err := constor.getid(li, inode.id, name)
				if err != nil {
					constor.error("getid failed on %d %s %s", li, inode.id, name)
					continue;
				}
				d.Ino = idtoino(id)

			}
			entries[name] = d
		}
		f.Close()
	}
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
	if F == nil {
		return fuse.ToStatus(syscall.ENOMEM)
	}
	F.stream = output
	constor.putfd(F)
	out.Fh = uint64(uintptr(unsafe.Pointer(F)))
	out.OpenFlags = 0
	return fuse.OK
}


func (constor *Constor) ReadDir(input *fuse.ReadIn, fuseout *fuse.DirEntryList) fuse.Status {
	ptr := uintptr(input.Fh)
	offset := input.Offset
	out := (*DirEntryList)(unsafe.Pointer(fuseout))

	F := constor.getfd(ptr)
	constor.log("%s %d", F.id, offset)
	stream := F.stream
	if stream == nil {
		constor.error("stream == nil for %s", F.id)
		return fuse.EIO
	}
	if offset > uint64(len(stream)) {
		constor.error("offset > %d for %s", len(stream), F.id)
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
	ptr := uintptr(input.Fh)
	F := constor.getfd(ptr)
	constor.log("%s", F.id)
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
	var err error
	uid := -1
	gid := -1

	inode :=  constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		constor.error("inode nil")
		return fuse.EIO
	}
	constor.log("%s %d", inode.id, input.Valid)
	// if ((input.Valid & fuse.FATTR_FH) !=0) && ((input.Valid & (fuse.FATTR_ATIME | fuse.FATTR_MTIME)) == 0) {
	if ((input.Valid & fuse.FATTR_FH) !=0) && ((input.Valid & fuse.FATTR_SIZE) != 0) {
		ptr := uintptr(input.Fh)
		F := constor.getfd(ptr)
		if F == nil {
			constor.error("F == nil for %s", inode.id)
			return fuse.EIO
		}
		if F.layer != 0 && inode.layer == -1 {
			/* FIXME handle this valid case */
			// file is in lower layer, opened, deleted, setattr-called
			constor.error("FSetAttr F.layer=%d inode.layer=%d", F.layer, inode.layer)
			return fuse.EIO
		}

		if F.layer != 0 && inode.layer != 0 {
			err := constor.copyup(inode)
			if err != nil {
				constor.error("copyup failed for %s - %s", inode.id, err)
				return fuse.ToStatus(err)
			}
			path := constor.getPath(0, inode.id)
			syscall.Close(F.fd)
			fd, err := syscall.Open(path, F.flags, 0)
			if err != nil {
				constor.error("open failed on %s - %s", path, err)
				return fuse.ToStatus(err)
			}
			F.fd = fd
			F.layer = 0
			constor.log("reset fd for %s", path)
		} else if F.layer != 0 && inode.layer == 0 {
			// when some other process already has done a copyup
			syscall.Close(F.fd)
			path := constor.getPath(0, inode.id)
			fd, err := syscall.Open(path, F.flags, 0)
			if err != nil {
				constor.error("open failed on %s - %s", path, err)
				return fuse.ToStatus(err)
			}
			F.fd = fd
			F.layer = 0
			constor.log("reset fd for %s", path)
		}

		if F.layer != 0 {
			constor.error("layer not 0")
			return fuse.EIO
		}

		if input.Valid&fuse.FATTR_MODE != 0 {
			permissions := uint32(07777) & input.Mode
			err = syscall.Fchmod(F.fd, permissions)
			if err != nil {
				constor.error("Fchmod failed on %s - %d : %s", F.id, permissions, err)
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
			err = syscall.Fchown(F.fd, uid, gid)
			if err != nil {
				constor.error("Fchown failed on %s - %d %d : %s", F.id, uid, gid, err)
				return fuse.ToStatus(err)
			}
		}
		if input.Valid&fuse.FATTR_SIZE != 0 {
			err := syscall.Ftruncate(F.fd, int64(input.Size))
			if err != nil {
				constor.error("Ftruncate failed on %s - %d : %s", F.id, input.Size, err)
				return fuse.ToStatus(err)
			}
		}
		if input.Valid&(fuse.FATTR_ATIME|fuse.FATTR_MTIME|fuse.FATTR_ATIME_NOW|fuse.FATTR_MTIME_NOW) != 0 {
			now := time.Now()
			var tv []syscall.Timeval

			tv = make([]syscall.Timeval, 2)

			if input.Valid&fuse.FATTR_ATIME_NOW != 0 {
				tv[0].Sec = now.Unix()
				tv[0].Usec = now.UnixNano() / 1000
			} else {
				tv[0].Sec = int64(input.Atime)
				tv[0].Usec = int64(input.Atimensec / 1000)
			}

			if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
				tv[1].Sec = now.Unix()
				tv[1].Usec = now.UnixNano() / 1000
			} else {
				tv[1].Sec = int64(input.Atime)
				tv[1].Usec = int64(input.Atimensec / 1000)
			}

			err := syscall.Futimes(F.fd, tv)
			if err != nil {
				constor.error("Futimes failed on %s : %s", F.id, err)
				return fuse.ToStatus(err)
			}
		}

		stat := syscall.Stat_t{}
		err = syscall.Fstat(F.fd, &stat)
		if err != nil {
			constor.error("Fstat failed on %s : %s", F.id, err)
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
			constor.error("copyup failed for %s - %s", inode.id, err)
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
			constor.error("Lchmod failed on %s - %d : %s", path, permissions, err)
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
			constor.error("Lchown failed on %s - %d %d : %s", path, uid, gid, err)
			return fuse.ToStatus(err)
		}
	}
	if input.Valid&fuse.FATTR_SIZE != 0 {
		err = syscall.Truncate(path, int64(input.Size))
		if err != nil {
			constor.error("Truncate failed on %s - %d : %s", path, input.Size, err)
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
				constor.error("Chtimes failed on %s : %s", path, err)
				return fuse.ToStatus(err)
			}
		} else {
			constor.error("Chtimes on Symlink not supported")
		}
	}
	attr := (*fuse.Attr)(&out.Attr)

	err = constor.Lstat(inode.layer, inode.id, &stat)
	if err != nil {
		constor.error("Lstat failed on %s : %s", inode.id, err)
		return fuse.ToStatus(err)
	}
	attr.FromStat(&stat)
	attr.Ino = stat.Ino
	return fuse.ToStatus(err)
}

func (constor *Constor) Readlink(header *fuse.InHeader) (out []byte, code fuse.Status) {
	inode := constor.inodemap.findInodePtr(header.NodeId)
	if inode == nil {
		constor.error("inode == nil")
		return nil, fuse.ENOENT
	}
	constor.log("%s", inode.id)
	path := constor.getPath(inode.layer, inode.id)
	link, err := os.Readlink(path)
	if err != nil {
		constor.error("Failed on %s : %s", path, err)
		return []byte{}, fuse.ToStatus(err)
	}
	return []byte(link), fuse.OK
}

func (constor *Constor) Mknod(input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		constor.error("inode == nil")
		return fuse.ENOENT
	}
	constor.log("%s %s", inode.id, name)
	err := constor.copyup(inode)
	if err != nil {
		constor.error("copyup failed on %s : ", inode.id, err)
		return fuse.ToStatus(err)
	}
	dirpath := constor.getPath(0, inode.id)
	entrypath := Path.Join(dirpath, name)
	syscall.Unlink(entrypath) // remove a deleted entry
	err = syscall.Mknod(entrypath, input.Mode, int(input.Rdev))
	if err != nil {
		constor.error("Failed on %s : %s", entrypath, err)
		return fuse.ToStatus(err)
	}
	id := constor.setid(entrypath, "")
	if id == "" {
		constor.error("setid failed on %s", entrypath)
		return fuse.ENOENT
	}
	if err :=  constor.createPath(id); err != nil {
		constor.error("createPath failed on %s : %s", id, err)
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, id)
	err = syscall.Mknod(path, input.Mode, int(input.Rdev))
	if err != nil {
		constor.error("Mknod failed on %s : %s", path, err)
		return fuse.ToStatus(err)
	}
	err = syscall.Chown(path, int(input.Uid), int(input.Gid))
	if err != nil {
		constor.error("Chown failed on %s : %s", path, err)
		return fuse.ToStatus(err)
	}
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, out)
}

func (constor *Constor) Mkdir(input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		constor.error("inode == nil")
		return fuse.ENOENT
	}
	constor.log("%s %s", inode.id, name)
	err := constor.copyup(inode)
	if err != nil {
		constor.error("copyup failed on %s : ", inode.id, err)
		return fuse.ToStatus(err)
	}
	dirpath := constor.getPath(0, inode.id)
	entrypath := Path.Join(dirpath, name)
	syscall.Unlink(entrypath) // remove a deleted entry
	err = syscall.Mkdir(entrypath, input.Mode)
	if err != nil {
		constor.error("Failed on %s : %s", entrypath, err)
		return fuse.ToStatus(err)
	}
	id := constor.setid(entrypath, "")
	if id == "" {
		constor.error("setid failed on %s", entrypath)
		return fuse.ENOENT
	}
	if err :=  constor.createPath(id); err != nil {
		constor.error("createPath failed on %s : %s", id, err)
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, id)
	err = syscall.Mkdir(path, input.Mode)
	if err != nil {
		constor.error("Mkdir failed on %s : %s", path, err)
		return fuse.ToStatus(err)
	}
	err = syscall.Chown(path, int(input.Uid), int(input.Gid))
	if err != nil {
		constor.error("Chown failed on %s : %s", path, err)
		return fuse.ToStatus(err)
	}
	return constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), name, out)
}

func (constor *Constor) Unlink(header *fuse.InHeader, name string) (code fuse.Status) {
	var stat syscall.Stat_t

	parent := constor.inodemap.findInodePtr(header.NodeId)
	if parent == nil {
		constor.error("parent == nil")
		return fuse.ENOENT
	}
	constor.log("%s %s", parent.id, name)
	id, err := constor.getid(-1, parent.id, name)
	if err != nil {
		constor.error("getid failed %s %s", parent.id, name)
		return fuse.ToStatus(err)
	}
	inode := constor.inodemap.findInodeId(id)
	if inode == nil {
		constor.error("%s %s : inode == nil", parent.id, name)
		return fuse.ENOENT
	}
	if inode.layer == 0 {
		linkcnt, err := constor.declinkscnt(inode.id)
		if err != nil {
			constor.error("declinkscnt %s : %s", inode.id, err)
			return fuse.ToStatus(err)
		}
		if linkcnt == 0 {
			path := constor.getPath(0, inode.id)
			if err := syscall.Unlink(path); err != nil {
				constor.error("Unlink failed for %s : %s", path, err)
				return fuse.ToStatus(err)
			}
			inode.layer = -1
		}
	}
	err = constor.copyup(parent)
	if err != nil {
		constor.error("copyup failed on %s : ", parent.id, err)
		return fuse.ToStatus(err)
	}
	// if there is an entry path, delete it
	entrypath := Path.Join(constor.getPath(0, parent.id), name)
	if err := syscall.Lstat(entrypath, &stat); err == nil {
		if err := syscall.Unlink(entrypath); err != nil {
			constor.error("Unlink failed for %s : %s", entrypath, err)
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
		constor.error("parent == nil")
		return fuse.ENOENT
	}
	constor.log("%s %s", parent.id, name)
	id, err := constor.getid(-1, parent.id, name)
	if err != nil {
		constor.error("getid failed %s %s", parent.id, name)
		return fuse.ToStatus(err)
	}
	inode := constor.inodemap.findInodeId(id)
	if inode == nil {
		constor.error("%s %s : inode == nil", parent.id, name)
		return fuse.ENOENT
	}

	entries := map[string]DirEntry{}
	for li, _ := range constor.layers {
		path := constor.getPath(li, inode.id)
		stat := syscall.Stat_t{}
		err := syscall.Lstat(path, &stat)
		if err != nil {
			// this means that the directory was created on the layer above this layer
			break
		}
		if (stat.Mode & syscall.S_IFMT) != syscall.S_IFDIR {
			constor.error("Not a dir: %s", path)
			break
		}

		f, err := os.Open(path)
		if err != nil {
			constor.error("Open failed on %s", path)
			break
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
			if constor.isdeleted(Path.Join(path, name), infos[i].Sys().(*syscall.Stat_t)) {
				d.Deleted = true
			} else {
				id, err := constor.getid(li, inode.id, name)
				if err != nil {
					constor.error("getid failed on %d %s %s", li, inode.id, name)
					continue;
				}
				d.Ino = idtoino(id)
			}
			entries[name] = d
		}
		f.Close()
	}
	output := make([]DirEntry, 0, 500)

	for _, d := range entries {
		if d.Deleted {
			continue
		}
		output = append(output, d)
	}

	if len(output) > 0 {
		constor.error("Directory not empty %s %s", parent.id, name)
		return fuse.Status(syscall.ENOTEMPTY)
	}

	if inode.layer == 0 {
		path := constor.getPath(0, inode.id)
		if err := os.RemoveAll(path); err != nil {
			constor.error("RemoveAll on %s : %s", path, err)
			return fuse.ToStatus(err)
		}
	}
	err = constor.copyup(parent)
	if err != nil {
		constor.error("copyup failed for %s - %s", parent.id, err)
		return fuse.ToStatus(err)
	}
	entrypath := Path.Join(constor.getPath(0, parent.id), name)
	if err := syscall.Lstat(entrypath, &stat); err == nil {
		if err := syscall.Rmdir(entrypath); err != nil {
			constor.error("Rmdir on %s : %s", entrypath, err)
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
	inode := constor.inodemap.findInodePtr(header.NodeId)
	if inode == nil {
		constor.error("inode == nil")
		return fuse.ENOENT
	}
	err := constor.copyup(inode)
	if err != nil {
		constor.error("copyup failed for %s - %s", inode.id, err)
		return fuse.ToStatus(err)
	}
	dirpath := constor.getPath(0, inode.id)
	entrypath := Path.Join(dirpath, linkName)

	constor.log("%s <- %s/%s", pointedTo, inode.id, linkName)
	syscall.Unlink(entrypath) // remove a deleted entry
	err = syscall.Symlink(pointedTo, entrypath)
	if err != nil {
		constor.error("Symlink failed %s <- %s : %s", pointedTo, entrypath, err)
		return fuse.ToStatus(err)
	}
	id := constor.setid(entrypath, "")
	if id == "" {
		constor.error("setid failed on %s", entrypath)
		return fuse.ENOENT
	}
	if err :=  constor.createPath(id); err != nil {
		constor.error("createPath failed on %s : %s", id, err)
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, id)
	err = syscall.Symlink(pointedTo, path)
	if err != nil {
		constor.error("Symlink failed %s <- %s : %s", pointedTo, path, err)
		return fuse.ToStatus(err)
	}
	err = syscall.Lchown(path, int(header.Uid), int(header.Gid))
	if err != nil {
		constor.error("Chown failed on %s : %s", path, err)
		return fuse.ToStatus(err)
	}
	return constor.Lookup(header, linkName, out)
}

func (constor *Constor) Rename(input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	sendEntryNotify := false
	var inodedel *Inode
	oldParent := constor.inodemap.findInodePtr(input.NodeId)
	if oldParent == nil {
		constor.error("oldParent == nil")
		return fuse.ENOENT
	}
	newParent := constor.inodemap.findInodePtr(input.Newdir)
	if newParent == nil {
		constor.error("newParent == nil")
		return fuse.ENOENT
	}
	if err := constor.copyup(newParent); err != nil {
		constor.error("copyup failed for %s - %s", newParent.id, err)
		return fuse.EIO
	}
	newParentPath := constor.getPath(0, newParent.id)
	newentrypath := Path.Join(newParentPath, newName)
	constor.log("%s %s %s %s", oldParent.id, oldName, newParent.id, newName)
	// remove any entry that existed in the newName's place
	if iddel, err := constor.getid(-1, newParent.id, newName); err == nil {
		if inodedel = constor.inodemap.findInodeId(iddel); inodedel != nil {
			if inodedel.layer == 0 {
				linkcnt, err := constor.declinkscnt(inodedel.id)
				if err != nil {
					constor.error("declinkscnt %s : %s", inodedel.id, err)
					return fuse.ToStatus(err)
				}
				if linkcnt == 0 {
					path := constor.getPath(0, iddel)
					fi, err := os.Lstat(path)
					if err != nil {
						constor.error("Lstat failed on %s", path)
						return fuse.ToStatus(err)
					}
					if fi.IsDir() {
						// FIXME: take care of this situation
						constor.error("path is a directory")
						return fuse.Status(syscall.EEXIST)
					}
					if err := syscall.Unlink(path); err != nil {
						constor.error("Unable to remove %s", path)
						return fuse.ToStatus(err)
					}
					inodedel.layer = -1
				}
			}
			stat := syscall.Stat_t{}
			// FIXME do copyup and declinkscnt
			if err := syscall.Lstat(newentrypath, &stat); err == nil {
				fi, err := os.Lstat(newentrypath)
				if err != nil {
					constor.error("Lstat failed on %s", newentrypath)
					return fuse.ToStatus(err)
				}
				if fi.IsDir() {
					// FIXME: take care of this situation
					constor.error("path is a directory")
					return fuse.Status(syscall.EEXIST)
				}
				if err := syscall.Unlink(newentrypath); err != nil {
					constor.error("Unable to remove %s", newentrypath)
					return fuse.ToStatus(err)
				}
			}
			// inodedel.parentPtr = input.Newdir
			// inodedel.name = newName
			sendEntryNotify = true
			// constor.ms.DeleteNotify(input.Newdir, uint64(uintptr(unsafe.Pointer(inodedel))), newName)
			// constor.ms.EntryNotify(input.Newdir, newName)
		} else {
			constor.error("inodedel == nil for %s %s", newParent.id, newName)
			return fuse.EIO
		}
	}
	// remove any deleted placeholder
	if constor.isdeleted(newentrypath, nil) {
		if err := syscall.Unlink(newentrypath); err != nil {
			constor.error("Unlink %s : %s", newentrypath, err)
			return fuse.ToStatus(err)
		}
	}
	oldid, err := constor.getid(-1, oldParent.id, oldName)
	if err != nil {
		constor.error("getid error %s %s", oldParent.id, oldName)
		return fuse.ToStatus(err)
	}
	oldinode := constor.inodemap.findInodeId(oldid)
	if oldinode == nil {
		constor.error("oldinode == nil for %s", oldid)
		return fuse.ENOENT
	}
	path := constor.getPath(oldinode.layer, oldid)
	fi, err := os.Lstat(path)
	if err != nil {
		constor.error("Lstat %s", path)
		return fuse.ToStatus(err)
	}
	oldParentPath := constor.getPath(0, oldParent.id)
	oldentrypath := Path.Join(oldParentPath, oldName)
	oldstat := syscall.Stat_t{}
	if err := syscall.Lstat(oldentrypath, &oldstat); err == nil {
		if fi.IsDir() {
			if err := syscall.Rmdir(oldentrypath); err != nil {
				constor.error("Rmdir %s : %s", oldentrypath, err)
				return fuse.ToStatus(err)
			}
		} else {
			if err := syscall.Unlink(oldentrypath); err != nil {
				constor.error("Unlink %s : %s", oldentrypath, err)
				return fuse.ToStatus(err)
			}
		}
	}
	if _, err := constor.getid(-1, oldParent.id, oldName); err == nil {
		constor.setdeleted(oldentrypath)
	}
	if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		err = os.Symlink("placeholder", newentrypath)
		if err != nil {
			constor.error("Symlink %s : %s", newentrypath, err)
			return fuse.ToStatus(err)
		}
	} else if fi.Mode()&os.ModeDir == os.ModeDir {
		err := os.Mkdir(newentrypath, fi.Mode())
		if err != nil {
			constor.error("Mkdir %s : %s", newentrypath, err)
			return fuse.ToStatus(err)
		}
	} else {
		fd, err := syscall.Creat(newentrypath, uint32(fi.Mode()))
		if err != nil {
			constor.error("create %s : %s", newentrypath, err)
			return fuse.ToStatus(err)
		}
		syscall.Close(fd)
	}
	id := constor.setid(newentrypath, oldid)
	if id == "" {
		constor.error("setid %s : %s", newentrypath, err)
		return fuse.EIO
	}
	if sendEntryNotify {
		go func() {
			// FIXME: is this needed?
			constor.ms.DeleteNotify(input.Newdir, uint64(uintptr(unsafe.Pointer(inodedel))), newName)
			constor.ms.DeleteNotify(input.NodeId, uint64(uintptr(unsafe.Pointer(oldinode))), oldName)
		}()
	}
	return fuse.OK
}


func (constor *Constor) Link(input *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	inodeold := constor.inodemap.findInodePtr(input.Oldnodeid)
	if inodeold == nil {
		constor.error("inodeold == nil")
		return fuse.ENOENT
	}
	parent := constor.inodemap.findInodePtr(input.NodeId)
	if parent == nil {
		constor.error("parent == nil")
		return fuse.ENOENT
	}
	constor.log("%s <- %s/%s", inodeold.id, parent.id, name)
	if err := constor.copyup(inodeold); err != nil {
		constor.error("copyup failed for %s - %s", inodeold.id, err)
		return fuse.ToStatus(err)
	}
	if err := constor.copyup(parent); err != nil {
		constor.error("copyup failed for %s - %s", parent.id, err)
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, parent.id)
	entrypath := Path.Join(path, name)

	if constor.isdeleted(entrypath, nil) {
		if err := syscall.Unlink(entrypath); err != nil {
			constor.error("Unlink %s : %s", entrypath, err)
			return fuse.ToStatus(err)
		}
	}

	if fd, err := syscall.Creat(entrypath, 0); err != nil {
		constor.error("Creat %s : %s", entrypath, err)
		return fuse.ToStatus(err)
	} else {
		syscall.Close(fd)
	}
	id := constor.setid(entrypath, inodeold.id)
	if id == "" {
		constor.error("setid %s : %s", entrypath)
		return fuse.EIO
	}
	if err := constor.inclinkscnt(inodeold.id); err != nil {
		constor.error("inclinkscnt %s : %s", inodeold.id, err)
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
	flags := 0

	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		constor.error("inode == nil")
		return fuse.ENOENT
	}
	err := constor.copyup(inode)
	if err != nil {
		constor.error("copyup failed for %s - %s", inode.id, err)
		return fuse.ToStatus(err)
	}
	dirpath := constor.getPath(0, inode.id)
	entrypath :=  Path.Join(dirpath, name)

	if constor.isdeleted(entrypath, nil) {
		if err := syscall.Unlink(entrypath); err != nil {
			constor.error("Unlink %s : %s", entrypath, err)
			return fuse.ToStatus(err)
		}
	}

	fd, err := syscall.Creat(entrypath, input.Mode)
	if err != nil {
		constor.error("Creat %s : %s", entrypath, err)
		return fuse.ToStatus(err)
	}
	syscall.Close(fd)
	id := constor.setid(entrypath, "")
	if id == "" {
		constor.error("setid %s : %s", entrypath, err)
		return fuse.EIO
	}
	constor.log("%s : %s", entrypath, id)
	if err :=  constor.createPath(id); err != nil {
		constor.error("createPath %s : %s", id, err)
		return fuse.ToStatus(err)
	}
	path := constor.getPath(0, id)
	if input.Flags != 0 {
		flags = int(input.Flags) | syscall.O_CREAT
	} else {
		flags = syscall.O_CREAT | syscall.O_RDWR | syscall.O_EXCL
	}
	fd, err = syscall.Open(path, flags, input.Mode)
	// fd, err = syscall.Open(path, int(input.Flags), input.Mode)
	if err != nil {
		constor.error("open %s : %s", path, err)
		return fuse.ToStatus(err)
	}
	err = syscall.Chown(path, int(input.Uid), int(input.Gid))
	if err != nil {
		constor.error("Chown %s : %s", path, err)
		return fuse.ToStatus(err)
	}
	F := new(FD)
	F.fd = fd
	F.layer = 0
	F.id = id
	F.pid = input.Pid
	constor.putfd(F)
	if flags & syscall.O_DIRECT != 0 {
		out.OpenFlags = fuse.FOPEN_DIRECT_IO
	} else {
		out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	}

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
	constor.log("%s %d", path, input.Flags)
	fd, err := syscall.Open(path, int(input.Flags), 0)
	if err != nil {
		constor.error("open failed %s : %s", path, err)
		return fuse.ToStatus(err)
	}
	F := new(FD)
	F.fd = fd
	F.flags = int(input.Flags)
	F.layer = inode.layer
	F.id = inode.id
	F.pid = input.Pid
	constor.putfd(F)
	out.Fh = uint64(uintptr(unsafe.Pointer(F)))
	if input.Flags & syscall.O_DIRECT != 0 {
		out.OpenFlags = fuse.FOPEN_DIRECT_IO
	} else {
		out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	}

	constor.log("%d", out.Fh)
	return fuse.OK
}

func (constor *Constor) Read(input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	constor.log("%d %d", input.Fh, len(buf))
	ptr := uintptr(input.Fh)
	inode := constor.inodemap.findInodePtr(input.NodeId)
	if inode == nil {
		constor.error("inode == nil")
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
			constor.error("open failed %s : %s", path, err)
			return nil, fuse.ToStatus(err)
		}
		F.fd = fd
		F.layer = 0
		constor.log("reset fd for %s", path)
	}
	if (F.layer != inode.layer) && (inode.layer >= 0) {
		constor.error("%s : %d", F.id, inode.layer)
		return nil, fuse.EBADF
	}
	fd := F.fd
	n, err := syscall.Pread(fd, buf, int64(offset))
	if err != nil && err != io.EOF {
		constor.error("%s", err)
		return nil, fuse.ToStatus(err)
	}
	return fuse.ReadResultData(buf[:n]), fuse.OK
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
	constor.log("%d %d", input.Fh, len(data))
	ptr := uintptr(input.Fh)
	offset := input.Offset
	wdata := data

	F := constor.getfd(ptr)
	if F == nil {
		constor.error("F == nil")
		return 0, fuse.EIO
	}
	if F.flags & syscall.O_DIRECT != 0 {
		wdata = directio.AlignedBlock(len(data))
		copy(wdata, data)
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
	n, err := syscall.Pwrite(fd, wdata, int64(offset))
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
	constor.log("")
	constor.log("%d", input.Offset)
	ptr := uintptr(input.Fh)
	offset := input.Offset
	entryOut := fuse.EntryOut{}
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
		// attr := (*fuse.Attr)(&entryOut.Attr)
		// attr.FromStat(&e.Stat)
		// entryOut.NodeId = attr.Ino
		// entryOut.Ino = attr.Ino
		constor.Lookup((*fuse.InHeader)(unsafe.Pointer(input)), e.Name, &entryOut)
		ok, _ := out.AddDirLookupEntry(e, &entryOut)
		if !ok {
			break
		}
	}
	return fuse.OK
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
	// defer profile.Start(profile.CPUProfile).Stop()
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
	// logf, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
	logfd := logf.Fd()
	syscall.Dup2(int(logfd), 1)
	syscall.Dup2(int(logfd), 2)


	constor := new(Constor)
	constor.inodemap = NewInodemap(constor)
	constor.fdmap = make(map[uintptr]*FD)
	constor.logf = logf
	constor.layers = strings.Split(layers, ":")

	numlayers := len(constor.layers)
	if len(constor.layers[numlayers-1]) == 0 {
		numlayers--
		constor.layers = constor.layers[:numlayers]
	}

	err = os.MkdirAll(Path.Join(constor.layers[0], ROOTID), 0777)
	if err != nil && err != os.ErrExist {
		constor.error("Unable to mkdir %s", ROOTID)
		os.Exit(1)
	}

	constor.log("%s %s", layers, mountPoint)

	mOpts := &fuse.MountOptions{
		Name:    "constor",
		// SingleThreaded: true,
		// Options: []string{"nonempty", "allow_other", "default_permissions", "user_id=0", "group_id=0", "fsname=" + constor.layers[0], "kernel_cache", "entry_timeout=100.0", "attr_timeout=100.0"},
		Options: []string{"nonempty", "allow_other", "default_permissions", "user_id=0", "group_id=0", "fsname=" + constor.layers[0]},
	}
	_ = syscall.Umask(000)
	state, err := fuse.NewServer(constor, mountPoint, mOpts)
	if err != nil {
		// fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}
	constor.ms = state
	state.Serve()
}
