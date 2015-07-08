package main

import (
	"fmt"
	"io"
	"os"
	Path "path"
	"runtime"
	"syscall"
	"unsafe"
	"strconv"
)


func (constor *Constor) log(format string, a ...interface{}) {
	pc, file, line, _ := runtime.Caller(1)
	info := fmt.Sprintf(format, a...)
	funcName := runtime.FuncForPC(pc).Name()
	fmt.Fprintf(constor.logf, "%s:%d:%s %v\n", Path.Base(file), line, funcName, info)
}

func (constor *Constor) error(format string, a ...interface{}) {
	pc, file, line, _ := runtime.Caller(1)
	info := fmt.Sprintf(format, a...)
	funcName := runtime.FuncForPC(pc).Name()
	fmt.Fprintf(constor.logf, "ERR %s:%d:%s %v\n", Path.Base(file), line, funcName, info)
}

func Lgetxattr(path string, attr string) ([]byte, error) {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return nil, err
	}
	attrBytes, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return nil, err
	}
	dest := make([]byte, 128)
	destBytes := unsafe.Pointer(&dest[0])
	sz, _, errno := syscall.Syscall6(syscall.SYS_LGETXATTR, uintptr(unsafe.Pointer(pathBytes)), uintptr(unsafe.Pointer(attrBytes)), uintptr(destBytes), uintptr(len(dest)), 0, 0)
	if errno == syscall.ENODATA {
		return nil, nil
	}
	if errno == syscall.ERANGE {
		dest = make([]byte, sz)
		destBytes := unsafe.Pointer(&dest[0])
		sz, _, errno = syscall.Syscall6(syscall.SYS_LGETXATTR, uintptr(unsafe.Pointer(pathBytes)), uintptr(unsafe.Pointer(attrBytes)), uintptr(destBytes), uintptr(len(dest)), 0, 0)
	}
	if errno != 0 {
		return nil, errno
	}

	return dest[:sz], nil
}

var _zero uintptr

func Lsetxattr(path string, attr string, data []byte, flags int) error {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return err
	}
	attrBytes, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return err
	}
	var dataBytes unsafe.Pointer
	if len(data) > 0 {
		dataBytes = unsafe.Pointer(&data[0])
	} else {
		dataBytes = unsafe.Pointer(&_zero)
	}
	_, _, errno := syscall.Syscall6(syscall.SYS_LSETXATTR, uintptr(unsafe.Pointer(pathBytes)), uintptr(unsafe.Pointer(attrBytes)), uintptr(dataBytes), uintptr(len(data)), uintptr(flags), 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func (constor *Constor) inclinkscnt(id string) error {
	count := 1
	path := constor.getPath(0, id)
	linksbyte, err := Lgetxattr(path, LINKSXATTR)
	if err == nil && len(linksbyte) != 0 {
		linksstr := string(linksbyte)
		linksint, err :=  strconv.Atoi(linksstr)
		if err != nil {
			fmt.Println(err)
			return err
		}
		count = linksint
	}
	count++
	linksstr := strconv.Itoa(count)
	linksbyte = []byte(linksstr)
	err = Lsetxattr(path, LINKSXATTR, linksbyte, 0)
	fmt.Println(err)
	return err
}

func (constor *Constor) declinkscnt(id string) (int, error) {
	count := 0
	path := constor.getPath(0, id)
	linksbyte, err := Lgetxattr(path, LINKSXATTR)
	if err == nil && len(linksbyte) != 0 {
		linksstr := string(linksbyte)
		linksint, err :=  strconv.Atoi(linksstr)
		if err != nil {
			return 0, err
		}
		count = linksint
	} else {
		return 0, nil
	}
	count--
	linksstr := strconv.Itoa(count)
	linksbyte = []byte(linksstr)
	err = Lsetxattr(path, LINKSXATTR, linksbyte, 0)
	return count, err
}

func (constor *Constor) setdeleted(path string) error {
	stat := syscall.Stat_t{}
	err := syscall.Stat(path, &stat)
	if err != nil {
		fd, err := syscall.Creat(path, 0)
		if err != nil {
			return err
		}
		syscall.Close(fd)
	}
	return syscall.Setxattr(path, DELXATTR, []byte{49}, 0)
}

func (constor *Constor) isdeleted(path string) bool {
	var inobyte []byte
	inobyte = make([]byte, 100, 100)
	if _, err := syscall.Getxattr(path, DELXATTR, inobyte); err == nil {
		return true
	} else {
		return false
	}
}

func (constor *Constor) getLayer(id string) int {
	for i, _ := range constor.layers {
		path := constor.getPath(i, id)
		if constor.isdeleted(path) {
			return -1
		}
		if _, err := os.Lstat(path); err == nil {
			return i
		}
	}
	return -1
}

// func (constor *Constor) getPath(li int, id string) string {
// 	return Path.Join(constor.layers[li], id[:2], id[2:4], id)
// }

func (constor *Constor) getPath(li int, id string) string {
	return Path.Join(constor.layers[li], id)
}

func (constor *Constor) Lstat(li int, id string, stat *syscall.Stat_t) error {
	path :=  constor.getPath(li, id)
	if err := syscall.Lstat(path, stat); err != nil {
		return err
	}
	count := 1
	linksbyte, err := Lgetxattr(path, LINKSXATTR)
	if err == nil && len(linksbyte) != 0 {
		linksstr := string(linksbyte)
		linksint, err :=  strconv.Atoi(linksstr)
		if err != nil {
			return err
		}
		count = linksint
	}
	stat.Nlink = uint64(count)
	stat.Ino = idtoino(id)
	return nil
}

func (constor *Constor) getid(li int, id string, name string) (string, error) {
	if li != -1 {
		dirpath := constor.getPath(li, id)
		path := Path.Join(dirpath, name)
		if constor.isdeleted(path) {
			return "", syscall.ENOENT
		}
		inobyte, err := Lgetxattr(path, IDXATTR)
		if err != nil || len(inobyte) == 0 {
			return "", syscall.ENOENT
		}
		return string(inobyte), nil
	}
	for li, _ := range constor.layers {
		dirpath := constor.getPath(li, id)
		path := Path.Join(dirpath, name)
		if constor.isdeleted(path) {
			return "", syscall.ENOENT
		}
		inobyte, err := Lgetxattr(path, IDXATTR)
		if err == nil {
			if len(inobyte) == 0 {
				return "", syscall.ENOENT
			}
			return string(inobyte), nil
		}
	}
	return "", syscall.ENOENT
}

func (constor *Constor) setid(path string, id string) string {
	if id == "" {
		id = newuuid().String()
	}
	err := Lsetxattr(path, IDXATTR, []byte(id), 0)
	if err == nil {
		return id
	} else {
		return ""
	}
}

func (constor *Constor) createPath(id string) error {
	return nil
	// path := Path.Join(constor.layers[0], id)
	// return os.MkdirAll(path, 0770)
}

func (constor *Constor) copyup(inode *Inode) error {
	if inode.layer == 0 {
		return nil
	}
	src := constor.getPath(inode.layer, inode.id)
	if src == "" {
		return syscall.EIO
	}
	dst := constor.getPath(0, inode.id)
	if dst == "" {
		return syscall.EIO
	}
	fi, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		linkName, err := os.Readlink(src)
		if err != nil {
			return err
		}
		err = os.Symlink(linkName, dst)
		if err != nil {
			return err
		}
	} else if fi.Mode()&os.ModeDir == os.ModeDir {
		err := os.Mkdir(dst, fi.Mode())
		if err != nil {
			return err
		}
	} else {
		in, err := os.Open(src)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(dst)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		if err != nil {
			return err
		}
		err = out.Close()
		if err != nil {
			return err
		}
	}
	stat := syscall.Stat_t{}
	if err = syscall.Lstat(src, &stat); err != nil {
		return err
	}
	if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		if err = syscall.Chmod(dst, stat.Mode); err != nil {
			return err
		}
	}
	if err = syscall.Lchown(dst, int(stat.Uid), int(stat.Gid)); err != nil {
		return err
	}
	links, err := Lgetxattr(src, LINKSXATTR)
	if err == nil && len(links) > 0 {
		err := Lsetxattr(dst, LINKSXATTR, links, 0)
		if err != nil {
			return err
		}
	}

	if fi.Mode()&os.ModeSymlink != os.ModeSymlink {
		if err = syscall.UtimesNano(dst, []syscall.Timespec{stat.Atim, stat.Mtim}); err != nil {
			return err
		}
	}
	inode.layer = 0
	constor.log("file %s", inode.id)
	return nil
}
