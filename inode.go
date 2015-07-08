package main

import (
	"sync"
	"unsafe"
)

// FIXME: lock is not held whenever layer is modified

type Inode struct {
	nlookup uint64
	id	string
	layer   int
	sync.Mutex
	constor *Constor
}

func (inode *Inode) lookup() {
	inode.Lock()
	defer inode.Unlock()
	inode.nlookup++
}

func (inode *Inode) forget(n uint64) {
	inode.Lock()
	inode.nlookup -= n
	if n == 0 {
		inode.nlookup = 0
	}
	n = inode.nlookup
	inode.Unlock()
	if n == 0 {
		inode.constor.inodemap.unhashInode(inode)
	}
}

func NewInode(constor *Constor, id string) *Inode {
	inode := new(Inode)
	inode.constor = constor
	inode.id = id
	inode.nlookup = 1
	inode.layer = -1
	return inode
}

type Inodemap struct {
	ptrmap    map[uint64]*Inode
	idmap     map[string]*Inode
	constor *Constor
}

func NewInodemap(constor *Constor) *Inodemap {
	inodemap := new(Inodemap)
	inodemap.constor = constor
	inodemap.ptrmap = make(map[uint64]*Inode)
	inodemap.idmap = make(map[string]*Inode)

	inode := NewInode(constor, ROOTID)
	inodemap.hashInode(inode)
	return inodemap
}

func (inodemap *Inodemap) findInodePtr(ptr uint64) (*Inode) {
	inodemap.constor.Lock()
	defer inodemap.constor.Unlock()
	inode, ok := inodemap.ptrmap[ptr]
	if ok {
		return inode
	}
	return nil
}

func (inodemap *Inodemap) findInodeId(id string) (*Inode) {
	inodemap.constor.Lock()
	defer inodemap.constor.Unlock()
	inode, ok := inodemap.idmap[id]
	if ok {
		return inode
	}
	return nil
}

func (inodemap *Inodemap) hashInode(inode *Inode) {
	inodemap.constor.Lock()
	defer inodemap.constor.Unlock()
	if inode.id == ROOTID {
		inodemap.ptrmap[1] = inode
		inodemap.idmap[ROOTID] = inode
		return
	}
	ptr := uint64 (uintptr(unsafe.Pointer(inode)))
	inodemap.ptrmap[ptr] = inode
	inodemap.idmap[inode.id] = inode
}

func (inodemap *Inodemap) unhashInode(inode *Inode) {
	inodemap.constor.Lock()
	defer inodemap.constor.Unlock()
	if inode.id == ROOTID {
		return
	}
	ptr := uint64(uintptr(unsafe.Pointer(inode)))
	delete(inodemap.ptrmap, ptr)
	delete(inodemap.idmap, inode.id)
}
