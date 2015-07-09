package main

import (
		"unsafe"
)

type FD struct {
	fd     int
	flags  int
	layer  int
	pid    uint32
	id     string
	stream []DirEntry
}

func (constor *Constor) putfd(F *FD) {
	constor.Lock()
	defer constor.Unlock()
	ptr := uintptr(unsafe.Pointer(F))
	constor.fdmap[ptr] = F
}

func (constor *Constor) getfd(ptr uintptr) *FD {
	constor.Lock()
	defer constor.Unlock()
	F := constor.fdmap[ptr]
	return F
}

func (constor *Constor) deletefd(ptr uintptr) {
	constor.Lock()
	defer constor.Unlock()
	delete(constor.fdmap, ptr)
}

// FIXME: need to optimize this
func (constor *Constor) fdlookup(id string, pid uint32) *FD {
	constor.Lock()
	defer constor.Unlock()

	for _, F := range constor.fdmap {
		if id == F.id && pid == F.pid {
			return F
		}
	}
	return nil
}
