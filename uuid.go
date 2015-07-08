package main

import (
    "encoding/hex"
    gouuid "github.com/satori/go.uuid"
)

type uuid gouuid.UUID

func (u uuid) String() string {
    ubuf := gouuid.UUID(u).Bytes()
    buf := make([]byte, 32)
    hex.Encode(buf, ubuf)
    return string(buf)
}

func (u uuid) ino() uint64 {
    ubuf := gouuid.UUID(u).Bytes()
    inobyte := ubuf[8:]
    var ino uint64
    for i := 0; i < 8; i++ {
        tmp := uint64(inobyte[i])
        ino = ino | (tmp << uint64((7-i)*8))
    }
    return ino
}

func (u *uuid) fromstring(s string) {
    b := []byte(s)
    // c := make([]byte, 16)
    hex.Decode(u[:], b)
}

func idtoino(id string) uint64 {
    if id == ROOTID {
        return 1
    }
    b := []byte(id)
    ubuf := [16]byte{}
    hex.Decode(ubuf[:], b)
    inobyte := ubuf[8:]
    var ino uint64
    for i := 0; i < 8; i++ {
        tmp := uint64(inobyte[i])
        ino = ino | (tmp << uint64((7-i)*8))
    }
    return ino
}

func newuuid() uuid {
    return uuid(gouuid.NewV4())
}
