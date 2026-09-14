// Package uuid is a minimal stand-in for github.com/google/uuid. It exposes just
// enough surface for the orchestratorgo analysis test fixtures to type-check.
package uuid

import "io"

type UUID [16]byte

func (u UUID) String() string { return "" }

func New() UUID { return UUID{} }

func NewString() string { return "" }

func NewRandom() (UUID, error) { return UUID{}, nil }

func NewRandomFromReader(r io.Reader) (UUID, error) { return UUID{}, nil }

func NewUUID() (UUID, error) { return UUID{}, nil }

func NewSHA1(space UUID, data []byte) UUID { return UUID{} }

func NewMD5(space UUID, data []byte) UUID { return UUID{} }

func Parse(s string) (UUID, error) { return UUID{}, nil }

func MustParse(s string) UUID { return UUID{} }

func Must(u UUID, err error) UUID { return u }

var NameSpaceDNS = UUID{}
