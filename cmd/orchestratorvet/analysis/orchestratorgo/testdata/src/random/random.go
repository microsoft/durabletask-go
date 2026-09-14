// Package random exercises the nondeterministic identifier and random checks.
package random

import (
	cryptorand "crypto/rand"
	"math/big"
	mathrand "math/rand"
	randv2 "math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/task"
)

func randomIdentifiers(ctx *task.OrchestrationContext) (any, error) {
	_ = uuid.New()                               // want `uuid\.New is not deterministic in an orchestrator`
	_ = uuid.NewString()                         // want `uuid\.NewString is not deterministic in an orchestrator`
	if id, err := uuid.NewRandom(); err == nil { // want `uuid\.NewRandom is not deterministic in an orchestrator`
		_ = id
	}
	if id, err := uuid.NewUUID(); err == nil { // want `uuid\.NewUUID is not deterministic in an orchestrator`
		_ = id
	}
	return nil, nil
}

func cryptoRandom(ctx *task.OrchestrationContext) (any, error) {
	buffer := make([]byte, 16)
	if _, err := cryptorand.Read(buffer); err != nil { // want `crypto/rand\.Read is not deterministic in an orchestrator`
		return nil, err
	}
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(10)) // want `crypto/rand\.Int is not deterministic in an orchestrator`
	if err != nil {
		return nil, err
	}
	return value.String(), nil
}

func globalMathRandom(ctx *task.OrchestrationContext) (any, error) {
	_ = mathrand.Intn(10)  // want `math/rand\.Intn uses the global random source`
	_ = mathrand.Float64() // want `math/rand\.Float64 uses the global random source`
	_ = mathrand.Perm(3)   // want `math/rand\.Perm uses the global random source`
	_ = randv2.IntN(10)    // want `math/rand/v2\.IntN uses the global random source`
	_ = randv2.Float64()   // want `math/rand/v2\.Float64 uses the global random source`
	return nil, nil
}

// seededFromWallClock draws its seed from the host clock, which differs on every
// replay, so the generator is provably nondeterministic.
func seededFromWallClock(ctx *task.OrchestrationContext) (any, error) {
	derived := mathrand.New(mathrand.NewSource(time.Now().UnixNano())) // want `time\.Now is not deterministic in an orchestrator`
	return derived.Intn(10), nil                                       // want `\(\*rand\.Rand\)\.Intn is seeded from a nondeterministic source`
}

// seededThroughLocal hoists the same host-clock seed into a local, which the
// analyzer follows through its single assignment.
func seededThroughLocal(ctx *task.OrchestrationContext) (any, error) {
	seed := time.Now().UnixNano() // want `time\.Now is not deterministic in an orchestrator`
	generator := mathrand.New(mathrand.NewSource(seed))
	return generator.Int63(), nil // want `\(\*rand\.Rand\)\.Int63 is seeded from a nondeterministic source`
}

// seededFromInput replays identically: orchestration input is restored from
// history on every turn, so a generator seeded from it produces the same values.
func seededFromInput(ctx *task.OrchestrationContext) (any, error) {
	var seed int64
	if err := ctx.GetInput(&seed); err != nil {
		return nil, err
	}
	derived := mathrand.New(mathrand.NewSource(seed))
	return derived.Intn(10), nil
}

// deterministicSeed uses a compile-time constant, which replays identically.
func deterministicSeed(ctx *task.OrchestrationContext) (any, error) {
	const seed = 42
	generator := mathrand.New(mathrand.NewSource(seed))
	pinned := randv2.New(randv2.NewPCG(1, 2))
	return generator.Intn(10) + pinned.IntN(10), nil
}

// seedHeldInIdentifier stores a constant-seeded source in its own variable
// before constructing the generator, which is still fully deterministic.
func seedHeldInIdentifier(ctx *task.OrchestrationContext) (any, error) {
	source := mathrand.NewSource(7)
	generator := mathrand.New(source)

	const pinnedSeed = 99
	pinned := mathrand.NewSource(pinnedSeed)
	fromConstant := mathrand.New(pinned)
	return generator.Intn(10) + fromConstant.Intn(10), nil
}

// seedFromParameter cannot be traced to a source either way, so the generator is
// left alone rather than guessed at.
func seedFromParameter(ctx *task.OrchestrationContext) (any, error) {
	return generatorFor(ctx.NewGuid()), nil
}

func generatorFor(token string) int {
	generator := mathrand.New(mathrand.NewSource(int64(len(token))))
	return generator.Intn(10)
}

// deterministicIdentifiers only uses name-based and durable sources.
func deterministicIdentifiers(ctx *task.OrchestrationContext) (any, error) {
	_ = ctx.NewGuid()
	_ = uuid.NewSHA1(uuid.NameSpaceDNS, []byte("stable"))
	_ = uuid.NewMD5(uuid.NameSpaceDNS, []byte("stable"))
	parsed, err := uuid.Parse("00000000-0000-0000-0000-000000000000")
	if err != nil {
		return nil, err
	}
	return parsed.String(), nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(randomIdentifiers)
	_ = registry.AddOrchestrator(cryptoRandom)
	_ = registry.AddOrchestrator(globalMathRandom)
	_ = registry.AddOrchestrator(seededFromWallClock)
	_ = registry.AddOrchestrator(seededThroughLocal)
	_ = registry.AddOrchestrator(seededFromInput)
	_ = registry.AddOrchestrator(deterministicSeed)
	_ = registry.AddOrchestrator(seedHeldInIdentifier)
	_ = registry.AddOrchestrator(seedFromParameter)
	_ = registry.AddOrchestrator(deterministicIdentifiers)
}
