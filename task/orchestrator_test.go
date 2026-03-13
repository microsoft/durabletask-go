package task

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
)

func Test_computeNextDelay(t *testing.T) {
	time1 := time.Now()
	time2 := time.Now().Add(1 * time.Minute)
	type args struct {
		currentTimeUtc time.Time
		policy         RetryPolicy
		attempt        int
		firstAttempt   time.Time
		err            error
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			name: "first attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      0,
				firstAttempt: time1,
			},
			want: 2 * time.Second,
		},
		{
			name: "second attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      1,
				firstAttempt: time1,
			},
			want: 4 * time.Second,
		},
		{
			name: "third attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      2,
				firstAttempt: time1,
			},
			want: 8 * time.Second,
		},
		{
			name: "fourth attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      3,
				firstAttempt: time1,
			},
			want: 10 * time.Second,
		},
		{
			name: "expired",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         30 * time.Second,
				},
				attempt:      3,
				firstAttempt: time1,
			},
			want: 0,
		},
		{
			name: "fourth attempt backoff 1",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   1,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      3,
				firstAttempt: time1,
			},
			want: 2 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := computeNextDelay(tt.args.currentTimeUtc, tt.args.policy, tt.args.attempt, tt.args.firstAttempt, tt.args.err); got != tt.want {
				t.Errorf("computeNextDelay() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_NewGuid_Deterministic(t *testing.T) {
	// Two contexts with the same ID and time should produce the same GUIDs
	makeCtx := func() *OrchestrationContext {
		return &OrchestrationContext{
			ID:             api.InstanceID("test-instance-123"),
			CurrentTimeUtc: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
		}
	}

	ctx1 := makeCtx()
	ctx2 := makeCtx()

	guid1a := ctx1.NewGuid()
	guid1b := ctx1.NewGuid()
	guid2a := ctx2.NewGuid()
	guid2b := ctx2.NewGuid()

	// Same sequence from same inputs should produce same GUIDs
	if guid1a != guid2a {
		t.Errorf("first GUID should be deterministic: %s != %s", guid1a, guid2a)
	}
	if guid1b != guid2b {
		t.Errorf("second GUID should be deterministic: %s != %s", guid1b, guid2b)
	}
}

func Test_NewGuid_Unique(t *testing.T) {
	ctx := &OrchestrationContext{
		ID:             api.InstanceID("test-instance"),
		CurrentTimeUtc: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		guid := ctx.NewGuid().String()
		if seen[guid] {
			t.Fatalf("duplicate GUID at iteration %d: %s", i, guid)
		}
		seen[guid] = true
	}
}

func Test_NewGuid_DifferentInstances(t *testing.T) {
	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	ctx1 := &OrchestrationContext{
		ID:             api.InstanceID("instance-A"),
		CurrentTimeUtc: ts,
	}
	ctx2 := &OrchestrationContext{
		ID:             api.InstanceID("instance-B"),
		CurrentTimeUtc: ts,
	}

	guid1 := ctx1.NewGuid()
	guid2 := ctx2.NewGuid()
	if guid1 == guid2 {
		t.Errorf("GUIDs from different instances should differ: %s == %s", guid1, guid2)
	}
}

func Test_NewGuid_DifferentTimes(t *testing.T) {
	ctx1 := &OrchestrationContext{
		ID:             api.InstanceID("same-instance"),
		CurrentTimeUtc: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
	}
	ctx2 := &OrchestrationContext{
		ID:             api.InstanceID("same-instance"),
		CurrentTimeUtc: time.Date(2025, 1, 15, 10, 31, 0, 0, time.UTC),
	}

	guid1 := ctx1.NewGuid()
	guid2 := ctx2.NewGuid()
	if guid1 == guid2 {
		t.Errorf("GUIDs from different timestamps should differ: %s == %s", guid1, guid2)
	}
}

func Test_NewGuid_NanosecondTruncation(t *testing.T) {
	// Two timestamps differing only in the last 2 nanosecond digits (sub-100ns)
	// should produce the SAME GUID, since .NET truncates to 100ns (tick) precision.
	ctx1 := &OrchestrationContext{
		ID:             api.InstanceID("truncation-test"),
		CurrentTimeUtc: time.Date(2025, 1, 15, 10, 30, 45, 123456700, time.UTC),
	}
	ctx2 := &OrchestrationContext{
		ID:             api.InstanceID("truncation-test"),
		CurrentTimeUtc: time.Date(2025, 1, 15, 10, 30, 45, 123456789, time.UTC),
	}

	guid1 := ctx1.NewGuid()
	guid2 := ctx2.NewGuid()
	if guid1 != guid2 {
		t.Errorf("sub-100ns differences should be truncated: %s != %s", guid1, guid2)
	}
}

func Test_NewGuid_TimestampFormat(t *testing.T) {
	// Verify the internal timestamp format matches .NET's DateTime.ToString("o")
	// .NET always produces exactly 7 fractional digits for UTC DateTimes
	ts := time.Date(2025, 1, 15, 10, 30, 45, 123456700, time.UTC)
	truncated := ts.Truncate(100 * time.Nanosecond)
	formatted := truncated.Format(dotnetDateTimeFormat)
	expected := "2025-01-15T10:30:45.1234567Z"
	if formatted != expected {
		t.Errorf("timestamp format mismatch: got %q, want %q", formatted, expected)
	}

	// Zero nanoseconds should still produce 7 fractional digits
	tsZero := time.Date(2025, 1, 15, 10, 30, 45, 0, time.UTC)
	formattedZero := tsZero.Format(dotnetDateTimeFormat)
	expectedZero := "2025-01-15T10:30:45.0000000Z"
	if formattedZero != expectedZero {
		t.Errorf("zero nanos format mismatch: got %q, want %q", formattedZero, expectedZero)
	}
}

func Test_NewGuid_GoldenValues(t *testing.T) {
	// Hardcoded expected UUIDs for known inputs. If the algorithm changes,
	// this test will fail — which is intentional, because changing the output
	// would break replay determinism for in-flight orchestrations.
	ctx := &OrchestrationContext{
		ID:             api.InstanceID("test-instance-123"),
		CurrentTimeUtc: time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	guid0 := ctx.NewGuid()
	guid1 := ctx.NewGuid()
	guid2 := ctx.NewGuid()

	if got := guid0.String(); got != "9521ac35-a19d-52bb-84e8-e0977ae0dfe0" {
		t.Errorf("guid[0] golden mismatch: got %s", got)
	}
	if got := guid1.String(); got != "0f10c9ff-99eb-5ec4-bd1d-8b29b5fcefbe" {
		t.Errorf("guid[1] golden mismatch: got %s", got)
	}
	if got := guid2.String(); got != "9668a7c0-9fdd-5030-ac13-f9444f086ab9" {
		t.Errorf("guid[2] golden mismatch: got %s", got)
	}
}

func Test_NewGuid_UUIDv5Properties(t *testing.T) {
	ctx := &OrchestrationContext{
		ID:             api.InstanceID("version-check"),
		CurrentTimeUtc: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
	}

	for i := 0; i < 10; i++ {
		guid := ctx.NewGuid()
		if guid.Version() != 5 {
			t.Errorf("call %d: expected UUID version 5, got %d", i, guid.Version())
		}
		if guid.Variant() != uuid.RFC4122 {
			t.Errorf("call %d: expected RFC4122 variant, got %s", i, guid.Variant())
		}
	}
}

func Test_NewGuid_CounterResetOnNewContext(t *testing.T) {
	// Simulates what happens during replay: a new context is created,
	// counter starts at 0, producing the same GUID sequence.
	makeAndGenerate := func() [3]uuid.UUID {
		ctx := &OrchestrationContext{
			ID:             api.InstanceID("replay-test"),
			CurrentTimeUtc: time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC),
		}
		return [3]uuid.UUID{ctx.NewGuid(), ctx.NewGuid(), ctx.NewGuid()}
	}

	run1 := makeAndGenerate()
	run2 := makeAndGenerate()

	for i := range run1 {
		if run1[i] != run2[i] {
			t.Errorf("guid[%d] differs across fresh contexts: %s != %s", i, run1[i], run2[i])
		}
	}
}
