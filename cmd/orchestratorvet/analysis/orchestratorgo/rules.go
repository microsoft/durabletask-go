package orchestratorgo

// wallClockFunctions maps time package entry points that read or wait on the
// host clock to the orchestration API that replaces them.
var wallClockFunctions = map[string]string{
	"Now":       "CurrentTimeUtc",
	"Since":     "CurrentTimeUtc",
	"Until":     "CurrentTimeUtc",
	"After":     "CreateTimer",
	"AfterFunc": "CreateTimer",
	"Sleep":     "CreateTimer",
	"Tick":      "CreateTimer",
	"NewTicker": "CreateTimer",
	"NewTimer":  "CreateTimer",
}

// randomConstructors are math/rand entry points that only build a generator.
// They are deterministic on their own; what matters is the seed they receive.
var randomConstructors = map[string]bool{
	"New":        true,
	"NewChaCha8": true,
	"NewPCG":     true,
	"NewSource":  true,
	"NewZipf":    true,
}

// nondeterministicUUIDFunctions are google/uuid constructors that draw on
// randomness or the host clock. Name-based constructors such as NewSHA1 and
// NewMD5, and parsing helpers, are deterministic and are not listed.
var nondeterministicUUIDFunctions = map[string]bool{
	"New":                 true,
	"NewRandom":           true,
	"NewRandomFromReader": true,
	"NewString":           true,
	"NewUUID":             true,
}

// syncOnceFunctions are sync package functions that return memoized closures.
var syncOnceFunctions = map[string]bool{
	"OnceFunc":   true,
	"OnceValue":  true,
	"OnceValues": true,
}

// externalOSFunctions are os package entry points that touch the filesystem,
// the process environment, or process lifetime.
var externalOSFunctions = map[string]bool{
	"Chdir": true, "Chmod": true, "Chown": true, "Chtimes": true,
	"Clearenv": true, "Create": true, "CreateTemp": true,
	"Environ": true, "Executable": true, "Exit": true, "ExpandEnv": true,
	"FindProcess": true, "Getegid": true, "Getenv": true, "Geteuid": true,
	"Getgid": true, "Getpid": true, "Getppid": true, "Getuid": true,
	"Getwd": true, "Hostname": true, "Lchown": true, "Link": true,
	"LookupEnv": true, "Lstat": true, "Mkdir": true, "MkdirAll": true,
	"MkdirTemp": true, "NewFile": true, "Open": true, "OpenFile": true,
	"OpenRoot": true, "Pipe": true, "ReadDir": true, "ReadFile": true,
	"Readlink": true, "Remove": true, "RemoveAll": true, "Rename": true,
	"Setenv": true, "StartProcess": true, "Stat": true, "Symlink": true,
	"TempDir": true, "Truncate": true, "Unsetenv": true,
	"UserCacheDir": true, "UserConfigDir": true, "UserHomeDir": true,
	"WriteFile": true,
}

// externalHTTPFunctions are net/http package entry points that open sockets.
var externalHTTPFunctions = map[string]bool{
	"Get": true, "Head": true, "ListenAndServe": true, "ListenAndServeTLS": true,
	"Post": true, "PostForm": true, "Serve": true, "ServeFile": true,
	"ServeTLS": true,
}

var externalHTTPClientMethods = map[string]bool{
	"Do": true, "Get": true, "Head": true, "Post": true, "PostForm": true,
}

var externalCmdMethods = map[string]bool{
	"CombinedOutput": true, "Output": true, "Run": true, "Start": true, "Wait": true,
}

// replayUnsafeLogFunctions are log package entry points that emit output.
var replayUnsafeLogFunctions = map[string]bool{
	"Fatal": true, "Fatalf": true, "Fatalln": true,
	"Output": true, "Panic": true, "Panicf": true, "Panicln": true,
	"Print": true, "Printf": true, "Println": true,
	"SetFlags": true, "SetOutput": true, "SetPrefix": true,
}

// replayUnsafeSlogFunctions are package-level log/slog entry points that emit
// output. Methods on *slog.Logger are deliberately absent because
// (*task.OrchestrationContext).Logger returns a replay-safe *slog.Logger.
var replayUnsafeSlogFunctions = map[string]bool{
	"Debug": true, "DebugContext": true,
	"Error": true, "ErrorContext": true, "Info": true, "InfoContext": true,
	"Log": true, "LogAttrs": true,
	"Warn": true, "WarnContext": true,
}

// processLoggerFunctions are log/slog entry points that read or replace the
// process-wide logger rather than emitting a record themselves. They are still
// reported, because the logger they hand back or install writes on every replay,
// but they warrant their own wording.
var processLoggerFunctions = map[string]bool{
	"Default": true, "SetDefault": true,
}

// pureLoopPackages are standard library packages whose functions cannot block,
// perform I/O, or advance orchestration state. Calls into them do not prevent
// the unbounded-loop check from proving that a loop makes no durable progress.
var pureLoopPackages = map[string]bool{
	"bytes": true, "cmp": true, "encoding/json": true, "errors": true,
	"fmt": true, "maps": true, "math": true, "math/bits": true,
	"slices": true, "sort": true, "strconv": true, "strings": true,
	"unicode": true, "unicode/utf8": true,
}
