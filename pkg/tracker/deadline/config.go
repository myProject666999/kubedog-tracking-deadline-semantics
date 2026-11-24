package deadline

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	// DefaultProgressTimeout is the historical no-activity timeout. Under the
	// new semantics it measures absence of substantive progress unless
	// LegacyEventActivityTimeout is enabled.
	DefaultProgressTimeout = 4 * time.Minute
	// DefaultPresenceTimeout remains the old presence polling budget.
	DefaultPresenceTimeout = 5 * time.Minute
	// DefaultCheckInterval is the coarse progress-deadline polling interval.
	// Event processing also checks deadlines immediately, so this does not
	// need to be millisecond-accurate.
	DefaultCheckInterval = 30 * time.Second
)

// ConfigError reports an invalid deadline configuration.
type ConfigError struct {
	Field string
	Value time.Duration
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("invalid deadline configuration: %s=%s: duration must be zero (unlimited) or positive", e.Field, e.Value)
}

func (e *ConfigError) Is(target error) bool {
	other, ok := target.(*ConfigError)
	return ok && (other.Field == "" || other.Field == e.Field)
}

// Config contains deadline settings.
//
// Zero values mean "unlimited" for GlobalTimeout and PerResourceTimeout.
// ProgressTimeout is defaulted to DefaultProgressTimeout by Normalize; set
// DisableProgressTimeout to express an explicit unlimited progress deadline.
// Negative durations are configuration errors.
type Config struct {
	// GlobalTimeout is the total budget shared by a batch. When Shared is set
	// it takes precedence and preserves one start time across resources.
	GlobalTimeout time.Duration
	// Global is an alias for GlobalTimeout.
	Global time.Duration

	// Shared is the common global deadline for a batch of resources.
	Shared *SharedDeadline
	// GlobalDeadline is an alias for Shared.
	GlobalDeadline *SharedDeadline

	// PerResourceTimeout is the default lifetime budget for one resource.
	PerResourceTimeout time.Duration
	// PerResource is an alias for PerResourceTimeout.
	PerResource time.Duration
	// DefaultPerResourceTimeout and DefaultTimeout are descriptive aliases.
	DefaultPerResourceTimeout time.Duration
	DefaultTimeout            time.Duration
	// Timeout and ResourceTimeout match the historical option vocabulary.
	Timeout         time.Duration
	ResourceTimeout time.Duration

	// ResourceTimeouts contains per-resource lifetime overrides.
	ResourceTimeouts map[string]time.Duration
	// ResourceTimeoutOverrides and ResourceOverrides are aliases.
	ResourceTimeoutOverrides map[string]time.Duration
	ResourceOverrides        map[string]time.Duration
	// Overrides is a short alias for ResourceTimeouts.
	Overrides map[string]time.Duration

	// ProgressTimeout bounds silence since substantive progress.
	ProgressTimeout time.Duration
	// Progress is an alias for ProgressTimeout.
	Progress time.Duration
	// ProgressDeadline, NoActivityTimeout and ActivityTimeout are vocabulary aliases.
	ProgressDeadline  time.Duration
	NoActivityTimeout time.Duration
	ActivityTimeout   time.Duration
	// DisableProgressTimeout explicitly disables the progress deadline.
	DisableProgressTimeout bool

	// LegacyEventActivityTimeout restores the old behavior where any event or
	// channel activity reset the no-activity timer. It is provided for one
	// compatibility window; the default is substantive-progress semantics.
	LegacyEventActivityTimeout bool
	// ResetOnAnyEvent is an alias for LegacyEventActivityTimeout.
	ResetOnAnyEvent bool
	// ActivityTimeoutMode can be set to ActivityTimeoutOnAnyEvent.
	ActivityTimeoutMode ActivityTimeoutMode

	// CheckInterval is the coarse timer interval. Zero uses
	// DefaultCheckInterval; a negative value is invalid.
	CheckInterval time.Duration
	// PollInterval is an alias for CheckInterval.
	PollInterval time.Duration

	// Clock supplies time. It may implement only PassiveClock for pure use;
	// runners require Clock. Defaults to the real system clock.
	Clock PassiveClock

	// Logger, when non-nil, receives reset and clamping records.
	Logger Logger
	// Logf is a printf-style logging hook; it is used when Logger is nil.
	Logf func(format string, args ...any)
	// Warnf receives configuration merge warnings. When nil, warnings are
	// also accumulated in ResolvedConfig.Warnings.
	Warnf func(format string, args ...any)
}

// ActivityTimeoutMode selects progress-reset behavior.
type ActivityTimeoutMode string

const (
	// ActivityTimeoutOnProgress is the new substantive-progress semantics.
	ActivityTimeoutOnProgress ActivityTimeoutMode = "progress"
	// ActivityTimeoutOnAnyEvent preserves the old event-interval semantics.
	ActivityTimeoutOnAnyEvent ActivityTimeoutMode = "any_event"
)

// Logger is the minimal structured logging interface used by this package.
type Logger interface {
	Printf(format string, args ...any)
}

// ResolvedConfig is the result of defaulting, validating, clamping overrides,
// and merging aliases.
type ResolvedConfig struct {
	GlobalTimeout      time.Duration
	PerResourceTimeout time.Duration
	ProgressTimeout    time.Duration
	CheckInterval      time.Duration

	LegacyEventActivityTimeout bool
	Clock                      PassiveClock
	Logger                     Logger
	Logf                       func(string, ...any)

	Warnings []string
}

// DefaultConfig returns the canonical defaults: a four-minute progress
// deadline, no global or per-resource deadline, and the real clock. Callers
// that construct Config literally and leave ProgressTimeout zero explicitly
// request an unlimited progress deadline.
func DefaultConfig() Config {
	return Config{
		ProgressTimeout: DefaultProgressTimeout,
		CheckInterval:   DefaultCheckInterval,
		Clock:           SystemClock,
	}
}

// Default is an alias for DefaultConfig.
func Default() Config { return DefaultConfig() }

// Normalize merges aliases and applies built-in non-duration defaults. It does
// not replace a zero progress timeout with DefaultProgressTimeout: zero means
// unlimited. Use DefaultConfig when the four-minute default is intended.
func (c Config) Normalize() (ResolvedConfig, error) {
	global := firstPositive(c.GlobalTimeout, c.Global)
	perResource := firstPositive(c.PerResourceTimeout, c.PerResource, c.DefaultPerResourceTimeout, c.DefaultTimeout, c.ResourceTimeout, c.Timeout)
	progress := firstPositive(c.ProgressTimeout, c.Progress, c.ProgressDeadline, c.NoActivityTimeout, c.ActivityTimeout)
	checkInterval := firstPositive(c.CheckInterval, c.PollInterval)
	if checkInterval == 0 {
		checkInterval = DefaultCheckInterval
	}

	for field, value := range map[string]time.Duration{
		"GlobalTimeout":             c.GlobalTimeout,
		"Global":                    c.Global,
		"PerResourceTimeout":        c.PerResourceTimeout,
		"PerResource":               c.PerResource,
		"DefaultPerResourceTimeout": c.DefaultPerResourceTimeout,
		"DefaultTimeout":            c.DefaultTimeout,
		"Timeout":                   c.Timeout,
		"ResourceTimeout":           c.ResourceTimeout,
		"ProgressDeadline":          c.ProgressDeadline,
		"NoActivityTimeout":         c.NoActivityTimeout,
		"ActivityTimeout":           c.ActivityTimeout,
		"ProgressTimeout":           c.ProgressTimeout,
		"Progress":                  c.Progress,
		"CheckInterval":             c.CheckInterval,
		"PollInterval":              c.PollInterval,
	} {
		if value < 0 {
			return ResolvedConfig{}, &ConfigError{Field: field, Value: value}
		}
	}
	for id, value := range c.allOverrides() {
		if value < 0 {
			return ResolvedConfig{}, &ConfigError{Field: "ResourceTimeouts[" + id + "]", Value: value}
		}
	}

	if c.DisableProgressTimeout {
		progress = 0
	}

	legacy := c.LegacyEventActivityTimeout || c.ResetOnAnyEvent || c.ActivityTimeoutMode == ActivityTimeoutOnAnyEvent

	return ResolvedConfig{
		GlobalTimeout:              global,
		PerResourceTimeout:         perResource,
		ProgressTimeout:            progress,
		CheckInterval:              checkInterval,
		LegacyEventActivityTimeout: legacy,
		Clock:                      passiveClockOrSystem(c.Clock),
		Logger:                     c.Logger,
		Logf:                       c.Logf,
	}, nil
}

// Resolve validates the configuration and resolves the effective per-resource
// timeout for resourceID. An override longer than the global deadline is
// clamped to the global deadline and produces a warning.
func (c Config) Resolve(resourceID string) (ResolvedResourceConfig, error) {
	base, err := c.Normalize()
	if err != nil {
		return ResolvedResourceConfig{}, err
	}

	effective := base.PerResourceTimeout
	override, hasOverride := c.overrideFor(resourceID)
	if hasOverride {
		effective = override
	}

	resolved := ResolvedResourceConfig{
		ResolvedConfig:     base,
		ResourceID:         resourceID,
		PerResourceTimeout: effective,
		Override:           override,
		HasOverride:        hasOverride,
	}

	if c.Shared != nil || c.GlobalDeadline != nil {
		shared := c.Shared
		if shared == nil {
			shared = c.GlobalDeadline
		}
		resolved.Shared = shared
		if shared.timeout > 0 {
			resolved.GlobalTimeout = shared.timeout
		}
	}

	globalTimeout := resolved.GlobalTimeout
	if globalTimeout > 0 && effective > globalTimeout {
		warning := fmt.Sprintf(
			"per-resource timeout %s for %q is greater than global deadline %s; using global deadline",
			effective, resourceID, globalTimeout,
		)
		resolved.Warnings = append(resolved.Warnings, warning)
		resolved.PerResourceTimeout = globalTimeout
		resolved.ClampedToGlobal = true
		if c.Warnf != nil {
			c.Warnf("%s", warning)
		}
	}

	return resolved, nil
}

// ResolvedResourceConfig contains the settings for one resource.
type ResolvedResourceConfig struct {
	ResolvedConfig

	ResourceID string
	// PerResourceTimeout is the effective value after override/global clamp.
	PerResourceTimeout time.Duration
	Override           time.Duration
	HasOverride        bool
	ClampedToGlobal    bool
	Shared             *SharedDeadline
}

func (c Config) allOverrides() map[string]time.Duration {
	merged := make(map[string]time.Duration)
	for _, m := range []map[string]time.Duration{c.ResourceTimeouts, c.ResourceTimeoutOverrides, c.ResourceOverrides, c.Overrides} {
		for k, v := range m {
			merged[k] = v
		}
	}
	return merged
}

func (c Config) overrideFor(resourceID string) (time.Duration, bool) {
	overrides := c.allOverrides()
	if v, ok := overrides[resourceID]; ok {
		return v, true
	}

	// Integrations use several stable identifier forms ("name",
	// "Kind/name", and "namespace/Kind/name"). Accept the shorter forms when
	// they unambiguously match an override.
	name := resourceID
	if idx := strings.LastIndex(resourceID, "/"); idx >= 0 {
		name = resourceID[idx+1:]
	}

	var match time.Duration
	found := false
	for key, value := range overrides {
		if key == name || strings.HasSuffix(resourceID, "/"+key) {
			if found && match != value {
				// Ambiguous overrides must not silently choose one. Callers
				// should use the full resource ID.
				return 0, false
			}
			match = value
			found = true
		}
	}
	return match, found
}

func firstPositive(values ...time.Duration) time.Duration {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

// SortedOverrideIDs returns override IDs in stable order; useful for reports
// and deterministic tests.
func SortedOverrideIDs(m map[string]time.Duration) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Validate is a convenience wrapper around Normalize.
func (c Config) Validate() error {
	_, err := c.Normalize()
	return err
}

// NormalizeConfig is the package-function form of Config.Normalize.
func NormalizeConfig(cfg Config) (ResolvedConfig, error) {
	return cfg.Normalize()
}

// ResolveConfig is the package-function form of Config.Resolve.
func ResolveConfig(cfg Config, resourceID string) (ResolvedResourceConfig, error) {
	return cfg.Resolve(resourceID)
}

// IsConfigError reports whether err is a *ConfigError.
func IsConfigError(err error) bool {
	var cfgErr *ConfigError
	return errors.As(err, &cfgErr)
}
