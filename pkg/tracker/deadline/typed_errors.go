package deadline

// Typed wrapper aliases make the deadline category available through
// errors.As to a category-specific struct in addition to switching on
// Error.Kind. The wrappers embed the structured *Error and preserve errors.Is
// and Unwrap behavior.

type GlobalDeadlineError struct{ Inner *Error }
type GlobalTimeoutError struct{ Inner *Error }

type PerResourceDeadlineError struct{ Inner *Error }
type PerResourceTimeoutError struct{ Inner *Error }
type ResourceDeadlineError struct{ Inner *Error }
type ResourceTimeoutError struct{ Inner *Error }

type ProgressDeadlineError struct{ Inner *Error }
type ProgressTimeoutError struct{ Inner *Error }

// As supports errors.As targets such as:
//
//	var globalErr *deadline.GlobalDeadlineError
//	errors.As(err, &globalErr)
//
// and value targets such as:
//
//	var globalErr deadline.GlobalTimeoutError
//	errors.As(err, &globalErr)
func (e *Error) As(target any) bool {
	if e == nil {
		return false
	}

	switch typed := target.(type) {
	case **GlobalDeadlineError:
		if e.Kind == KindGlobal {
			*typed = &GlobalDeadlineError{Inner: e}
			return true
		}
	case *GlobalDeadlineError:
		if e.Kind == KindGlobal {
			*typed = GlobalDeadlineError{Inner: e}
			return true
		}
	case **GlobalTimeoutError:
		if e.Kind == KindGlobal {
			*typed = &GlobalTimeoutError{Inner: e}
			return true
		}
	case *GlobalTimeoutError:
		if e.Kind == KindGlobal {
			*typed = GlobalTimeoutError{Inner: e}
			return true
		}

	case **PerResourceDeadlineError:
		if e.Kind == KindPerResource {
			*typed = &PerResourceDeadlineError{Inner: e}
			return true
		}
	case *PerResourceDeadlineError:
		if e.Kind == KindPerResource {
			*typed = PerResourceDeadlineError{Inner: e}
			return true
		}
	case **PerResourceTimeoutError:
		if e.Kind == KindPerResource {
			*typed = &PerResourceTimeoutError{Inner: e}
			return true
		}
	case *PerResourceTimeoutError:
		if e.Kind == KindPerResource {
			*typed = PerResourceTimeoutError{Inner: e}
			return true
		}
	case **ResourceDeadlineError:
		if e.Kind == KindPerResource {
			*typed = &ResourceDeadlineError{Inner: e}
			return true
		}
	case *ResourceDeadlineError:
		if e.Kind == KindPerResource {
			*typed = ResourceDeadlineError{Inner: e}
			return true
		}
	case **ResourceTimeoutError:
		if e.Kind == KindPerResource {
			*typed = &ResourceTimeoutError{Inner: e}
			return true
		}
	case *ResourceTimeoutError:
		if e.Kind == KindPerResource {
			*typed = ResourceTimeoutError{Inner: e}
			return true
		}

	case **ProgressDeadlineError:
		if e.Kind == KindProgress {
			*typed = &ProgressDeadlineError{Inner: e}
			return true
		}
	case *ProgressDeadlineError:
		if e.Kind == KindProgress {
			*typed = ProgressDeadlineError{Inner: e}
			return true
		}
	case **ProgressTimeoutError:
		if e.Kind == KindProgress {
			*typed = &ProgressTimeoutError{Inner: e}
			return true
		}
	case *ProgressTimeoutError:
		if e.Kind == KindProgress {
			*typed = ProgressTimeoutError{Inner: e}
			return true
		}
	}

	return false
}

var (
	_ error = (*GlobalDeadlineError)(nil)
	_ error = (*GlobalTimeoutError)(nil)
	_ error = (*PerResourceDeadlineError)(nil)
	_ error = (*PerResourceTimeoutError)(nil)
	_ error = (*ResourceDeadlineError)(nil)
	_ error = (*ResourceTimeoutError)(nil)
	_ error = (*ProgressDeadlineError)(nil)
	_ error = (*ProgressTimeoutError)(nil)
)

func (e *GlobalDeadlineError) Error() string { return e.Inner.Error() }
func (e *GlobalDeadlineError) Unwrap() error { return e.Inner }

func (e *GlobalTimeoutError) Error() string { return e.Inner.Error() }
func (e *GlobalTimeoutError) Unwrap() error { return e.Inner }

func (e *PerResourceDeadlineError) Error() string { return e.Inner.Error() }
func (e *PerResourceDeadlineError) Unwrap() error { return e.Inner }

func (e *PerResourceTimeoutError) Error() string { return e.Inner.Error() }
func (e *PerResourceTimeoutError) Unwrap() error { return e.Inner }

func (e *ResourceDeadlineError) Error() string { return e.Inner.Error() }
func (e *ResourceDeadlineError) Unwrap() error { return e.Inner }

func (e *ResourceTimeoutError) Error() string { return e.Inner.Error() }
func (e *ResourceTimeoutError) Unwrap() error { return e.Inner }

func (e *ProgressDeadlineError) Error() string { return e.Inner.Error() }
func (e *ProgressDeadlineError) Unwrap() error { return e.Inner }

func (e *ProgressTimeoutError) Error() string { return e.Inner.Error() }
func (e *ProgressTimeoutError) Unwrap() error { return e.Inner }
