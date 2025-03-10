package task

type createTimerOption func(*createTimerOptions) error

type createTimerOptions struct {
	name string
}

func WithTimerName(name string) createTimerOption {
	return func(opt *createTimerOptions) error {
		opt.name = name
		return nil
	}
}
