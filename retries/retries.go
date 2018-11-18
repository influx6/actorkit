package retries

import "time"

// LinearDoUntil will continuously run the giving function until no error is returned.
// If duration is supplied, the goroutine is made to sleep before making next run.
// The same duration is consistently used for each sleep.
// The last error received is returned if the count is exhausted without formally
// ending function call without an error.
func LinearDoUntil(fx func() error, total int, elapse time.Duration) error {
	var err error
	for i := total; i > 0; i-- {
		if err = fx(); err == nil {
			return nil
		}

		if elapse > 0 {
			time.Sleep(elapse)
		}
	}
	return err
}
