package retries

import (
	"math"
	"math/rand"
	"time"
)

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

//***************************************************************
// BackOff Generators
//
// Taken from the ff:
// 1. https://github.com/sethgrid/pester
// 2. https://github.com/cenkalti/backoff
// 3. https://github.com/hashicorp/go-retryablehttp
//***************************************************************

var (
	// random is used to generate pseudo-random numbers.
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// LinearBackOff returns increasing durations, each a second longer than the last
func LinearBackOff(i int) time.Duration {
	return time.Duration(i) * time.Second
}

// ExponentialBackOff returns ever increasing backoffs by a power of 2
func ExponentialBackOff(i int) time.Duration {
	return time.Duration(1<<uint(i)) * time.Second
}

// ExponentialJitterBackOff returns ever increasing backoffs by a power of 2
// with +/- 0-33% to prevent synchronized requests.
func ExponentialJitterBackOff(i int) time.Duration {
	return JitterDuration(int(1 << uint(i)))
}

// LinearJitterBackOff returns increasing durations, each a second longer than the last
// with +/- 0-33% to prevent synchronized requests.
func LinearJitterBackOff(i int) time.Duration {
	return JitterDuration(i)
}

// JitterDuration keeps the +/- 0-33% logic in one place
func JitterDuration(i int) time.Duration {
	ms := i * 1000
	maxJitter := ms / 3

	// ms ± rand
	ms += random.Intn(2*maxJitter) - maxJitter

	// a jitter of 0 messes up the time.Tick chan
	if ms <= 0 {
		ms = 1
	}

	return time.Duration(ms) * time.Millisecond
}

// RandomizedJitters returns a function which returns a new duration for randomized
// value of passed integer ranged within provided initial time duration seed.
func RandomizedJitters(initial time.Duration, randomFactor float64) func(int) time.Duration {
	return func(attempt int) time.Duration {
		rr := math.Abs(random.Float64())
		initial = GetRandomValueFromInterval(randomFactor, rr*float64(attempt), initial)
		return initial
	}
}

// LinearRangeJitters returns a function which returns a new duration for increasing
// value of passed integer ranged within provided minimum and maximum time durations.
func LinearRangeJitters(min, max time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		return LinearRangedJitterBackOff(min, max, attempt)
	}
}

// RangedExponential returns a function which returns a new duration for increasing
// value of passed integer ranged within provided minimum and maximum time durations.
func RangedExponential(min, max time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		return RangeExponentialBackOff(min, max, attempt)
	}
}

// RangeExponentialBackOff provides a back off value which will perform
// exponential back off based on the attempt number and limited
// by the provided minimum and maximum durations.
func RangeExponentialBackOff(min, max time.Duration, attemptNum int) time.Duration {
	mult := math.Pow(2, float64(attemptNum)) * float64(min)
	sleep := time.Duration(mult)
	if float64(sleep) != mult || sleep > max {
		sleep = max
	}
	return sleep
}

// LinearRangedJitterBackOff provides a back off value which will
// perform linear back off based on the attempt number and with jitter to
// prevent a thundering herd.
//
// min and max here are *not* absolute values. The number to be multiplied by
// the attempt number will be chosen at random from between them, thus they are
// bounding the jitter.
//
// For instance:
// * To get strictly linear back off of one second increasing each retry, set
// both to one second (1s, 2s, 3s, 4s, ...)
// * To get a small amount of jitter centered around one second increasing each
// retry, set to around one second, such as a min of 800ms and max of 1200ms
// (892ms, 2102ms, 2945ms, 4312ms, ...)
// * To get extreme jitter, set to a very wide spread, such as a min of 100ms
// and a max of 20s (15382ms, 292ms, 51321ms, 35234ms, ...)
func LinearRangedJitterBackOff(min, max time.Duration, attemptNum int) time.Duration {
	// attemptNum always starts at zero but we want to start at 1 for multiplication
	attemptNum++

	if max <= min {
		// Unclear what to do here, or they are the same, so return min *
		// attemptNum
		return min * time.Duration(attemptNum)
	}

	// Pick a random number that lies somewhere between the min and max and
	// multiply by the attemptNum. attemptNum starts at zero so we always
	// increment here. We first get a random percentage, then apply that to the
	// difference between min and max, and add to min.
	jitter := random.Float64() * float64(max-min)
	jitterMin := int64(jitter) + int64(min)
	return time.Duration(jitterMin * int64(attemptNum))
}

// GetRandomValueFromInterval returns a random value from the following interval:
// 	[randomizationFactor * currentInterval, randomizationFactor * currentInterval].
func GetRandomValueFromInterval(randomizationFactor, random float64, currentInterval time.Duration) time.Duration {
	var delta = randomizationFactor * float64(currentInterval)
	var minInterval = float64(currentInterval) - delta
	var maxInterval = float64(currentInterval) + delta

	// Get a random value from the range [minInterval, maxInterval].
	// The formula used below has a +1 because if the minInterval is 1 and the maxInterval is 3 then
	// we want a 33% chance for selecting either 1, 2 or 3.
	return time.Duration(minInterval + (random * (maxInterval - minInterval + 1)))
}
