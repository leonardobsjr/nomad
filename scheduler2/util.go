package scheduler2

import (
	"fmt"

	"github.com/hashicorp/nomad/nomad/structs"
	v1 "github.com/hashicorp/nomad/scheduler"
)

// retryMax is used to retry a callback until it returns success or
// a maximum number of attempts is reached. An optional reset function may be
// passed which is called after each failed iteration. If the reset function is
// set and returns true, the number of attempts is reset back to max.
func retryMax(max int, cb func() (bool, error), reset func() bool) error {
	attempts := 0
	for attempts < max {
		done, err := cb()
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		// Check if we should reset the number attempts
		if reset != nil && reset() {
			attempts = 0
		} else {
			attempts++
		}
	}
	return &v1.SetStatusError{
		Err:        fmt.Errorf("maximum attempts reached (%d)", max),
		EvalStatus: structs.EvalStatusFailed,
	}
}
