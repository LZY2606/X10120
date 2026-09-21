package store

import (
	"fmt"
	"strconv"
)

// NotFoundError indicates a referenced entity does not exist.
type NotFoundError struct {
	What string
	ID   string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s %q not found", e.What, e.ID)
}

func unknownRecord(t string) error {
	return fmt.Errorf("unknown audit record type %q", t)
}

func parseSmallInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

func formatSmallInt(n int) string {
	return strconv.Itoa(n)
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
