package util

import (
	"math"
	"math/rand"
	"sort"
	"time"
)

// Panics if there is an error, otherwise returns the result
func Try[T any](result T, err error) T {
	CheckErr(err)
	return result
}

// Panics if error is not null
func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}

// Returns the current unix time in seconds
func EpochSeconds() float64 {
	return float64(time.Now().UnixNano()) / float64(1e9)
}

// Computes a percentile (0-100) from an array
func Percentile(a []float64, p int) float64 {
	if len(a) <= 1 {
		return math.NaN()
	}

	sort.Slice(a, func(i, j int) bool {
		return a[i] < a[j]
	})

	r := (float64(p)/100)*float64(len(a)) - 1

	if r == float64(int(r)) {
		return a[int(r)]
	} else {
		ri := int(r)
		rf := r - float64(ri)
		return a[ri] + rf*(a[ri+1]-a[ri])
	}
}

// Returns the first argument
func First[T1, T2 any](r1 T1, _ T2) T1 {
	return r1
}

// Returns the second argument
func Second[T1, T2 any](_ T1, r2 T2) T2 {
	return r2
}

// Casts an array to an array with a given type
func CastArray[T1, T2 any](array []T1) []T2 {
	new := []T2{}
	for _, x := range array {
		var y any = x
		new = append(new, y.(T2))
	}
	return new
}

const alphanumerics = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// Returns a random alphanumeric string with 'length' bytes
func RandomString(length int) string {
	var s = make([]byte, length)
	for i := 0; i < length; i++ {
		s[i] = alphanumerics[rand.Intn(len(alphanumerics))]
	}
	return string(s)
}
