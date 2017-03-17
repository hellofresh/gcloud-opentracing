package gcloudtracer

import "log"

type defaultLogger struct{}

func (defaultLogger) Errorf(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

// Logger defines an interface to log an error.
type Logger interface {
	Errorf(string, ...interface{})
}
