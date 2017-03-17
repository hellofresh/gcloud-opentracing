package gcloudtracer

// Options containes options for recorder and StackDriver client.
type Options struct {
	log         Logger
	projectID   string
	credentials JWTCredentials
}

// Valid validates Options.
func (o *Options) Valid() error {
	if o.projectID == "" {
		return ErrInvalidProjectID
	}
	return nil
}

// Option defines an recorder option.
type Option func(o *Options)

// WithProject returns a Option that specifies a project identifier.
func WithProject(pid string) Option {
	return func(o *Options) {
		o.projectID = pid
	}
}

// WithLogger returns an Option that specifies a logger of the Recorder.
func WithLogger(logger Logger) Option {
	return func(o *Options) {
		o.log = logger
	}
}

// JWTCredentials represents the json file from the google Appplication Default Configuration
type JWTCredentials struct {
	Email        string
	PrivateKey   []byte
	PrivateKeyID string
}

// WithJWTCredentials retuns an option that the JWT Credentials.
func WithJWTCredentials(credentials JWTCredentials) Option {
	return func(o *Options) {
		o.credentials = credentials
	}
}
