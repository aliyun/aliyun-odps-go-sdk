package common

type Result struct {
	Data  interface{}
	Error error
}

func (r *Result) IsOk() bool {
	return r.Error == nil
}

func (r *Result) IsErr() bool {
	return r.Error != nil
}
