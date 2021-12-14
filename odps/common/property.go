package common

type Property struct {
	Name  string
	Value string
}

// Properties just alias to []Property
type Properties []Property

func (ps Properties) Get(key string) string {
	for _, p := range []Property(ps) {
		if p.Name == key {
			return p.Value
		}
	}

	return ""
}
