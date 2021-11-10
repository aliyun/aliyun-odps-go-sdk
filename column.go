package odps

type Column struct {
	Name            string
	Type            DataType
	Comment         string
	Label           string
	IsNullable      bool
	HasDefaultValue bool
	ExtendedLabels  []string
}
