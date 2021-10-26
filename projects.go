package odps

type Projects struct {
	Marker string
	MaxItems int
	Projects []Project `xml:"Project"`


}
