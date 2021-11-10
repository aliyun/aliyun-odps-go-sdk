package odps

import (
	"encoding/json"
	"encoding/xml"
	"time"
)

type GMTTime time.Time

func (t GMTTime) String() string  {
	return time.Time(t).String()
}

func (t *GMTTime) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error  {
	var s string

	err := d.DecodeElement(&s, &start)
	if err != nil {
		return err
	}

	if s == "" {
		*t = GMTTime(time.Time{})
		return nil
	}

	timeParsed, err := ParseRFC1123Date(s)
	if err != nil {
		return err
	}

	*t = GMTTime(timeParsed)

	return nil
}

func ParseRFC1123Date(s string) (time.Time, error)  {
	return time.ParseInLocation(time.RFC1123, s, GMT)
}

func (t *GMTTime) UnmarshalJSON(b []byte) error  {
	var intTime int64
	err := json.Unmarshal(b, &intTime)
	if err != nil {
		return err
	}

	*t = GMTTime(time.Unix(intTime, 0))
	return nil
}

func (t *GMTTime) MarshalJSON() ([]byte, error) {
	timestamp := time.Time(*t).Unix()

	return json.Marshal(timestamp)
}

type Property struct {
	Name  string
	Value string
}

// Properties just alias to []Property
type Properties []Property

func (ps Properties) Get(key string) string  {
	for _, p := range []Property(ps) {
		if p.Name == key {
			return p.Value
		}
	}

	return ""
}

