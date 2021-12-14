package common

import (
	"encoding/json"
	"encoding/xml"
	"github.com/pkg/errors"
	"time"
)

type GMTTime time.Time

func (t GMTTime) String() string {
	return time.Time(t).String()
}

func (t GMTTime) Format(f string) string {
	return time.Time(t).Format(f)
}

func (t *GMTTime) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string

	err := d.DecodeElement(&s, &start)
	if err != nil {
		return errors.WithStack(err)
	}

	if s == "" {
		*t = GMTTime(time.Time{})
		return nil
	}

	timeParsed, err := ParseRFC1123Date(s)
	if err != nil {
		return errors.WithStack(err)
	}

	*t = GMTTime(timeParsed)

	return nil
}

func ParseRFC1123Date(s string) (time.Time, error) {
	t, err := time.ParseInLocation(time.RFC1123, s, GMT)
	return t, errors.WithStack(err)
}

func (t *GMTTime) UnmarshalJSON(b []byte) error {
	var intTime int64
	err := json.Unmarshal(b, &intTime)
	if err != nil {
		return errors.WithStack(err)
	}

	*t = GMTTime(time.Unix(intTime, 0))
	return nil
}

func (t *GMTTime) MarshalJSON() ([]byte, error) {
	timestamp := time.Time(*t).Unix()

	b, err := json.Marshal(timestamp)
	return b, errors.WithStack(err)
}
