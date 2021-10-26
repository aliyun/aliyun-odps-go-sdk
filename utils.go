package odps

import (
	"encoding/xml"
	"time"
)

type GMTTime time.Time

func (t *GMTTime) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error  {
	var s string

	err := d.DecodeElement(&s, &start)
	if err != nil {
		return err
	}

	timeParsed, err := time.ParseInLocation(time.RFC1123, s, GMT)
	if err != nil {
		return err
	}

	*t = GMTTime(timeParsed)

	return nil
}