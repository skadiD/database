package types

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"github.com/cridenour/go-postgis"
	"strconv"
)

type GeometryPoint struct {
	// 经度
	Lon float64
	// 纬度
	Lat float64
}

// Scan 实现 postgis.Geometry
func (g *GeometryPoint) Scan(src any) error {
	var p postgis.PointS
	err := p.Scan(src)
	if err != nil {
		return err
	}
	g.Lon, g.Lat = p.X, p.Y
	return nil
}

// Value 实现 postgis.Geometry
func (g GeometryPoint) Value() (driver.Value, error) {
	p := postgis.PointS{
		SRID: 4326,
		X:    g.Lon,
		Y:    g.Lat,
	}
	v, err := p.Value()
	if err != nil {
		return nil, err
	}
	return hex.EncodeToString(v.([]byte)), nil
}

// GetType 实现 postgis.Geometry
func (g GeometryPoint) GetType() uint32 {
	// postgis.PointS.GetType()
	return 0x20000001
}

// Value 实现 postgis.Geometry
func (g GeometryPoint) Write(buffer *bytes.Buffer) error {
	p := postgis.PointS{
		SRID: 4326,
		X:    g.Lon,
		Y:    g.Lat,
	}
	return p.Write(buffer)
}

func (g GeometryPoint) MarshalJSON() ([]byte, error) {
	return g.EncodeGeoJson(), nil
}

type GeometryPointJson struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

func (g *GeometryPoint) UnmarshalJSON(b []byte) error {
	var data GeometryPointJson
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	if len(data.Coordinates) != 2 {
		return nil
	}
	g.Lon, g.Lat = data.Coordinates[0], data.Coordinates[1]
	return nil
}
func (g GeometryPoint) EncodeGeoJson() []byte {
	return []byte(`{"type":"Point","coordinates":[` +
		strconv.FormatFloat(g.Lon, 'f', -1, 64) + `,` +
		strconv.FormatFloat(g.Lat, 'f', -1, 64) + `]}`)
}

type GeometryPointConv interface {
	ToWgs84() Wgs84Point
	ToGcj02() Gcj02Point
	ToBd09() Bd09Point
	ToRaw() GeometryPoint
}

type Wgs84Point GeometryPoint

func (p Wgs84Point) ToWgs84() Wgs84Point {
	return p
}
func (p Wgs84Point) ToGcj02() Gcj02Point {
	return geocWgs84ToGcj02Point(p)
}
func (p Wgs84Point) ToBd09() Bd09Point {
	return geocWgs84ToBd09Point(p)
}
func (p Wgs84Point) ToRaw() GeometryPoint {
	return GeometryPoint(p)
}
func (p *Wgs84Point) Scan(src any) error {
	return (*GeometryPoint)(p).Scan(src)
}
func (p Wgs84Point) Value() (driver.Value, error) {
	return (GeometryPoint)(p).Value()
}
func (p Wgs84Point) GetType() uint32 {
	return (GeometryPoint)(p).GetType()
}
func (p Wgs84Point) Write(buffer *bytes.Buffer) error {
	return (GeometryPoint)(p).Write(buffer)
}
func (p Wgs84Point) MarshalJSON() ([]byte, error) {
	return (GeometryPoint)(p).EncodeGeoJson(), nil
}
func (p *Wgs84Point) UnmarshalJSON(b []byte) error {
	return (*GeometryPoint)(p).UnmarshalJSON(b)
}

type Gcj02Point GeometryPoint

func (p Gcj02Point) ToWgs84() Wgs84Point {
	return geocGcj02ToWgs84Point(p)
}
func (p Gcj02Point) ToGcj02() Gcj02Point {
	return p
}
func (p Gcj02Point) ToBd09() Bd09Point {
	return geocGcj02ToBd09Point(p)
}
func (p Gcj02Point) ToRaw() GeometryPoint {
	return GeometryPoint(p)
}
func (p Gcj02Point) MarshalJSON() ([]byte, error) {
	return (GeometryPoint)(p).EncodeGeoJson(), nil
}
func (p *Gcj02Point) UnmarshalJSON(b []byte) error {
	return (*GeometryPoint)(p).UnmarshalJSON(b)
}

type Bd09Point GeometryPoint

func (p Bd09Point) ToWgs84() Wgs84Point {
	return geocBd09ToWgs84Point(p)
}
func (p Bd09Point) ToGcj02() Gcj02Point {
	return geocBd09ToGcj02Point(p)
}
func (p Bd09Point) ToBd09() Bd09Point {
	return p
}
func (p Bd09Point) ToRaw() GeometryPoint {
	return GeometryPoint(p)
}
func (p Bd09Point) MarshalJSON() ([]byte, error) {
	return (GeometryPoint)(p).EncodeGeoJson(), nil
}
func (p *Bd09Point) UnmarshalJSON(b []byte) error {
	return (*GeometryPoint)(p).UnmarshalJSON(b)
}
