package types

import "math"

// 参考：https://github.com/geocompass/pg-coordtransform

func geocIsInChinaBBox(lon, lat float64) bool {
	return lon >= 72.004 && lon <= 137.8347 && lat >= 0.8293 && lat <= 55.8271
}

func geocTransformLat(x, y float64) float64 {
	return -100 + 2*x + 3*y + 0.2*y*y + 0.1*x*y + 0.2*math.Sqrt(math.Abs(x)) +
		(20*math.Sin(6*x*math.Pi)+20*math.Sin(2*x*math.Pi))*2/3 +
		(20*math.Sin(y*math.Pi)+40*math.Sin(y/3*math.Pi))*2/3 +
		(160*math.Sin(y/12*math.Pi)+320*math.Sin(y*math.Pi/30))*2/3
}

func geocTransformLon(x, y float64) float64 {
	return 300 + x + 2*y + 0.1*x*x + 0.1*x*y + 0.1*math.Sqrt(math.Abs(x)) +
		(20*math.Sin(6*x*math.Pi)+20*math.Sin(2*x*math.Pi))*2/3 +
		(20*math.Sin(x*math.Pi)+40*math.Sin(x/3*math.Pi))*2/3 +
		(150*math.Sin(x/12*math.Pi)+300*math.Sin(x/30*math.Pi))*2/3
}

func geocDelta(lon, lat float64) (float64, float64) {
	dLon := geocTransformLon(lon-105, lat-35)
	dLat := geocTransformLat(lon-105, lat-35)
	radLat := lat / 180 * math.Pi
	magic := math.Sin(radLat)
	magic = 1 - 0.006693421622965823*magic*magic
	sqrtMagic := math.Sqrt(magic)
	dLon = (dLon * 180.) / (6378245. / sqrtMagic * math.Cos(radLat) * math.Pi)
	dLat = (dLat * 180.) / ((6378245. * (1. - 0.006693421622965823)) / (magic * sqrtMagic) * math.Pi)
	return dLon, dLat
}

func geocWgs84ToGcj02Point(p Wgs84Point) Gcj02Point {
	if !geocIsInChinaBBox(p.Lon, p.Lat) {
		return Gcj02Point(p)
	}
	dLon, dLat := geocDelta(p.Lon, p.Lat)
	return Gcj02Point{
		Lon: p.Lon + dLon,
		Lat: p.Lat + dLat,
	}
}

func geocGcj02ToWgs84Point(p Gcj02Point) Wgs84Point {
	if !geocIsInChinaBBox(p.Lon, p.Lat) {
		return Wgs84Point(p)
	}
	dLon, dLat := geocDelta(p.Lon, p.Lat)
	return Wgs84Point{
		Lon: p.Lon - dLon,
		Lat: p.Lat - dLat,
	}
}

func geocGcj02ToBd09Point(p Gcj02Point) Bd09Point {
	if !geocIsInChinaBBox(p.Lon, p.Lat) {
		return Bd09Point(p)
	}
	x := p.Lon
	y := p.Lat
	z := math.Sqrt(x*x+y*y) + 2e-5*math.Sin(y*52.359877559829887307710723054658)
	theta := math.Atan2(y, x) + 3e-6*math.Cos(x*52.359877559829887307710723054658)
	return Bd09Point{
		Lon: z*math.Cos(theta) + 6.5e-3,
		Lat: z*math.Sin(theta) + 6e-3,
	}
}

func geocBd09ToGcj02Point(p Bd09Point) Gcj02Point {
	if !geocIsInChinaBBox(p.Lon, p.Lat) {
		return Gcj02Point(p)
	}
	x := p.Lon - 6.5e-3
	y := p.Lat - 6e-3
	z := math.Sqrt(x*x+y*y) - 2e-5*math.Sin(y*52.359877559829887307710723054658)
	theta := math.Atan2(y, x) - 3e-6*math.Cos(x*52.359877559829887307710723054658)
	return Gcj02Point{
		Lon: z * math.Cos(theta),
		Lat: z * math.Sin(theta),
	}
}

func geocWgs84ToBd09Point(p Wgs84Point) Bd09Point {
	return geocGcj02ToBd09Point(geocWgs84ToGcj02Point(p))
}

func geocBd09ToWgs84Point(p Bd09Point) Wgs84Point {
	return geocGcj02ToWgs84Point(geocBd09ToGcj02Point(p))
}
