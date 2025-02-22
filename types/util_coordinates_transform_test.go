package types

import "testing"

func TestGcj02ToWgs84(t *testing.T) {
	gcj02Point := Gcj02Point{
		Lon: 104.074702,
		Lat: 30.686747,
	}
	expectedPoint := Wgs84Point{
		Lon: 104.072178447,
		Lat: 30.689144357,
	}
	transformedPoint := gcj02Point.ToWgs84()
	if !comparePoint(transformedPoint, expectedPoint, 1e-8) {
		t.Errorf("expected %v, got %v", expectedPoint, transformedPoint)
	}
}

func TestWgs84ToGcj02(t *testing.T) {
	wgs84Point := Wgs84Point{
		Lon: 104.072178447,
		Lat: 30.689144357,
	}
	expectedPoint := Gcj02Point{
		Lon: 104.074698338,
		Lat: 30.686745304,
	}
	transformedPoint := wgs84Point.ToGcj02()
	if !comparePoint(transformedPoint, expectedPoint, 1e-8) {
		t.Errorf("expected %v, got %v", expectedPoint, transformedPoint)
	}
}

func TestBd09ToWgs84Point(t *testing.T) {
	bd09Point := Bd09Point{
		Lon: 104.081201852,
		Lat: 30.692663693,
	}
	expectedPoint := Wgs84Point{
		Lon: 104.07217476,
		Lat: 30.689142717,
	}
	transformedPoint := bd09Point.ToWgs84()
	if !comparePoint(transformedPoint, expectedPoint, 1e-8) {
		t.Errorf("expected %v, got %v", expectedPoint, transformedPoint)
	}
}

func TestWgs84ToBd09Point(t *testing.T) {
	wgs84Point := Wgs84Point{
		Lon: 104.072178447,
		Lat: 30.689144357,
	}
	expectedPoint := Bd09Point{
		Lon: 104.081201852,
		Lat: 30.692663693,
	}
	transformedPoint := wgs84Point.ToBd09()
	if !comparePoint(transformedPoint, expectedPoint, 1e-8) {
		t.Errorf("expected %v, got %v", expectedPoint, transformedPoint)
	}
}

func comparePoint[T GeometryPointConv](pA, pB T, tolerance float64) bool {
	a := pA.ToRaw()
	b := pB.ToRaw()
	if a.Lon-b.Lon > tolerance || b.Lon-a.Lon > tolerance {
		return false
	}
	if a.Lat-b.Lat > tolerance || b.Lat-a.Lat > tolerance {
		return false
	}
	return true
}
