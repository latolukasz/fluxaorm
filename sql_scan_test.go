package fluxaorm

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestSQLScanTargetNumericParity(t *testing.T) {
	type namedInt int64
	type namedUint uint64
	type namedFloat float64
	type namedString string
	sources := []any{
		nil, []byte(nil), []byte{}, "", "0", "-0", "-1", "+1", "01", "1_000", "0x10", " 1", "1 ", "invalid",
		"9223372036854775807", "9223372036854775808", "-9223372036854775808", "-9223372036854775809",
		"18446744073709551615", "18446744073709551616", "12.5", "1e6", "1e1000", "NaN", "+Inf", "-Inf",
		[]byte("1234567890123456789"), []byte("-7"), []byte("12.5"), []byte("invalid"), []byte("18446744073709551616"),
		int64(0), int64(-1), int64(math.MaxInt64), uint64(0), uint64(math.MaxInt64), uint64(math.MaxInt64) + 1, uint64(math.MaxUint64),
		float64(0.1), float64(1), float64(1e6), math.Inf(1), math.NaN(), math.Copysign(0, -1), float32(0.1), float32(1),
		float32(math.MaxFloat32), float32(math.SmallestNonzeroFloat32), float32(math.Inf(1)), float32(math.Inf(-1)),
		float32(math.NaN()), float32(math.Copysign(0, -1)), float32(1e6), float32(1e-20), float32(-123456.789),
		true, false, int(12), uint(12), int8(-7), uint32(12), namedInt(12), namedUint(12), namedFloat(0.1), namedString("12"),
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	for i, source := range sources {
		t.Run(fmt.Sprintf("%02d_%T", i, source), func(t *testing.T) {
			checkSQLScalarParity(t, source, uint64(99))
			checkSQLScalarParity(t, source, int64(99))
			checkSQLScalarParity(t, source, float64(99))
			for _, initiallyValid := range []bool{false, true} {
				wantInt := sql.NullInt64{Int64: 99, Valid: initiallyValid}
				gotInt := wantInt
				wantErr := wantInt.Scan(source)
				gotErr := SQLScanTarget(&gotInt).(sql.Scanner).Scan(source)
				checkSQLScanErrors(t, wantErr, gotErr)
				if wantInt != gotInt {
					t.Errorf("NullInt64: got %#v, want %#v", gotInt, wantInt)
				}
				wantFloat := sql.NullFloat64{Float64: 99, Valid: initiallyValid}
				gotFloat := wantFloat
				wantErr = wantFloat.Scan(source)
				gotErr = SQLScanTarget(&gotFloat).(sql.Scanner).Scan(source)
				checkSQLScanErrors(t, wantErr, gotErr)
				if wantFloat.Valid != gotFloat.Valid || !sameSQLNumber(wantFloat.Float64, gotFloat.Float64) {
					t.Errorf("NullFloat64: got %#v, want %#v", gotFloat, wantFloat)
				}
			}
		})
	}
}

func checkSQLScalarParity[T uint64 | int64 | float64](t *testing.T, source any, initial T) {
	t.Helper()
	standard := sql.Null[T]{V: initial}
	wantErr := standard.Scan(source)
	want := standard.V
	if source == nil {
		wantErr = fmt.Errorf("converting NULL to %s is unsupported", reflect.TypeOf(initial).Kind())
		want = initial
	}
	got := initial
	gotErr := SQLScanTarget(&got).(sql.Scanner).Scan(source)
	checkSQLScanErrors(t, wantErr, gotErr)
	if !sameSQLNumber(want, got) {
		t.Errorf("%T: got %#v, want %#v", initial, got, want)
	}
}

func sameSQLNumber[T uint64 | int64 | float64](a, b T) bool {
	if x, ok := any(a).(float64); ok {
		y := any(b).(float64)
		return math.Float64bits(x) == math.Float64bits(y) || math.IsNaN(x) && math.IsNaN(y)
	}
	return a == b
}

func checkSQLScanErrors(t *testing.T, want, got error) {
	t.Helper()
	if want == nil && got == nil {
		return
	}
	if want == nil || got == nil || want.Error() != got.Error() {
		t.Errorf("scan error: got %v, want %v", got, want)
	}
}

func TestSQLScanTargetPassesOtherDestinationsThrough(t *testing.T) {
	var text string
	var nullableText sql.NullString
	var boolean bool
	var nullableBool sql.NullBool
	var timestamp time.Time
	var nullableTime sql.NullTime
	var raw []byte
	var arbitrary any
	for _, destination := range []any{&text, &nullableText, &boolean, &nullableBool, &timestamp, &nullableTime, &raw, &arbitrary} {
		if SQLScanTarget(destination) != destination {
			t.Errorf("changed unoptimized destination %T", destination)
		}
	}
}

func TestSQLScanTargetPreservesNilDestinations(t *testing.T) {
	for _, destination := range []any{nil, (*uint64)(nil), (*int64)(nil), (*float64)(nil), (*sql.NullInt64)(nil), (*sql.NullFloat64)(nil)} {
		if SQLScanTarget(destination) != destination {
			t.Errorf("changed typed nil destination %T", destination)
		}
	}
}

func TestSQLScanTargetDoesNotRetainNumericBytes(t *testing.T) {
	input := []byte("1234567890123456789")
	var value uint64
	if err := SQLScanTarget(&value).(sql.Scanner).Scan(input); err != nil {
		t.Fatal(err)
	}
	for i := range input {
		input[i] = '0'
	}
	if value != 1234567890123456789 {
		t.Fatalf("numeric destination retained source storage: %d", value)
	}
}

func TestSQLScanTargetFastPathAllocations(t *testing.T) {
	var unsigned uint64
	var integer int64
	var number float64
	var nullableInt sql.NullInt64
	var nullableFloat sql.NullFloat64
	for _, tc := range []struct {
		name        string
		destination any
		source      any
	}{
		{"uint_bytes", &unsigned, []byte("1234567890123456789")},
		{"int_bytes", &integer, []byte("-1234567890123456789")},
		{"float_bytes", &number, []byte("123456.25")},
		{"float32_decimal", &number, float32(0.1)},
		{"nullable_uint", &nullableInt, uint64(1234567890123456789)},
		{"nullable_float", &nullableFloat, []byte("123456.25")},
		{"nullable_float32", &nullableFloat, float32(0.1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scanner := SQLScanTarget(tc.destination).(sql.Scanner)
			var err error
			allocations := testing.AllocsPerRun(100, func() { err = scanner.Scan(tc.source) })
			if err != nil {
				t.Fatal(err)
			}
			if allocations != 0 {
				t.Fatalf("successful numeric scan allocated %.0f objects", allocations)
			}
		})
	}
}

var sqlScanBenchmarkError error

func BenchmarkSQLScanNumeric(b *testing.B) {
	var integer sql.NullInt64
	var number sql.NullFloat64
	for _, tc := range []struct {
		name        string
		destination sql.Scanner
		source      any
	}{
		{"StandardReference", &integer, uint64(1234567890123456789)},
		{"AdaptedReference", SQLScanTarget(&integer).(sql.Scanner), uint64(1234567890123456789)},
		{"StandardDecimal", &number, []byte("123456.25")},
		{"AdaptedDecimal", SQLScanTarget(&number).(sql.Scanner), []byte("123456.25")},
		{"StandardFloat32", &number, float32(0.1)},
		{"AdaptedFloat32", SQLScanTarget(&number).(sql.Scanner), float32(0.1)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sqlScanBenchmarkError = tc.destination.Scan(tc.source)
			}
			if sqlScanBenchmarkError != nil {
				b.Fatal(sqlScanBenchmarkError)
			}
		})
	}
}
