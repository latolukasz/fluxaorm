package fluxaorm

import (
	"database/sql"
	"errors"
	"strconv"
)

// SQLScanTarget adapts numeric destinations used by generated batch readers.
// The adapters share the destination itself, not a separately allocated wrapper.
// Successful text conversions avoid database/sql's temporary numeric strings;
// uncommon sources and failed conversions retain database/sql's behavior.
func SQLScanTarget(destination any) any {
	switch value := destination.(type) {
	case *uint64:
		if value == nil {
			return destination
		}
		return (*sqlScanUint64)(value)
	case *int64:
		if value == nil {
			return destination
		}
		return (*sqlScanInt64)(value)
	case *float64:
		if value == nil {
			return destination
		}
		return (*sqlScanFloat64)(value)
	case *sql.NullInt64:
		if value == nil {
			return destination
		}
		return (*sqlScanNullInt64)(value)
	case *sql.NullFloat64:
		if value == nil {
			return destination
		}
		return (*sqlScanNullFloat64)(value)
	default:
		return destination
	}
}

type sqlScanUint64 uint64

func (destination *sqlScanUint64) Scan(source any) error {
	if value, ok := sqlScanUint(source); ok {
		*destination = sqlScanUint64(value)
		return nil
	}
	if source == nil {
		return errors.New("converting NULL to uint64 is unsupported")
	}
	value := sql.Null[uint64]{V: uint64(*destination)}
	if err := value.Scan(source); err != nil {
		return err
	}
	*destination = sqlScanUint64(value.V)
	return nil
}

type sqlScanInt64 int64

func (destination *sqlScanInt64) Scan(source any) error {
	if value, ok := sqlScanInt(source); ok {
		*destination = sqlScanInt64(value)
		return nil
	}
	if source == nil {
		return errors.New("converting NULL to int64 is unsupported")
	}
	value := sql.NullInt64{Int64: int64(*destination)}
	if err := value.Scan(source); err != nil {
		return err
	}
	*destination = sqlScanInt64(value.Int64)
	return nil
}

type sqlScanFloat64 float64

func (destination *sqlScanFloat64) Scan(source any) error {
	if value, ok := sqlScanFloat(source); ok {
		*destination = sqlScanFloat64(value)
		return nil
	}
	if source == nil {
		return errors.New("converting NULL to float64 is unsupported")
	}
	value := sql.NullFloat64{Float64: float64(*destination)}
	if err := value.Scan(source); err != nil {
		return err
	}
	*destination = sqlScanFloat64(value.Float64)
	return nil
}

type sqlScanNullInt64 sql.NullInt64

func (destination *sqlScanNullInt64) Scan(source any) error {
	if value, ok := sqlScanInt(source); ok {
		destination.Int64, destination.Valid = value, true
		return nil
	}
	return (*sql.NullInt64)(destination).Scan(source)
}

type sqlScanNullFloat64 sql.NullFloat64

func (destination *sqlScanNullFloat64) Scan(source any) error {
	if value, ok := sqlScanFloat(source); ok {
		destination.Float64, destination.Valid = value, true
		return nil
	}
	return (*sql.NullFloat64)(destination).Scan(source)
}

func sqlScanUint(source any) (uint64, bool) {
	switch value := source.(type) {
	case uint64:
		return value, true
	case int64:
		if value >= 0 {
			return uint64(value), true
		}
	case []byte:
		parsed, err := strconv.ParseUint(string(value), 10, 64)
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseUint(value, 10, 64)
		return parsed, err == nil
	}
	return 0, false
}

func sqlScanInt(source any) (int64, bool) {
	switch value := source.(type) {
	case int64:
		return value, true
	case uint64:
		if value <= 1<<63-1 {
			return int64(value), true
		}
	case []byte:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		return parsed, err == nil
	}
	return 0, false
}

func sqlScanFloat(source any) (float64, bool) {
	switch value := source.(type) {
	case float64:
		return value, true
	case float32:
		// Match database/sql's decimal round trip exactly. A direct cast would
		// turn float32(0.1) into 0.10000000149011612 instead of 0.1.
		var buffer [32]byte
		digits := strconv.AppendFloat(buffer[:0], float64(value), 'g', -1, 32)
		parsed, err := strconv.ParseFloat(string(digits), 64)
		return parsed, err == nil
	case []byte:
		parsed, err := strconv.ParseFloat(string(value), 64)
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(value, 64)
		return parsed, err == nil
	}
	return 0, false
}
