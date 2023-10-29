/*
Copyright 2020 JM Robles (@jmrobles)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package h2go

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/encoding/unicode"
)

const (
	TCP_PROTOCOL_VERSION_17            int32 = 17
	TCP_PROTOCOL_VERSION_18            int32 = 18
	TCP_PROTOCOL_VERSION_19            int32 = 19
	TCP_PROTOCOL_VERSION_20            int32 = 20
	TCP_PROTOCOL_VERSION_MIN_SUPPORTED int32 = TCP_PROTOCOL_VERSION_17
	TCP_PROTOCOL_VERSION_MAX_SUPPORTED int32 = TCP_PROTOCOL_VERSION_20
)

// Value types
const (
	UNKNOWN            int32 = -1
	NULL               int32 = 0
	BOOLEAN            int32 = 1
	TINYINT            int32 = 2
	SMALLINT           int32 = 3
	INTEGER            int32 = 4
	BIGINT             int32 = 5
	NUMERIC            int32 = 6
	DOUBLE             int32 = 7
	REAL               int32 = 8
	TIME               int32 = 9
	DATE               int32 = 10
	TIMESTAMP          int32 = 11
	VARBINARY          int32 = 12
	VARCHAR            int32 = 13
	VARCHAR_IGNORECASE int32 = 14
	BLOB               int32 = 15
	CLOB               int32 = 16
	ARRAY              int32 = 17
	JAVA_OBJECT        int32 = 18
	JavaObject         int32 = 19
	UUID               int32 = 20
	CHAR               int32 = 21
	GEOMETRY           int32 = 22
	// 1.4.192
	TIMESTAMP_TZ int32 = 24
	// 1.4.195
	ENUM int32 = 25
	// 1.4.198
	INTERVAL int32 = 26
	ROW      int32 = 27
	// 1.4.200
	JSON    int32 = 28
	TIME_TZ int32 = 29
	// 2.0.202
	BINARY   int32 = 30
	DECFLOAT int32 = 31
)

type transfer struct {
	conn    net.Conn
	buff    *bufio.ReadWriter
	version int32
}

func newTransfer(conn net.Conn) transfer {

	buffReader := bufio.NewReader(conn)
	buffWriter := bufio.NewWriter(conn)
	buff := bufio.NewReadWriter(buffReader, buffWriter)
	return transfer{conn: conn, buff: buff}
}

func (t *transfer) reset() {
	t.buff.Reader.Discard(t.buff.Reader.Buffered())
}

func (t *transfer) getVersion() int32 {
	return t.version
}

func (t *transfer) readInt32() (int32, error) {
	var ret int32
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read int32 value from socket")
	}
	return ret, nil
}
func (t *transfer) readInt16() (int16, error) {
	n, err := t.readInt32()
	if err != nil {
		return int16(-1), err
	}
	return int16(n), err
}
func (t *transfer) readInt64() (int64, error) {
	var ret int64
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read int64 value from socket")
	}
	return ret, nil
}

func (t *transfer) readFloat32() (float32, error) {
	var ret float32
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read float32 value from socket")
	}
	return ret, nil
}

func (t *transfer) readFloat64() (float64, error) {
	var ret float64
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read float64 value from socket")
	}
	return ret, nil
}

func (t *transfer) writeInt32(v int32) error {
	return binary.Write(t.buff, binary.BigEndian, v)
}

func (t *transfer) writeInt64(v int64) error {
	return binary.Write(t.buff, binary.BigEndian, v)
}
func (t *transfer) writeFloat64(v float64) error {
	return binary.Write(t.buff, binary.BigEndian, v)
}

func (t *transfer) readString() (string, error) {
	var err error
	n, err := t.readInt32()
	if err != nil {
		return "", errors.Wrapf(err, "can't read string length from socket")
	}
	if n == -1 || n == 0 {
		return "", nil
	}
	buf := make([]byte, n*2)

	n2, err := io.ReadFull(t.buff, buf)
	if err != nil {
		return "", err
	}
	if n2 != len(buf) {
		return "", errors.Errorf("Can't read all data needed")
	}
	dec := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder()
	buf, err = dec.Bytes(buf)
	if err != nil {
		return "", errors.Wrapf(err, "can't convert from UTF-16 a UTF-8 string")
	}
	return string(buf), nil

}

func (t *transfer) writeString(s string) error {
	var err error
	data := []byte(s)
	n := int32(len(data))
	if n == 0 {
		n = -1
	}
	err = t.writeInt32(n)
	if err != nil {
		return errors.Wrapf(err, "can't write string length to socket")
	}
	if n == -1 {
		return nil
	}
	enc := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewEncoder()
	data, err = enc.Bytes(data)
	if err != nil {
		return errors.Wrapf(err, "can't convert to UTF-16")
	}
	/*
		n = int32(len(data))
		for {
			n2, err := t.buff.Write(data[pos:n])
			if err != nil {
				return errors.Wrapf(err, "can't write string to socket")
			}
			pos += int32(n2)
			if pos == n {
				break
			}
		}
	*/
	n2, err := t.buff.Write(data)
	if err != nil {
		return errors.Wrapf(err, "can't write string to socket")
	}
	if n2 != len(data) {
		return errors.Errorf("Data send not equal to wished")
	}
	return nil
}

func (t *transfer) readBytes() ([]byte, error) {
	n, err := t.readInt32()
	if err != nil {
		return nil, errors.Wrapf(err, "can't read bytes length from socket")
	}
	if n == -1 {
		return nil, nil
	}
	return t.readBytesDef(int(n))

}
func (t *transfer) writeBool(b bool) error {
	var v byte = 0
	if b {
		v = 1
	}
	return t.writeByte(v)
}

func (t *transfer) writeByte(b byte) error {
	return t.buff.WriteByte(b)
}

func (t *transfer) writeBytes(data []byte) error {
	var err error
	s := int32(len(data))
	if data == nil || s == 0 {
		s = -1
	}
	err = t.writeInt32(s)
	if err != nil {
		return errors.Wrapf(err, "can't write bytes length to socket")
	}
	if s == -1 {
		return nil
	}
	n, err := t.buff.Write(data)
	if err != nil {
		return errors.Wrapf(err, "can't write bytes to socket")
	}
	if int32(n) != s {
		return errors.Wrapf(err, "can't write all bytes to socket => %d != %d", n, s)
	}
	return nil
}

func (t *transfer) readBool() (bool, error) {
	v, err := t.readByte()
	if err != nil {
		return false, err
	}
	return v == 1, nil
}

func (t *transfer) readByte() (byte, error) {
	v, err := t.buff.ReadByte()
	return v, err
}

func (t *transfer) readLong() (int64, error) {
	var ret int64
	err := binary.Read(t.buff, binary.BigEndian, &ret)
	if err != nil {
		return -1, errors.Wrapf(err, "can't read long value from socket")
	}
	return ret, nil
}
func (t *transfer) readDate() (time.Time, error) {
	n, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2date(n)
	return date, nil
}

func (t *transfer) readTimestamp() (time.Time, error) {
	nDate, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	nNsecs, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2ts(nDate, nNsecs)
	return date, nil
}

func (t *transfer) readTimestampTZ() (time.Time, error) {
	nDate, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	nNsecs, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	nDiffTZ, err := t.readInt32()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2tsz(nDate, nNsecs, nDiffTZ)
	return date, nil
}

func (t *transfer) flush() error {
	return t.buff.Flush()
}

func (t *transfer) readValue() (interface{}, error) {
	var err error
	kind, err := t.readInt32()
	if err != nil {
		return nil, errors.Wrapf(err, "can't read type of value")
	}
	L(log.DebugLevel, "Value type: %d", kind)
	switch kind {
	case NULL:
		// TODO: review
		return nil, nil
	case VARBINARY:
		return t.readBytes()
	case UUID:
		return nil, errors.Errorf("UUID not implemented")
	case JavaObject:
		return nil, errors.Errorf("Java Object not implemented")
	case BOOLEAN:
		return t.readBool()
	case TINYINT:
		return t.readByte()
	case DATE:
		return t.readDate()
	case TIME:
		return t.readTime()
	case TIME_TZ:
		return t.readTimeTZ()
	case TIMESTAMP:
		return t.readTimestamp()
	case TIMESTAMP_TZ:
		return t.readTimestampTZ()
	case NUMERIC:
		return nil, errors.Errorf("Decimal not implemented")
	case DOUBLE:
		return t.readFloat64()
	case REAL:
		return t.readFloat32()
	case ENUM:
		return nil, errors.Errorf("Enum not implemented")
	case INTEGER:
		return t.readInt32()
	case BIGINT:
		return t.readLong()
	case SMALLINT:
		return t.readInt16()
	case VARCHAR:
		return t.readString()
	case VARCHAR_IGNORECASE:
		return t.readString()
	case CHAR:
		return t.readString()
	case BLOB:
		return nil, errors.Errorf("Blob not implemented")
	case CLOB:
		return nil, errors.Errorf("Clob not implemented")
	case ARRAY:
		return nil, errors.Errorf("Array not implemented")
	case ROW:
		return nil, errors.Errorf("Row not implemented")
	case JAVA_OBJECT:
		return nil, errors.Errorf("Result Set not implemented")
	case GEOMETRY:
		return nil, errors.Errorf("Geometry not implemented")
	case JSON:
		return nil, errors.Errorf("JSON not implemented")
	default:
		L(log.ErrorLevel, "Unknown type: %d", kind)
		return nil, errors.Errorf("Unknown type: %d", kind)
	}

}

func (t *transfer) writeValue(v interface{}) error {
	switch kind := v.(type) {
	case nil:
		t.writeInt32(NULL)
	case bool:
		t.writeInt32(BOOLEAN)
		t.writeBool(v.(bool))
	case int:
		s := unsafe.Sizeof(v)
		if s == 4 {
			t.writeInt32(INTEGER)
			t.writeInt32(int32(v.(int)))
		} else {
			// 8 bytes
			t.writeInt32(BIGINT)
			t.writeInt64(int64(v.(int)))
		}
	case int16:
		t.writeInt32(SMALLINT)
		t.writeInt32(v.(int32))
	case int32:
		t.writeInt32(INTEGER)
		t.writeInt32(int32(v.(int32)))
	case int64:
		t.writeInt32(BIGINT)
		t.writeInt64(int64(v.(int64)))
	case float64:
		t.writeInt32(NUMERIC)
		t.writeFloat64(v.(float64))
	case string:
		t.writeInt32(VARCHAR)
		t.writeString(v.(string))
	case byte:
		t.writeInt32(TINYINT)
		t.writeByte(v.(byte))
	case []byte:
		t.writeInt32(VARBINARY)
		t.writeBytes(v.([]byte))
	// case time.Time:
	default:
		return fmt.Errorf("Can't convert type %T to H2 Type", kind)
	}
	return nil
}
func (t *transfer) writeDatetimeValue(dt time.Time, mdp h2parameter) error {
	var kind int32
	if t.getVersion() < TCP_PROTOCOL_VERSION_20 {
		kind = mdp.typeinfo.(TypeInfo19).Type
	} else {
		kind = mdp.typeinfo.(TypeInfo20).Type
	}
	L(log.DebugLevel, "Date/time type: %d", kind)
	var err error
	switch kind {
	case DATE:
		t.writeInt32(DATE)
		bin := date2bin(&dt)
		err = t.writeInt64(bin)
		if err != nil {
			return err
		}
	case TIMESTAMP:
		t.writeInt32(TIMESTAMP)
		dateBin, nsecBin := ts2bin(&dt)
		err = t.writeInt64(dateBin)
		if err != nil {
			return err
		}
		err = t.writeInt64(nsecBin)
		if err != nil {
			return err
		}
	case TIMESTAMP_TZ:
		t.writeInt32(TIMESTAMP_TZ)
		dateBin, nsecBin, offsetTZBin := tsz2bin(&dt)
		err = t.writeInt64(dateBin)
		if err != nil {
			return err
		}
		err = t.writeInt64(nsecBin)
		if err != nil {
			return err
		}
		err = t.writeInt32(offsetTZBin)
		if err != nil {
			return err
		}
	case TIME:
		t.writeInt32(TIME)
		nsecBin := time2bin(&dt)
		err = t.writeInt64(nsecBin)
		if err != nil {
			return err
		}
	case TIME_TZ:
		t.writeInt32(TIME_TZ)
		nsecBin, offsetTZBin := timetz2bin(&dt)
		err = t.writeInt64(nsecBin)
		if err != nil {
			return err
		}
		err = t.writeInt32(offsetTZBin)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Datatype unsupported: %d", kind)
	}
	return nil
}
func (t *transfer) readBytesDef(n int) ([]byte, error) {

	buf := make([]byte, n)
	n2, err := t.buff.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != n2 {
		return nil, errors.Errorf("Read byte size differs: %d != %d", n, n2)
	}
	return buf, nil

}
func (t *transfer) close() error {
	// TODO: check close
	return nil
}

// Helpers

func date2bin(dt *time.Time) int64 {
	return int64((dt.Year() << 9) + (int(dt.Month()) << 5) + dt.Day())
}

func bin2date(n int64) time.Time {
	day := int(n & 0x1f)
	month := time.Month((n >> 5) & 0xf)
	year := int(n >> 9)
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func ts2bin(dt *time.Time) (int64, int64) {
	var nsecBin int64
	dateBin := date2bin(dt)
	nsecBin = int64(dt.Hour()*3600 + dt.Minute()*60 + dt.Second())
	nsecBin *= int64(1e9)
	nsecBin += int64(dt.Nanosecond())
	return dateBin, nsecBin
}

func bin2ts(dateBin int64, nsecBin int64) time.Time {
	// TODO: optimization
	day := int(dateBin & 0x1f)
	month := time.Month((dateBin >> 5) & 0xf)
	year := int(dateBin >> 9)
	nsecs := int(nsecBin % int64(1e9))
	nsecBin = nsecBin / int64(1e9)
	sec := int(nsecBin % 60)
	nsecBin = nsecBin / 60
	minute := int(nsecBin % 60)
	hour := int(nsecBin / 60)
	return time.Date(year, month, day, hour, minute, sec, nsecs, time.UTC)
}

func bin2tsz(dateBin int64, nsecBin int64, secsTZ int32) time.Time {
	// TODO: optimization
	day := int(dateBin & 0x1f)
	month := time.Month((dateBin >> 5) & 0xf)
	year := int(dateBin >> 9)
	nsecs := int(nsecBin % int64(1e9))
	nsecBin = nsecBin / int64(1e9)
	sec := int(nsecBin % 60)
	nsecBin = nsecBin / 60
	minute := int(nsecBin % 60)
	hour := int(nsecBin / 60)
	tz := time.FixedZone(fmt.Sprintf("tz_%d", secsTZ), int(secsTZ))
	return time.Date(year, month, day, hour, minute, sec, nsecs, tz)
}

func tsz2bin(dt *time.Time) (int64, int64, int32) {
	var nsecBin int64
	dateBin := date2bin(dt)
	nsecBin = int64(dt.Hour()*3600 + dt.Minute()*60 + dt.Second())
	nsecBin *= int64(1e9)
	nsecBin += int64(dt.Nanosecond())
	_, offsetTZ := dt.Zone()
	return dateBin, nsecBin, int32(offsetTZ)
}

func time2bin(dt *time.Time) int64 {
	var nsecBin int64
	nsecBin = int64(dt.Hour()*3600 + dt.Minute()*60 + dt.Second())
	nsecBin *= int64(1e9)
	nsecBin += int64(dt.Nanosecond())
	return nsecBin
}

func bin2time(nsecBin int64) time.Time {
	// TODO: optimization
	nsecs := int(nsecBin % int64(1e9))
	nsecBin = nsecBin / int64(1e9)
	sec := int(nsecBin % 60)
	nsecBin = nsecBin / 60
	minute := int(nsecBin % 60)
	hour := int(nsecBin / 60)
	return time.Date(0, 1, 1, hour, minute, sec, nsecs, time.UTC)
}

func (t *transfer) readTime() (time.Time, error) {
	nNsecs, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2time(nNsecs)
	return date, nil
}

func (t *transfer) readTimeTZ() (time.Time, error) {
	nNsecs, err := t.readInt64()
	if err != nil {
		return time.Time{}, err
	}
	nDiffTZ, err := t.readInt32()
	if err != nil {
		return time.Time{}, err
	}
	date := bin2timetz(nNsecs, nDiffTZ)
	return date, nil
}

func bin2timetz(nsecBin int64, secsTZ int32) time.Time {
	// TODO: optimization
	nsecs := int(nsecBin % int64(1e9))
	nsecBin = nsecBin / int64(1e9)
	sec := int(nsecBin % 60)
	nsecBin = nsecBin / 60
	minute := int(nsecBin % 60)
	hour := int(nsecBin / 60)
	tz := time.FixedZone(fmt.Sprintf("tz_%d", secsTZ), int(secsTZ))
	return time.Date(0, 1, 1, hour, minute, sec, nsecs, tz)
}

func timetz2bin(dt *time.Time) (int64, int32) {
	var nsecBin int64
	nsecBin = int64(dt.Hour()*3600 + dt.Minute()*60 + dt.Second())
	nsecBin *= int64(1e9)
	nsecBin += int64(dt.Nanosecond())
	_, offsetTZ := dt.Zone()
	return nsecBin, int32(offsetTZ)
}

type TypeInfo19 struct {
	Type      int32
	Precision int64
	Scale     int32
}

func (t *transfer) getTypeInfo19() (info TypeInfo19, err error) {
	// Skip other info
	// - Value type (int)
	info.Type, err = t.readInt32()
	if err != nil {
		return
	}
	// - Precision (long)
	info.Precision, err = t.readLong()
	if err != nil {
		return
	}
	// - Scale (int)
	info.Scale, err = t.readInt32()
	return
}

type TypeInfo20 struct {
	Type            int32
	Precision       int64
	Scale           int32
	ExtTypeInfoFlag bool
}

func (t *transfer) getTypeInfo20() (info TypeInfo20, err error) {
	// Skip other info
	// - Value type (int)
	info.Type, err = t.readInt32()
	if err != nil {
		return
	}

	switch info.Type {
	case UNKNOWN, NULL, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DATE, UUID:
		return
	case CHAR, VARCHAR, VARCHAR_IGNORECASE, BINARY, VARBINARY, DECFLOAT, JAVA_OBJECT, JSON:
		// - Precision (int32)
		var precision int32
		precision, err = t.readInt32()
		if err != nil {
			return
		}
		info.Precision = int64(precision)
	case CLOB, BLOB:
		// - Precision (long)
		info.Precision, err = t.readLong()
		if err != nil {
			return
		}
	case NUMERIC:
		// - Precision (int32)
		var precision int32
		precision, err = t.readInt32()
		if err != nil {
			return
		}
		info.Precision = int64(precision)
		// - Scale (int)
		info.Scale, err = t.readInt32()
	// TO DO more types
	default:
		return info, fmt.Errorf("handle typeinfo20 for type %d is not implemented", info.Type)
	}

	return
}
