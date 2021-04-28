package vindexes

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn = (*SlotMask)(nil)
	_ Reversible   = (*SlotMask)(nil)
)

type shardParams struct {
	rangeSize uint64
	slotRange uint64
	mask      uint64
}

// SlotMask defines vindex that applies a mask on the number and multiplies it by the shard size
// It's Unique, Reversible and Functional.
type SlotMask struct {
	name string
	*shardParams
}

// NewSlotMask creates a new SlotMask.
func NewSlotMask(name string, m map[string]string) (Vindex, error) {
	shard := &shardParams{}

	if shardRangeSize, ok := m["shard_range_size"]; ok {
		rangeSize, err := strconv.ParseUint(shardRangeSize, 16, 64)
		if err != nil {
			return nil, err
		}
		shard.rangeSize = rangeSize
	}

	if shardSlotRange, ok := m["shard_slot_range"]; ok {
		slotRange, err := strconv.ParseUint(shardSlotRange, 16, 64)
		if err != nil {
			return nil, err
		}
		shard.slotRange = slotRange
	}

	if shardMask, ok := m["shard_slot_mask"]; ok {
		mask, err := strconv.ParseUint(shardMask, 16, 64)
		if err != nil {
			return nil, err
		}
		shard.mask = mask
	}

	return &SlotMask{name: name, shardParams: shard}, nil
}

// String returns the name of the vindex.
func (vind *SlotMask) String() string {
	return vind.name
}

// Cost returns the cost of this index as 1.
func (vind *SlotMask) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *SlotMask) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *SlotMask) NeedsVCursor() bool {
	return false
}

// Map returns the corresponding KeyspaceId values for the given ids.
func (vind *SlotMask) Map(_ VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	for _, id := range ids {
		ksId, _ := vind.IdToKeyspaceId(id)
		out = append(out, key.DestinationKeyspaceID(ksId))
	}

	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *SlotMask) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		ksId, err := vind.IdToKeyspaceId(ids[i])
		if err != nil {
			return nil, err
		}
		out[i] = bytes.Equal(ksId, ksids[i])
	}
	return out, nil
}

// ReverseMap returns the ids from ksids.
func (vind *SlotMask) ReverseMap(_ VCursor, ksids [][]byte) ([]sqltypes.Value, error) {
	reverseIds := make([]sqltypes.Value, 0, len(ksids))
	for _, keyspaceID := range ksids {
		val, err := vind.KeyspaceIdToId(keyspaceID)
		if err != nil {
			return reverseIds, err
		}
		reverseIds = append(reverseIds, val)
	}

	return reverseIds, nil
}

func (vind *SlotMask) IdToKeyspaceId(id sqltypes.Value) ([]byte, error) {
	num, err := evalengine.ToUint64(id)
	var keybytes [8]byte

	if err != nil {
		return keybytes[:], err
	}

	slot := num & vind.shardParams.mask
	ksID := num + vind.shardParams.rangeSize*(slot/vind.shardParams.slotRange)

	binary.BigEndian.PutUint64(keybytes[:], ksID)

	return keybytes[:], nil
}

func (vind *SlotMask) KeyspaceIdToId(ksId []byte) (sqltypes.Value, error) {
	if len(ksId) != 8 {
		return sqltypes.NULL, fmt.Errorf("Numeric.ReverseMap: length of keyspaceId is not 8: %d", len(ksId))
	}

	val := binary.BigEndian.Uint64(ksId)
	slot := val & vind.shardParams.mask
	id := val - vind.shardParams.rangeSize*(slot/vind.shardParams.slotRange)

	return sqltypes.NewUint64(id), nil
}

func init() {
	Register("slot_mask", NewSlotMask)
}
