// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: log_count_by_public_key.proto

package models

import (
	context "context"
	fmt "fmt"
	
	_ "github.com/infobloxopen/protoc-gen-gorm/options"
	math "math"

	gorm2 "github.com/infobloxopen/atlas-app-toolkit/gorm"
	errors1 "github.com/infobloxopen/protoc-gen-gorm/errors"
	gorm1 "github.com/jinzhu/gorm"
	field_mask1 "google.golang.org/genproto/protobuf/field_mask"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = fmt.Errorf
var _ = math.Inf

type LogCountByPublicKeyORM struct {
	Count           uint64 `gorm:"index:log_count_by_address_idx_count"`
	LogIndex        uint64
	PublicKey       string `gorm:"primary_key"`
	TransactionHash string
}

// TableName overrides the default tablename generated by GORM
func (LogCountByPublicKeyORM) TableName() string {
	return "log_count_by_public_keys"
}

// ToORM runs the BeforeToORM hook if present, converts the fields of this
// object to ORM format, runs the AfterToORM hook, then returns the ORM object
func (m *LogCountByPublicKey) ToORM(ctx context.Context) (LogCountByPublicKeyORM, error) {
	to := LogCountByPublicKeyORM{}
	var err error
	if prehook, ok := interface{}(m).(LogCountByPublicKeyWithBeforeToORM); ok {
		if err = prehook.BeforeToORM(ctx, &to); err != nil {
			return to, err
		}
	}
	to.PublicKey = m.PublicKey
	to.TransactionHash = m.TransactionHash
	to.LogIndex = m.LogIndex
	to.Count = m.Count
	if posthook, ok := interface{}(m).(LogCountByPublicKeyWithAfterToORM); ok {
		err = posthook.AfterToORM(ctx, &to)
	}
	return to, err
}

// ToPB runs the BeforeToPB hook if present, converts the fields of this
// object to PB format, runs the AfterToPB hook, then returns the PB object
func (m *LogCountByPublicKeyORM) ToPB(ctx context.Context) (LogCountByPublicKey, error) {
	to := LogCountByPublicKey{}
	var err error
	if prehook, ok := interface{}(m).(LogCountByPublicKeyWithBeforeToPB); ok {
		if err = prehook.BeforeToPB(ctx, &to); err != nil {
			return to, err
		}
	}
	to.PublicKey = m.PublicKey
	to.TransactionHash = m.TransactionHash
	to.LogIndex = m.LogIndex
	to.Count = m.Count
	if posthook, ok := interface{}(m).(LogCountByPublicKeyWithAfterToPB); ok {
		err = posthook.AfterToPB(ctx, &to)
	}
	return to, err
}

// The following are interfaces you can implement for special behavior during ORM/PB conversions
// of type LogCountByPublicKey the arg will be the target, the caller the one being converted from

// LogCountByPublicKeyBeforeToORM called before default ToORM code
type LogCountByPublicKeyWithBeforeToORM interface {
	BeforeToORM(context.Context, *LogCountByPublicKeyORM) error
}

// LogCountByPublicKeyAfterToORM called after default ToORM code
type LogCountByPublicKeyWithAfterToORM interface {
	AfterToORM(context.Context, *LogCountByPublicKeyORM) error
}

// LogCountByPublicKeyBeforeToPB called before default ToPB code
type LogCountByPublicKeyWithBeforeToPB interface {
	BeforeToPB(context.Context, *LogCountByPublicKey) error
}

// LogCountByPublicKeyAfterToPB called after default ToPB code
type LogCountByPublicKeyWithAfterToPB interface {
	AfterToPB(context.Context, *LogCountByPublicKey) error
}

// DefaultCreateLogCountByPublicKey executes a basic gorm create call
func DefaultCreateLogCountByPublicKey(ctx context.Context, in *LogCountByPublicKey, db *gorm1.DB) (*LogCountByPublicKey, error) {
	if in == nil {
		return nil, errors1.NilArgumentError
	}
	ormObj, err := in.ToORM(ctx)
	if err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(LogCountByPublicKeyORMWithBeforeCreate_); ok {
		if db, err = hook.BeforeCreate_(ctx, db); err != nil {
			return nil, err
		}
	}
	if err = db.Create(&ormObj).Error; err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(LogCountByPublicKeyORMWithAfterCreate_); ok {
		if err = hook.AfterCreate_(ctx, db); err != nil {
			return nil, err
		}
	}
	pbResponse, err := ormObj.ToPB(ctx)
	return &pbResponse, err
}

type LogCountByPublicKeyORMWithBeforeCreate_ interface {
	BeforeCreate_(context.Context, *gorm1.DB) (*gorm1.DB, error)
}
type LogCountByPublicKeyORMWithAfterCreate_ interface {
	AfterCreate_(context.Context, *gorm1.DB) error
}

// DefaultApplyFieldMaskLogCountByPublicKey patches an pbObject with patcher according to a field mask.
func DefaultApplyFieldMaskLogCountByPublicKey(ctx context.Context, patchee *LogCountByPublicKey, patcher *LogCountByPublicKey, updateMask *field_mask1.FieldMask, prefix string, db *gorm1.DB) (*LogCountByPublicKey, error) {
	if patcher == nil {
		return nil, nil
	} else if patchee == nil {
		return nil, errors1.NilArgumentError
	}
	var err error
	for _, f := range updateMask.Paths {
		if f == prefix+"PublicKey" {
			patchee.PublicKey = patcher.PublicKey
			continue
		}
		if f == prefix+"TransactionHash" {
			patchee.TransactionHash = patcher.TransactionHash
			continue
		}
		if f == prefix+"LogIndex" {
			patchee.LogIndex = patcher.LogIndex
			continue
		}
		if f == prefix+"Count" {
			patchee.Count = patcher.Count
			continue
		}
	}
	if err != nil {
		return nil, err
	}
	return patchee, nil
}

// DefaultListLogCountByPublicKey executes a gorm list call
func DefaultListLogCountByPublicKey(ctx context.Context, db *gorm1.DB) ([]*LogCountByPublicKey, error) {
	in := LogCountByPublicKey{}
	ormObj, err := in.ToORM(ctx)
	if err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(LogCountByPublicKeyORMWithBeforeListApplyQuery); ok {
		if db, err = hook.BeforeListApplyQuery(ctx, db); err != nil {
			return nil, err
		}
	}
	db, err = gorm2.ApplyCollectionOperators(ctx, db, &LogCountByPublicKeyORM{}, &LogCountByPublicKey{}, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(LogCountByPublicKeyORMWithBeforeListFind); ok {
		if db, err = hook.BeforeListFind(ctx, db); err != nil {
			return nil, err
		}
	}
	db = db.Where(&ormObj)
	db = db.Order("public_key")
	ormResponse := []LogCountByPublicKeyORM{}
	if err := db.Find(&ormResponse).Error; err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(LogCountByPublicKeyORMWithAfterListFind); ok {
		if err = hook.AfterListFind(ctx, db, &ormResponse); err != nil {
			return nil, err
		}
	}
	pbResponse := []*LogCountByPublicKey{}
	for _, responseEntry := range ormResponse {
		temp, err := responseEntry.ToPB(ctx)
		if err != nil {
			return nil, err
		}
		pbResponse = append(pbResponse, &temp)
	}
	return pbResponse, nil
}

type LogCountByPublicKeyORMWithBeforeListApplyQuery interface {
	BeforeListApplyQuery(context.Context, *gorm1.DB) (*gorm1.DB, error)
}
type LogCountByPublicKeyORMWithBeforeListFind interface {
	BeforeListFind(context.Context, *gorm1.DB) (*gorm1.DB, error)
}
type LogCountByPublicKeyORMWithAfterListFind interface {
	AfterListFind(context.Context, *gorm1.DB, *[]LogCountByPublicKeyORM) error
}