// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: transaction.proto

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

type TransactionORM struct {
	BlockNumber      uint64 `gorm:"index:transaction_idx_block_number"`
	BlockTimestamp   uint64
	FromAddress      string `gorm:"index:transaction_idx_from_address"`
	Hash             string `gorm:"primary_key"`
	LogIndex         int32  `gorm:"primary_key"`
	ToAddress        string `gorm:"index:transaction_idx_to_address"`
	TransactionFee   string
	TransactionIndex uint32
	Value            string
}

// TableName overrides the default tablename generated by GORM
func (TransactionORM) TableName() string {
	return "transactions"
}

// ToORM runs the BeforeToORM hook if present, converts the fields of this
// object to ORM format, runs the AfterToORM hook, then returns the ORM object
func (m *Transaction) ToORM(ctx context.Context) (TransactionORM, error) {
	to := TransactionORM{}
	var err error
	if prehook, ok := interface{}(m).(TransactionWithBeforeToORM); ok {
		if err = prehook.BeforeToORM(ctx, &to); err != nil {
			return to, err
		}
	}
	to.FromAddress = m.FromAddress
	to.ToAddress = m.ToAddress
	to.Value = m.Value
	to.Hash = m.Hash
	to.BlockNumber = m.BlockNumber
	to.TransactionIndex = m.TransactionIndex
	to.BlockTimestamp = m.BlockTimestamp
	to.TransactionFee = m.TransactionFee
	to.LogIndex = m.LogIndex
	if posthook, ok := interface{}(m).(TransactionWithAfterToORM); ok {
		err = posthook.AfterToORM(ctx, &to)
	}
	return to, err
}

// ToPB runs the BeforeToPB hook if present, converts the fields of this
// object to PB format, runs the AfterToPB hook, then returns the PB object
func (m *TransactionORM) ToPB(ctx context.Context) (Transaction, error) {
	to := Transaction{}
	var err error
	if prehook, ok := interface{}(m).(TransactionWithBeforeToPB); ok {
		if err = prehook.BeforeToPB(ctx, &to); err != nil {
			return to, err
		}
	}
	to.FromAddress = m.FromAddress
	to.ToAddress = m.ToAddress
	to.Value = m.Value
	to.Hash = m.Hash
	to.BlockNumber = m.BlockNumber
	to.TransactionIndex = m.TransactionIndex
	to.BlockTimestamp = m.BlockTimestamp
	to.TransactionFee = m.TransactionFee
	to.LogIndex = m.LogIndex
	if posthook, ok := interface{}(m).(TransactionWithAfterToPB); ok {
		err = posthook.AfterToPB(ctx, &to)
	}
	return to, err
}

// The following are interfaces you can implement for special behavior during ORM/PB conversions
// of type Transaction the arg will be the target, the caller the one being converted from

// TransactionBeforeToORM called before default ToORM code
type TransactionWithBeforeToORM interface {
	BeforeToORM(context.Context, *TransactionORM) error
}

// TransactionAfterToORM called after default ToORM code
type TransactionWithAfterToORM interface {
	AfterToORM(context.Context, *TransactionORM) error
}

// TransactionBeforeToPB called before default ToPB code
type TransactionWithBeforeToPB interface {
	BeforeToPB(context.Context, *Transaction) error
}

// TransactionAfterToPB called after default ToPB code
type TransactionWithAfterToPB interface {
	AfterToPB(context.Context, *Transaction) error
}

// DefaultCreateTransaction executes a basic gorm create call
func DefaultCreateTransaction(ctx context.Context, in *Transaction, db *gorm1.DB) (*Transaction, error) {
	if in == nil {
		return nil, errors1.NilArgumentError
	}
	ormObj, err := in.ToORM(ctx)
	if err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(TransactionORMWithBeforeCreate_); ok {
		if db, err = hook.BeforeCreate_(ctx, db); err != nil {
			return nil, err
		}
	}
	if err = db.Create(&ormObj).Error; err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(TransactionORMWithAfterCreate_); ok {
		if err = hook.AfterCreate_(ctx, db); err != nil {
			return nil, err
		}
	}
	pbResponse, err := ormObj.ToPB(ctx)
	return &pbResponse, err
}

type TransactionORMWithBeforeCreate_ interface {
	BeforeCreate_(context.Context, *gorm1.DB) (*gorm1.DB, error)
}
type TransactionORMWithAfterCreate_ interface {
	AfterCreate_(context.Context, *gorm1.DB) error
}

// DefaultApplyFieldMaskTransaction patches an pbObject with patcher according to a field mask.
func DefaultApplyFieldMaskTransaction(ctx context.Context, patchee *Transaction, patcher *Transaction, updateMask *field_mask1.FieldMask, prefix string, db *gorm1.DB) (*Transaction, error) {
	if patcher == nil {
		return nil, nil
	} else if patchee == nil {
		return nil, errors1.NilArgumentError
	}
	var err error
	for _, f := range updateMask.Paths {
		if f == prefix+"FromAddress" {
			patchee.FromAddress = patcher.FromAddress
			continue
		}
		if f == prefix+"ToAddress" {
			patchee.ToAddress = patcher.ToAddress
			continue
		}
		if f == prefix+"Value" {
			patchee.Value = patcher.Value
			continue
		}
		if f == prefix+"Hash" {
			patchee.Hash = patcher.Hash
			continue
		}
		if f == prefix+"BlockNumber" {
			patchee.BlockNumber = patcher.BlockNumber
			continue
		}
		if f == prefix+"TransactionIndex" {
			patchee.TransactionIndex = patcher.TransactionIndex
			continue
		}
		if f == prefix+"BlockTimestamp" {
			patchee.BlockTimestamp = patcher.BlockTimestamp
			continue
		}
		if f == prefix+"TransactionFee" {
			patchee.TransactionFee = patcher.TransactionFee
			continue
		}
		if f == prefix+"LogIndex" {
			patchee.LogIndex = patcher.LogIndex
			continue
		}
	}
	if err != nil {
		return nil, err
	}
	return patchee, nil
}

// DefaultListTransaction executes a gorm list call
func DefaultListTransaction(ctx context.Context, db *gorm1.DB) ([]*Transaction, error) {
	in := Transaction{}
	ormObj, err := in.ToORM(ctx)
	if err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(TransactionORMWithBeforeListApplyQuery); ok {
		if db, err = hook.BeforeListApplyQuery(ctx, db); err != nil {
			return nil, err
		}
	}
	db, err = gorm2.ApplyCollectionOperators(ctx, db, &TransactionORM{}, &Transaction{}, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(TransactionORMWithBeforeListFind); ok {
		if db, err = hook.BeforeListFind(ctx, db); err != nil {
			return nil, err
		}
	}
	db = db.Where(&ormObj)
	db = db.Order("log_index")
	ormResponse := []TransactionORM{}
	if err := db.Find(&ormResponse).Error; err != nil {
		return nil, err
	}
	if hook, ok := interface{}(&ormObj).(TransactionORMWithAfterListFind); ok {
		if err = hook.AfterListFind(ctx, db, &ormResponse); err != nil {
			return nil, err
		}
	}
	pbResponse := []*Transaction{}
	for _, responseEntry := range ormResponse {
		temp, err := responseEntry.ToPB(ctx)
		if err != nil {
			return nil, err
		}
		pbResponse = append(pbResponse, &temp)
	}
	return pbResponse, nil
}

type TransactionORMWithBeforeListApplyQuery interface {
	BeforeListApplyQuery(context.Context, *gorm1.DB) (*gorm1.DB, error)
}
type TransactionORMWithBeforeListFind interface {
	BeforeListFind(context.Context, *gorm1.DB) (*gorm1.DB, error)
}
type TransactionORMWithAfterListFind interface {
	AfterListFind(context.Context, *gorm1.DB, *[]TransactionORM) error
}
