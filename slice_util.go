//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// +build !safe

package fuego

import (
	"reflect"
	"unsafe"
)

// Uint64SliceToByteSlice gives access to []uint64 as []byte.  By
// default, an efficient O(1) implementation of this function is used,
// but which requires the unsafe package.  See the "safe" build tag to
// use an O(N) implementation that does not need the unsafe package.
func Uint64SliceToByteSlice(in []uint64) ([]byte, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))

	var out []byte
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len * 8
	outHeader.Cap = inHeader.Cap * 8

	return out, nil
}

// ByteSliceToUint64Slice gives access to []byte as []uint64.  By
// default, an efficient O(1) implementation of this function is used,
// but which requires the unsafe package.  See the "safe" build tag to
// use an O(N) implementation that does not need the unsafe package.
func ByteSliceToUint64Slice(in []byte) ([]uint64, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))

	var out []uint64
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len / 8
	outHeader.Cap = outHeader.Len

	return out, nil
}

// --------------------------------------------------------------

// Uint32SliceToByteSlice gives access to []uint32 as []byte.  By
// default, an efficient O(1) implementation of this function is used,
// but which requires the unsafe package.  See the "safe" build tag to
// use an O(N) implementation that does not need the unsafe package.
func Uint32SliceToByteSlice(in []uint32) ([]byte, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))

	var out []byte
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len * 4
	outHeader.Cap = inHeader.Cap * 4

	return out, nil
}

// ByteSliceToUint32Slice gives access to []byte as []uint32.  By
// default, an efficient O(1) implementation of this function is used,
// but which requires the unsafe package.  See the "safe" build tag to
// use an O(N) implementation that does not need the unsafe package.
func ByteSliceToUint32Slice(in []byte) ([]uint32, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))

	var out []uint32
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len / 4
	outHeader.Cap = outHeader.Len

	return out, nil
}

// --------------------------------------------------------------

// Uint16SliceToByteSlice gives access to []uint16 as []byte.  By
// default, an efficient O(1) implementation of this function is used,
// but which requires the unsafe package.  See the "safe" build tag to
// use an O(N) implementation that does not need the unsafe package.
func Uint16SliceToByteSlice(in []uint16) ([]byte, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))

	var out []byte
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len * 2
	outHeader.Cap = inHeader.Cap * 2

	return out, nil
}

// ByteSliceToUint16Slice gives access to []byte as []uint16.  By
// default, an efficient O(1) implementation of this function is used,
// but which requires the unsafe package.  See the "safe" build tag to
// use an O(N) implementation that does not need the unsafe package.
func ByteSliceToUint16Slice(in []byte) ([]uint16, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))

	var out []uint16
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len / 2
	outHeader.Cap = outHeader.Len

	return out, nil
}

// --------------------------------------------------------------

func endian() string { // See golang-nuts / how-to-tell-endian-ness-of-machine,
	var x uint32 = 0x01020304
	if *(*byte)(unsafe.Pointer(&x)) == 0x01 {
		return "big"
	}
	return "little"
}
