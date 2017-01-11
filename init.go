//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuego

import (
	"fmt"
	"runtime"

	"github.com/blevesearch/bleve/registry"
)

const Name = "fuego"

// RowBufferSize should ideally this is sized to be the smallest
// size that can contain an index row key and its corresponding
// value.  It is not a limit, if need be a larger buffer is
// allocated, but performance will be more optimal if *most*
// rows fit this size.
const RowBufferSize = 4 * 1024

var VersionKey = []byte{'v'}

const Version uint8 = 6

var IncompatibleVersion = fmt.Errorf("incompatible version, %d is supported", Version)

func init() {
	registry.RegisterIndexType(Name, NewFuego)

	AnalyzeAuxQueue = NewAnalyzeAuxQueue(0, runtime.GOMAXPROCS(runtime.NumCPU()))
}
