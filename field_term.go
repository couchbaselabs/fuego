//  Copyright (c) 2017 Couchbase, Inc.
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
	"strings"
)

type fieldTerm struct {
	field uint16
	term  string
}

type fieldTerms []fieldTerm

func (a fieldTerms) Len() int {
	return len(a)
}

func (a fieldTerms) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a fieldTerms) Less(i, j int) bool {
	if a[i].field < a[j].field {
		return true
	}

	return strings.Compare(a[i].term, a[j].term) < 0
}
