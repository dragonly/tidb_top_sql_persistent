/*
Copyright © 2021 Li Yilong <liyilongko@gmail.com>

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

package app

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func testPlanBinaryDecoderFunc(_ string) (string, error) {
	return "", nil
}

func TestTopSQL_RegisterNormalizedSQL(t *testing.T) {
	assert := assert.New(t)
	ts, err := NewTopSQL(testPlanBinaryDecoderFunc, 1000)
	assert.NoError(err, "NewTopSQL should not return error")

	for i := 0; i < 100; i++ {
		key := "digest" + strconv.Itoa(i)
		value := "normalized" + strconv.Itoa(i)
		ts.RegisterNormalizedSQL(key, value)
		time.Sleep(10 * time.Millisecond)
		value1, found := ts.normalizedSQLCache.Get(key)
		assert.Equal(true, found, "expect cache to get key '%s'", key)
		assert.Equal(value1, value, "expect cache['%s']='$s'", key, value)
	}
	for i := 0; i < 100; i++ {
		key := "digest" + strconv.Itoa(i)
		t.Log(ts.normalizedSQLCache.Get(key))
	}
}
