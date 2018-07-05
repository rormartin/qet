// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package transform

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"text/template"
)

const testTemplate = "test {{.test1}} for unit test {{.test2}}"

func TestPayloadGeneratorConstructor(t *testing.T) {

	tmpl := template.Must(template.New("test.gotpl").Parse(testTemplate))
	getType := func(map[string]interface{}) ([]string, error) {
		return []string{"test"}, nil
	}

	t.Run("Ok - full extension", func(t *testing.T) {
		pg := NewPayloadGenerator(tmpl, getType, ".gotpl")
		assert.Equal(t, tmpl, pg.templates)
		assert.Equal(t, ".gotpl", pg.tplExt)
	})

	t.Run("Ok - extension with implicit separator", func(t *testing.T) {
		pg := NewPayloadGenerator(tmpl, getType, "gotpl")
		assert.Equal(t, tmpl, pg.templates)
		assert.Equal(t, ".gotpl", pg.tplExt)
	})
}

func TestEventTemplate(t *testing.T) {
	data := map[string]interface{}{
		"test1": "val1",
		"test2": "val2",
	}

	tmpl := template.Must(template.New("test.gotpl").Parse(testTemplate))

	t.Run("Ok", func(t *testing.T) {
		res, err := executeEventTemplate(tmpl, []string{"test"}, data, ".gotpl")
		if err != nil {
			t.Errorf("Error in event execution not expected: %v", err)
		}

		const testResult = "test val1 for unit test val2"
		assert.Equal(t, testResult, string(res))
	})

	t.Run("MissingValue", func(t *testing.T) {
		data := map[string]interface{}{
			"test1": "val1",
		}
		_, err := executeEventTemplate(tmpl, []string{"test"}, data, ".gotpl")
		assert.Errorf(t, err, "expected error for missing value")
	})

	t.Run("MissingAllTemplate", func(t *testing.T) {
		_, err := executeEventTemplate(tmpl, []string{"error"}, data, ".gotpl")
		assert.Errorf(t, err, "expected error for missing template")
	})

	t.Run("MissingOneTemplate(and success)", func(t *testing.T) {
		res, err := executeEventTemplate(tmpl, []string{"error", "test"}, data, ".gotpl")
		if err != nil {
			t.Errorf("Error in event execution not expected: %v", err)
		}

		const testResult = "test val1 for unit test val2"
		assert.Equal(t, testResult, string(res))
	})

}

func TestGenEventPayload(t *testing.T) {

	tmpl := template.Must(template.New("test.gotpl").Parse(testTemplate))
	jsonTX := []byte(`{ "test2": "val2" }`)
	config := map[string]interface{}{
		"test1": "val1",
	}

	getType := func(map[string]interface{}) ([]string, error) {
		return []string{"test"}, nil
	}

	pg := NewPayloadGenerator(tmpl, getType, ".gotpl")

	t.Run("Ok", func(t *testing.T) {
		res, err := pg.GenEventPayload(jsonTX, config)
		if err != nil {
			t.Errorf("Error in event generation payload: %v", err)
		}

		const testResult = "test val1 for unit test val2"
		assert.Equal(t, testResult, string(res))
	})

	t.Run("ErrorJson", func(t *testing.T) {
		jsonTX := []byte(` "test2": "val2" }`)

		_, err := pg.GenEventPayload(jsonTX, config)
		assert.Errorf(t, err, "expected error for wrong json")
	})

	t.Run("ErrorOnType", func(t *testing.T) {
		getType := func(map[string]interface{}) ([]string, error) {
			fmt.Println("calling get type")
			return nil, fmt.Errorf("Test error")
		}

		pg := NewPayloadGenerator(tmpl, getType, ".gotpl")

		_, err := pg.GenEventPayload(jsonTX, config)
		assert.Errorf(t, err, "expected error for wrong transaction type")

	})

}
