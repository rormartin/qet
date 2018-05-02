package transform

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"text/template"
)

const testTemplate = "test {{.test1}} for unit test {{.test2}}"

func TestEventTemplate(t *testing.T) {
	data := map[string]interface{}{
		"test1": "val1",
		"test2": "val2",
	}

	tmpl := template.Must(template.New("test.tmpl").Parse(testTemplate))

	t.Run("Ok", func(t *testing.T) {
		res, err := executeEventTemplate(tmpl, []string{"test"}, data)
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
		_, err := executeEventTemplate(tmpl, []string{"test"}, data)
		assert.Errorf(t, err, "expected error for missing value")
	})

	t.Run("MissingAllTemplate", func(t *testing.T) {
		_, err := executeEventTemplate(tmpl, []string{"error"}, data)
		assert.Errorf(t, err, "expected error for missing template")
	})

	t.Run("MissingOneTemplate(and success)", func(t *testing.T) {
		res, err := executeEventTemplate(tmpl, []string{"error", "test"}, data)
		if err != nil {
			t.Errorf("Error in event execution not expected: %v", err)
		}

		const testResult = "test val1 for unit test val2"
		assert.Equal(t, testResult, string(res))
	})

}

func TestGenEventPayload(t *testing.T) {

	tmpl := template.Must(template.New("test.tmpl").Parse(testTemplate))
	jsonTX := []byte(`{ "test2": "val2" }`)
	config := map[string]interface{}{
		"test1": "val1",
	}

	getType := func(map[string]interface{}) ([]string, error) {
		return []string{"test"}, nil
	}

	pg := PayloadGenerator{
		Templates: tmpl,
		GetType:   getType,
	}

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

		pg := PayloadGenerator{
			Templates: tmpl,
			GetType:   getType,
		}

		_, err := pg.GenEventPayload(jsonTX, config)
		assert.Errorf(t, err, "expected error for wrong transaction type")

	})

}
