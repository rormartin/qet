package transform // import "github.com/icemobilelab/qet/pkg/transform"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/imdario/mergo"
	"strings"
	"text/template"
)

type PayloadGenerator struct {
	Templates *template.Template
	GetType   func(map[string]interface{}) ([]string, error)
}

func (p PayloadGenerator) GenEventPayload(
	raw []byte,
	configuration map[string]interface{}) ([]byte, error) {

	return p.genEventPayload(raw, configuration, executeEventTemplate)
}

func (p PayloadGenerator) genEventPayload(
	raw []byte,
	configuration map[string]interface{},
	templateExecutor func(*template.Template, []string, map[string]interface{}) ([]byte, error)) ([]byte, error) {

	var content interface{}
	if err := json.Unmarshal(raw, &content); err != nil {
		return nil, fmt.Errorf("Error in transaction parsing: %v", err)
	}

	data := content.(map[string]interface{})
	rawType, err := p.GetType(data)
	if err != nil {
		return nil, fmt.Errorf("Error to identify the transaction type: %v", err)
	}

	// always compatible types per definition, not possible error
	mergo.Merge(&data, configuration)

	return templateExecutor(p.Templates, rawType, data)
}

func executeEventTemplate(template *template.Template, eventNames []string, data map[string]interface{}) ([]byte, error) {

	const tplExtension = ".tmpl"

	var result bytes.Buffer

	for index, eventName := range eventNames {
		en := strings.ToLower(eventName)

		err := template.ExecuteTemplate(&result, en+tplExtension, data)
		if err == nil {
			break // keep the first one that is working
		}
		if err != nil && index == (len(eventNames)-1) {
			return nil, fmt.Errorf("Error on template execution: %v", err)
		}
	}

	bytesResult := result.Bytes()

	// Constant from https://golang.org/src/text/template/exec.go - line 914
	noValue := []byte("<no value>")
	if bytes.Contains(bytesResult, noValue) {
		return nil, fmt.Errorf("Missing value in data for the template")
	}

	return bytesResult, nil
}
