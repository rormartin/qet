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
	templates *template.Template
	tplExt    string
	getType   func(map[string]interface{}) ([]string, error)
}

func NewPayloadGenerator(
	templates *template.Template,
	getType func(map[string]interface{}) ([]string, error),
	templateExtension string) PayloadGenerator {

	tplExt := templateExtension
	if !strings.HasPrefix(templateExtension, ".") {
		tplExt = "." + tplExt
	}

	return PayloadGenerator{
		templates: templates,
		tplExt:    tplExt,
		getType:   getType,
	}
}

func (p PayloadGenerator) GenEventPayload(
	raw []byte,
	configuration map[string]interface{}) ([]byte, error) {

	return p.genEventPayload(raw, configuration, executeEventTemplate)
}

func (p PayloadGenerator) genEventPayload(
	raw []byte,
	configuration map[string]interface{},
	templateExecutor func(*template.Template, []string, map[string]interface{}, string) ([]byte, error)) ([]byte, error) {

	var content interface{}
	if err := json.Unmarshal(raw, &content); err != nil {
		return nil, fmt.Errorf("Error in transaction parsing: %v", err)
	}

	data := content.(map[string]interface{})
	rawType, err := p.getType(data)
	if err != nil {
		return nil, fmt.Errorf("Error to identify the transaction type: %v", err)
	}

	// always compatible types per definition, not possible error
	mergo.Merge(&data, configuration)

	return templateExecutor(p.templates, rawType, data, p.tplExt)
}

func executeEventTemplate(
	template *template.Template,
	eventNames []string,
	data map[string]interface{},
	templateExtension string) ([]byte, error) {

	var result bytes.Buffer

	for index, eventName := range eventNames {
		en := strings.ToLower(eventName)

		err := template.ExecuteTemplate(&result, en+templateExtension, data)
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
