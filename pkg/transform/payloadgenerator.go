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

// PayloadGenerator hold the templates and the mapping function
// between data and templates names
type PayloadGenerator struct {
	templates *template.Template
	tplExt    string
	getType   func(map[string]interface{}) ([]string, error)
}

// NewPayloadGenerator creates an instance of a PayloadGenerator with
// the available templates, a function that maps a input Json to a
// template name and the file extension used for the template files.
//
// The getType is a function that maps a input data object (Json
// structure transformed into a map) to a template name (extension not
// included). The output map is a list of names that the payload
// generator will try to map, in the provided order, to the available
// templates. For example, a JSON:
//   {"operation": {"sum": [1,2]} }
// can be mapped to ["sum", "operation"].
//
// The payload generator will look for the "sum" template, and if that
// one no exist in the "templates" parameter, it will try the
// "operation" template. If also the "operation" template is not
// available, a error will be raise
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

// GenEventPayload generates a payload using the predefined templates
// and mapping functions for the input raw data. With the input data,
// a configuration map can be defined to fill values for the
// transformation not available in the raw data (application
// configuration like API keys, host information, versions ...) The
// method returns the result of the transformation.
//
// If the template is not found for the input raw data a error will be
// raise. If one the parameters of the template is not possible to be
// filled using the input data (raw data and configuration map), an
// error will be raise. Conditional flows can be used for a
// non-mandatory data: https://golang.org/pkg/text/template/
//
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
