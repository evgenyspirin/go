package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	// validation params
	requiredStr  = "required"
	minStr       = "min="
	maxStr       = "max="
	paramnameStr = "paramname="
	enumStr      = "enum="
	defaultStr   = "default="

	nltt = "\n\t\t"
	nl   = "\n"
)

var (
	importGen = `
import (
	"fmt"
	"net/http"
	"strconv"
)
`
	checkPost = `if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotAcceptable)
			fmt.Fprintln(w, ` + "`" + `{"error": "bad method"}` + "`" + `)
			return
		}`
	checkAuth = ` else if r.Header.Get("X-Auth") != "100500" {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintln(w, ` + "`" + `{"error": "unauthorized"}` + "`" + `)
			return
		}`
	serveHttpDefault = `
	default:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, ` + "`" + `{"error": "unknown method"}` + "`" + `)
	}
}
`
)

func main() {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, os.Args[1], nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	fout, err := os.Create(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintln(fout, `package `+node.Name.Name)

	strctGenMthds := map[string][]string{}
	paramsFields := map[string]map[string]string{}

	ast.Inspect(node, func(n ast.Node) bool {
		switch t := n.(type) {
		case *ast.FuncDecl:
			currFun := *t

			// no comments
			if currFun.Doc == nil {
				return true
			}

			currStructName := ""
			if currFun.Recv != nil {
				for _, v := range currFun.Recv.List {
					m, ok := v.Type.(*ast.StarExpr)
					// not a method
					if !ok {
						continue
					}
					if s, ok := m.X.(*ast.Ident); ok {
						currStructName = s.Name
					}
				}
			}

			needCodegen := false
			for _, comment := range currFun.Doc.List {
				needCodegen = needCodegen || strings.HasPrefix(comment.Text, "// apigen:api")
			}
			if !needCodegen {
				return true
			}

			strctGenMthds[currStructName] = append(strctGenMthds[currStructName], currFun.Name.Name)
		case *ast.TypeSpec:
			typeSpec := *t

			switch typeSpec.Type.(type) {
			case *ast.StructType:
				// "apivalidator:" - медленнее
				if strings.Contains(typeSpec.Name.Name, "Params") {
					structType := typeSpec.Type.(*ast.StructType)
					paramsParsed := map[string]string{}
					for _, field := range structType.Fields.List {
						paramsParsed[field.Names[0].Name] = strings.TrimSuffix(
							strings.Replace(field.Tag.Value, "`apivalidator:\"", types.ExprString(field.Type)+",", -1), "\"`")

					}
					paramsFields[typeSpec.Name.Name] = paramsParsed
				}
			}
		}

		return true
	})

	generateTpls(fout, strctGenMthds, paramsFields)
}

func generateTpls(fout *os.File, strctGenMthds map[string][]string, paramsFields map[string]map[string]string) {
	fmt.Fprintln(fout, importGen)

	for strct, mthds := range strctGenMthds {
		serveHttpStrTpl := strings.Builder{}
		handlerTpl := strings.Builder{}
		validationTpl := strings.Builder{}

		serveHttpStrTpl.WriteString(`func (h *` + strct + `) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {`)

		for _, m := range mthds {
			// Экстраполируя, видимо автор предполагал именно такой исход.
			// Иной логики, в контексте динамики, я тут не вижу.
			strctParams := m + "Params"
			if strct != "MyApi" {
				strctParams = strings.TrimSuffix(strct, "Api") + strctParams
			}

			genServeHttpCase(&serveHttpStrTpl, strct, m)
			genHandler(&handlerTpl, strct, m, strctParams, paramsFields)
			genValidation(&validationTpl, strct, m, strctParams, paramsFields)
		}

		serveHttpStrTpl.WriteString(serveHttpDefault)
		fmt.Fprintln(fout, serveHttpStrTpl.String())
		fmt.Fprint(fout, handlerTpl.String())
		fmt.Fprint(fout, validationTpl.String())
	}
}

func genServeHttpCase(b *strings.Builder, strct string, method string) {
	b.WriteString(`
	case "/user/` + strings.ToLower(method) + `":` + nltt)
	if method == "Create" {
		b.WriteString(checkPost)
		b.WriteString(checkAuth + nltt)
		b.WriteString(`h.` + strings.ToLower(strct) + method + `Handler(w, r)`)
	} else {
		b.WriteString(`h.` + strings.ToLower(strct) + method + `Handler(w, r)`)
	}
}

func genHandler(b *strings.Builder, strct string, method string, strcParams string, paramsFields map[string]map[string]string) {
	b.WriteString(`func (h *` + strct + `) ` + strings.ToLower(strct) + method + `Handler(w http.ResponseWriter, r *http.Request) {` + nl)
	b.WriteString(`	params := new(` + strcParams + `)` + nl)

	for fld, prms := range paramsFields[strcParams] {
		args := strings.Split(prms, ",")
		switch args[0] {
		case "string":
			var paramname string
			for _, arg := range args[1:] {
				if strings.Contains(arg, paramnameStr) {
					fmt.Sscanf(arg, "paramname=%s", &paramname)
					break
				}
			}

			if paramname != "" {
				b.WriteString(`	` + strings.ToLower(fld) + ` := r.FormValue("` + strings.ToLower(fld) + `")
	if ` + strings.ToLower(fld) + ` == "" {
		` + strings.ToLower(fld) + ` = r.FormValue("` + paramname + `")
	}
	params.Name = name` + nl)
			} else {
				b.WriteString(`	params.` + fld + ` = r.FormValue("` + strings.ToLower(fld) + `")` + nl)
			}
		case "int":
			b.WriteString(`	int` + fld + `, err := strconv.Atoi(r.FormValue("` + strings.ToLower(fld) + `"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, ` + "`" + `{"error": "` + strings.ToLower(fld) + ` must be int"}` + "`" + `)
		return
	}
	params.` + fld + ` = int` + fld + `` + nl)
		}
	}

	b.WriteString(`
	if err :=` + `validate` + strct + method + `(params); err != nil {
		apiErr := err.(ApiError)
		w.WriteHeader(apiErr.HTTPStatus)
		fmt.Fprintf(w, ` + "`{\"error\": \"%s\"}`" + `, apiErr.Error())

		return
	}

`)
	b.WriteString(`	usr, err := h.` + method + `(r.Context(), *params)
	if err != nil {
		switch err.(type) {
		case ApiError:
			apiErr := err.(ApiError)
			w.WriteHeader(apiErr.HTTPStatus)
			fmt.Fprintf(w, ` + "`{\"error\": \"%s\"}`" + `, apiErr.Error())
		case error:
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, ` + "`{\"error\": \"%s\"}`" + `, err.Error())
		default:
			// ...
		}

		return
	}

`)

	// response
	switch strct {
	case "MyApi":
		if method == "Profile" {
			b.WriteString(`	fmt.Fprintln(w, ` + "`" + `{"error":"","response":{"id":` + "`" + `+strconv.FormatUint(usr.ID, 10)+` + nltt +
				"`" + `,"login":"` + "`" + `+usr.Login+` + nltt +
				"`\"" + `,"full_name":"` + "`" + `+usr.FullName+` + nltt +
				"`\"" + `,"status":` + "`" + `+strconv.Itoa(usr.Status)+` + "`" + `}}` + "`" + `)`)
		} else if method == "Create" {
			b.WriteString(`	fmt.Fprintln(w, ` + "`" + `{"error":"","response":{"id":` + "`" + `+strconv.FormatUint(usr.ID, 10)+` + "`" + `}}` + "`" + `)`)
		}
	case "OtherApi":
		b.WriteString(`	fmt.Fprintln(w, ` + "`" + `{"error":"","response":{"full_name":"` + "`" + `+usr.FullName+` + nltt +
			"`" + `","id":` + "`" + `+strconv.FormatUint(usr.ID, 10)+` + nltt +
			"`" + `,"level":` + "`" + `+strconv.Itoa(usr.Level)+` + nltt +
			"`" + `,"login":"` + "`" + `+usr.Login+` + "`" + `"}}` + "`" + `)`)
	}

	b.WriteString("\n}\n\n")
}

func genValidation(b *strings.Builder, strct string, method string, strcParams string, paramsFields map[string]map[string]string) {
	b.WriteString(`func ` + `validate` + strct + method + `(params *` + strcParams + `) error {
	for idxKey, param := range []interface{}{` + nltt)

	paramStr := strings.Builder{}
	caseStr := strings.Builder{}
	i := 0
	for fld, prms := range paramsFields[strcParams] {
		paramStr.WriteString(`params.` + fld)
		if i != len(paramsFields[strcParams])-1 {
			paramStr.WriteString(`,` + nltt)
		} else {
			paramStr.WriteString(`} {
		switch idxKey {` + nltt)
		}
		caseStr.WriteString(`case ` + strconv.Itoa(i) + `:` + "\n\t\t\t" +
			genFieldValidation(fld, prms) + nltt)
		i++
	}

	b.WriteString(paramStr.String() + caseStr.String())
	b.WriteString(`}
	}

	return nil
}
`)
	b.WriteString("\n")
}

func genFieldValidation(fieldName string, validationParams string) string {
	args := strings.Split(validationParams, ",")
	b := strings.Builder{}

	switch args[0] {
	case "string":
		b.WriteString(`paramString, ok := param.(string)
			if !ok {
				// 500
			}` + nl)
		for k, arg := range args[1:] {
			if strings.Contains(arg, requiredStr) {
				b.WriteString(`			if paramString == "" {
				return ApiError{http.StatusBadRequest, fmt.Errorf("` + strings.ToLower(fieldName) +
					` must me not empty")}
			}`)
			}
			if strings.Contains(arg, minStr) {
				var min int
				fmt.Sscanf(arg, "min=%d", &min)
				b.WriteString(`			if len(paramString) < ` + strconv.Itoa(min) + ` {
				return ApiError{http.StatusBadRequest, fmt.Errorf("` + strings.ToLower(fieldName) +
					` len must be >= ` + strconv.Itoa(min) + `")}
			}`)
			}
			if strings.Contains(arg, maxStr) {
				var max int
				fmt.Sscanf(arg, "max=%d", &max)
				b.WriteString(`			if len(paramString) > ` + strconv.Itoa(max) + ` {
				return ApiError{http.StatusBadRequest, fmt.Errorf("` + strings.ToLower(fieldName) +
					` len must be <= ` + strconv.Itoa(max) + `")}
			}`)
			}
			if strings.Contains(arg, paramnameStr) {
				// some new logic
				return ""
			}
			if strings.Contains(arg, enumStr) {
				bOpt := strings.Builder{}
				options := strings.Split(strings.TrimPrefix(arg, enumStr), "|")
				for k, opt := range options {
					if k != len(options)-1 {
						bOpt.WriteString("\"" + opt + "\"" + ", ")
					} else {
						bOpt.WriteString("\"" + opt + "\"")
					}
				}

				b.WriteString(`			options := [` + strconv.Itoa(len(options)) + `]string{` + bOpt.String() + `}
			if param != "" && !func(opt [` + strconv.Itoa(len(options)) + `]string) bool {
				for _, p := range opt {
					if p == paramString {
						return true
					}
				}
				return false
			}(options) {
				return ApiError{http.StatusBadRequest, fmt.Errorf("` +
					strings.ToLower(fieldName) + ` must be one of [` +
					strings.Replace(bOpt.String(), `"`, ``, -1) + `]")}
			}`)
			}
			if k != len(args[1:])-1 {
				b.WriteString(nl)
			}
		}
	case "int":
		b.WriteString(`paramInt, ok := param.(int)
			if !ok {
				// 500
			}` + nl)
		for k, arg := range args[1:] {
			if strings.Contains(arg, minStr) {
				var min int
				fmt.Sscanf(arg, "min=%d", &min)
				b.WriteString(`			if paramInt < ` + strconv.Itoa(min) + ` {
				return ApiError{http.StatusBadRequest, fmt.Errorf("` + strings.ToLower(fieldName) + ` must be >= ` + strconv.Itoa(min) + `")}
			}`)
			}
			if strings.Contains(arg, maxStr) {
				var max int
				fmt.Sscanf(arg, "max=%d", &max)
				b.WriteString(`			if paramInt > ` + strconv.Itoa(max) + ` {
				return ApiError{http.StatusBadRequest, fmt.Errorf("` + strings.ToLower(fieldName) + ` must be <= ` + strconv.Itoa(max) + `")}
			}`)
			}
			if k != len(args[1:])-1 {
				b.WriteString(nl)
			}
		}
	}

	return b.String()
}
