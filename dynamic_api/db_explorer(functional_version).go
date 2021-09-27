package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

//	Conditions:
//	1. Global vars are prohibited
//	2. Implementation in one file

const (
	// endpoints
	rootPath     = "/"
	tableEndPath = "/$table"
	tablePath    = "/$table/"
	tableIdPath  = "/$table/$id"

	// sql
	showTables    = `SHOW TABLES`
	showTableSpec = `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, EXTRA, COLUMN_KEY, COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '%s';`

	qSelect = `SELECT `
	qInsert = `INSERT INTO `
	qUpdate = `UPDATE `
	qSet    = `SET `
	qValues = `VALUES `
	qFrom   = `FROM `
	qWhere  = `WHERE `
	qLimit  = `LIMIT `
	qOffset = `OFFSET `
	qEqual  = `= `
	qAll    = `* `
	qSpace  = ` `
	bt      = "`"

	tInt     = "INT"
	tVarchar = "VARCHAR"
	tText    = "TEXT"
	tString  = "STRING"
	tFloat64 = "FLOAT64"
	tNil     = "nil"

	// json
	rJsonOpen  = `{"response":{`
	rJsonClose = `}}`

	// err
	dbQueryErr     = "DB: Query() err: "
	dbRowsCloseErr = "DB: rows Close() err: "
	dbRowsScanErr  = "DB: rows Scan() err: "
	dbExecErr      = "DB: Exec() err: "
)

func NewDbExplorer(db *sql.DB) (h http.Handler, err error) {
	rootHandler := &DBExplorer{
		db:          db,
		errorLogger: log.New(os.Stderr, "ERROR: ", log.LstdFlags|log.Ldate|log.Ltime|log.Lshortfile),
	}
	// Possible to update cache etc.
	if err = rootHandler.initDBSchema(rootHandler.errorLogger); err != nil {
		rootHandler.errorLogger.Fatal("Impossible to build DBSchema")
	}

	errCheck := panicMiddleware(rootHandler)
	http.Handle("/", errCheck)

	return
}

func panicMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				if h, castOk := next.(*DBExplorer); castOk {
					h.errorLogger.Printf("Panic attack:  %s\n", err)
				}
				http.Error(w, "Internal server error", 500)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func getErrRespByType(w http.ResponseWriter, err error) (respErr string) {
	if _, jsonTypeOk := err.(*jsonResponseError); jsonTypeOk {
		respErr = err.Error()
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		respErr = "Internal server error"
	}

	return
}

func makeRoute(endpoints []string, rEntity string, rId string) (route string) {
	if len(endpoints) == 1 && rEntity == "" {
		route = rootPath
	} else if len(endpoints) == 2 && rEntity != "" && endpoints[1] == "" {
		route = tablePath
	} else {
		if len(endpoints) == 1 && rEntity != "" {
			route = tableEndPath
		} else if len(endpoints) == 2 && rEntity != "" && rId != "" {
			route = tableIdPath
		} else {

		}
	}

	return
}

type DBExplorer struct {
	db            *sql.DB
	cacheDBSchema map[string][]map[string]string
	errorLogger   *log.Logger
}

func (h *DBExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		resp         string
		err          error
		rEntity, rId string
	)

	rPath := strings.Split(r.URL.RequestURI(), "?")[0]
	endpoints := strings.Split(rPath, rootPath)[1:]

	if len(endpoints) == 1 {
		rEntity = endpoints[0]
	} else if len(endpoints) == 2 {
		rEntity, rId = endpoints[0], endpoints[1]
	}

	route := makeRoute(endpoints, rEntity, rId)

	switch r.Method {
	case http.MethodGet:
		switch route {
		case rootPath:
			resp, err = createResponseRoot(h.db, h.errorLogger)
			if err != nil {
				resp = getErrRespByType(w, err)
			}
		case tableEndPath, tableIdPath:
			resp, err = createResponseSelect(w, r, h, rEntity, rId)
			if err != nil {
				resp = getErrRespByType(w, err)
			}
		}
	case http.MethodPost:
		switch route {
		case tableIdPath:
			resp, err = createResponseUpdate(w, r, h, rEntity, rId)
			if err != nil {
				resp = getErrRespByType(w, err)
			}
		}
	case http.MethodPut:
		switch route {
		case tablePath:
			resp, err = createResponseInsert(w, r, h, rEntity)
			if err != nil {
				resp = getErrRespByType(w, err)
			}
		}
	case http.MethodDelete:
		switch route {
		case tableIdPath:
			resp, err = createResponseDelete(w, h, rEntity, rId)
			if err != nil {
				resp = getErrRespByType(w, err)
			}
		}
	default:
		resp = "Welcome!"
	}

	fmt.Fprintln(w, resp)
}

func (h *DBExplorer) initDBSchema(el *log.Logger) (err error) {
	var tNames []string
	h.cacheDBSchema = map[string][]map[string]string{}
	tables, err := h.db.Query(showTables)

	if nil != err {
		el.Println(dbQueryErr + err.Error())
		return
	}
	defer func() {
		err = tables.Close()
		if nil != err {
			el.Println(dbRowsCloseErr + err.Error())
		}
	}()

	for tables.Next() {
		var tableName sql.NullString
		if err = tables.Scan(&tableName); nil != err {
			el.Println(dbRowsScanErr + err.Error())
			continue
		}

		tNames = append(tNames, tableName.String)
	}

	for _, n := range tNames {
		func() {
			tSpec, err := h.db.Query(fmt.Sprintf(showTableSpec, n))
			if nil != err {
				el.Println(dbQueryErr + err.Error())
				return
			}
			defer func() {
				if err = tSpec.Close(); nil != err {
					el.Println(dbRowsCloseErr + err.Error())
				}
			}()

			for tSpec.Next() {
				var (
					fName      sql.NullString
					fType      sql.NullString
					nullable   sql.NullString
					extra      sql.NullString
					ck         sql.NullString
					defaultVal sql.NullString
				)

				if err = tSpec.Scan(
					&fName,
					&fType,
					&nullable,
					&extra,
					&ck,
					&defaultVal,
				); nil != err {
					continue
				}

				//  In case of "performance_schema_users_size" = 1
				if inSlice([]string{"USER", "CURRENT_CONNECTIONS", "TOTAL_CONNECTIONS"}, fName.String) {
					continue
				}

				m := map[string]string{
					"fName":    fName.String,
					"fType":    strings.ToUpper(fType.String),
					"nullable": nullable.String,
					"extra":    extra.String,
					"ck":       ck.String,
					"default":  defaultVal.String,
				}

				h.cacheDBSchema[n] = append(h.cacheDBSchema[n], m)
			}
		}()
	}

	return
}

func (h *DBExplorer) getPK(rEntity string) (PK string) {
	for _, e := range h.cacheDBSchema[rEntity] {
		if e["ck"] == "PRI" {
			PK = e["fName"]
			break
		}
	}

	return
}

type jsonResponseError struct {
	Body string
}

func (re *jsonResponseError) Error() string {
	return re.Body
}

// MAIN PAGE
func createResponseRoot(db *sql.DB, el *log.Logger) (string, error) {
	tables, err := db.Query(showTables)
	if nil != err {
		el.Println(dbQueryErr + err.Error())
		return "", err
	}
	defer func() {
		if err = tables.Close(); nil != err {
			el.Println(dbRowsCloseErr + err.Error())
		}
	}()

	b := strings.Builder{}
	b.WriteString(rJsonOpen + `"tables":[`)

	for tables.Next() {
		var tableName sql.NullString
		if err = tables.Scan(&tableName); nil != err {
			continue
		}

		b.WriteString(`"` + tableName.String + `",`)
	}

	return strings.TrimSuffix(b.String(), ",") + `]` + rJsonClose, err
}

// CREATE
func createResponseInsert(w http.ResponseWriter,
	r *http.Request,
	h *DBExplorer,
	rEntity string) (string, error) {
	qParams := map[string]interface{}{}
	if err := json.NewDecoder(r.Body).Decode(&qParams); err != nil {
		h.errorLogger.Println("JSON: Decode() err: " + err.Error())
	}

	if err := validateCreateParams(w, map[string]*string{"rEntity": &rEntity}, getMapKeys(h.cacheDBSchema)); err != nil {
		return "", err
	}

	PK := h.getPK(rEntity)
	q, args := buildQueryInsert(rEntity, PK, qParams, h.cacheDBSchema[rEntity])
	result, err := h.db.Exec(q, args...)
	if err != nil {
		h.errorLogger.Println(dbExecErr + err.Error())
	}
	lastId, err := result.LastInsertId()
	if err != nil {
		h.errorLogger.Println("DB: LastInsertId() err: " + err.Error())
	}

	return rJsonOpen + `"` + PK + `": ` + strconv.FormatInt(lastId, 10) + rJsonClose, nil
}

func validateCreateParams(w http.ResponseWriter, params map[string]*string, currTabs []string) error {
	for pKey, pVal := range params {
		if *pVal == "" {
			continue
		}
		if pKey == "rEntity" && !inSlice(currTabs, *pVal) {
			w.WriteHeader(http.StatusNotFound)
			return &jsonResponseError{Body: `{"error": "unknown table"}`}
		}
		// Possible to implement validation params by DbSchema...
	}

	return nil
}

func buildQueryInsert(rEntity string,
	PK string,
	params map[string]interface{},
	eSchema []map[string]string) (string, []interface{}) {
	b := strings.Builder{}
	b.WriteString(qInsert + bt + rEntity + bt + qSpace + "(")

	argLen := len(eSchema)
	for _, e := range eSchema {
		if e["fName"] == PK {
			argLen--
		}
	}
	args := make([]interface{}, argLen, argLen)

	i := 0
	for idx, fArgs := range eSchema {
		if fArgs["fName"] == PK {
			continue
		}
		b.WriteString(bt + fArgs["fName"] + bt)
		if idx != len(eSchema)-1 {
			b.WriteString(", ")
		}

		if _, ok := params[fArgs["fName"]]; ok {
			args[i] = params[fArgs["fName"]]
		} else if fArgs["default"] == "" && fArgs["nullable"] == "YES" {
			args[i] = nil
		} else if fArgs["default"] == "" && fArgs["nullable"] == "NO" {
			switch fArgs["fType"] {
			case tVarchar, tText:
				args[i] = ""
			case tInt, tFloat64:
				args[i] = 0
			}
		}
		i++
	}
	b.WriteString(") " + qValues + "(")
	for i != 0 {
		b.WriteString("?, ")
		i--
	}

	return strings.TrimSuffix(b.String(), ", ") + ")", args
}

// READ
func createResponseSelect(w http.ResponseWriter,
	r *http.Request,
	h *DBExplorer,
	rEntity string,
	rId string) (string, error) {

	params := r.URL.Query()
	l, o := params.Get("limit"), params.Get("offset")
	if err := validateReadParams(w, map[string]*string{
		"rEntity": &rEntity,
		"rId":     &rId,
		"limit":   &l,
		"offset":  &o},
		getMapKeys(h.cacheDBSchema)); err != nil {
		return "", err
	}

	q := buildQuerySelect(rEntity, rId, h.getPK(rEntity), l, o)
	rows, err := h.db.Query(q)
	if nil != err {
		h.errorLogger.Println(dbQueryErr + err.Error())
		return "", err
	}
	defer func() {
		if err = rows.Close(); nil != err {
			h.errorLogger.Println(dbRowsCloseErr + err.Error())
		}
	}()

	colTypes, err := rows.ColumnTypes()
	if nil != err {
		h.errorLogger.Println("DB: rows ColumnTypes() err: " + err.Error())
		return "", err
	}
	columns, err := rows.Columns()
	if nil != err {
		h.errorLogger.Println("DB: rows Columns() err: " + err.Error())
		return "", err
	}

	values := make([]sql.RawBytes, len(columns), len(columns))
	scanArgs := make([]interface{}, cap(values), cap(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	b := strings.Builder{}
	b.WriteString(rJsonOpen + func() string {
		if rId == "" {
			return `"records":[`
		} else {
			return `"record":`
		}
	}())

	rowsCount := 0
	for rows.Next() {
		b.WriteString(`{`)

		if err = rows.Scan(scanArgs...); err != nil {
			h.errorLogger.Println(dbRowsScanErr + err.Error())
			return "", err
		}

		var value string
		for i, col := range values {
			if col == nil {
				value = "null"
			} else {
				switch colTypes[i].DatabaseTypeName() {
				case tVarchar, tText:
					value = `"` + jsonStringEscape(string(col)) + `"`
				case tInt:
					value = string(col)
				}
			}

			b.WriteString(`"` + columns[i] + `": ` + value)

			if i != len(values)-1 {
				b.WriteString(`,`)
			}
		}
		b.WriteString(`},`)
		rowsCount++
	}

	if err = rows.Err(); err != nil {
		h.errorLogger.Println("DB: rows err: " + err.Error())
		return "", err
	}
	if rId != "" && rowsCount == 0 {
		w.WriteHeader(http.StatusNotFound)
		return "", &jsonResponseError{Body: `{"error": "record not found"}`}
	}

	return strings.TrimSuffix(b.String(), ",") +
		func() string {
			if rId == "" {
				return `]`
			} else {
				return ``
			}
		}() +
		rJsonClose, err
}

func validateReadParams(w http.ResponseWriter, params map[string]*string, currTabs []string) error {
	for pKey, pVal := range params {
		if *pVal == "" {
			continue
		}
		if pKey == "rEntity" && !inSlice(currTabs, *pVal) {
			w.WriteHeader(http.StatusNotFound)
			return &jsonResponseError{Body: `{"error": "unknown table"}`}
		}

		if _, err := strconv.Atoi(*pVal); err != nil {
			switch pKey {
			case "rId":
				*pVal = "1"
			case "limit":
				*pVal = "5"
			case "offset":
				*pVal = "0"
			}
			continue
		}
	}

	return nil
}

func buildQuerySelect(rEntity string, rId string, PK string, limit string, offset string) string {
	b := strings.Builder{}

	b.WriteString(qSelect + qAll + qFrom + bt + rEntity + bt + qSpace)
	if rId != "" {
		b.WriteString(qWhere + bt + PK + bt + qSpace + qEqual + rId + qSpace)
	}
	if limit != "" {
		b.WriteString(qLimit + limit + qSpace)
	}
	if offset != "" {
		b.WriteString(qOffset + offset + qSpace)
	}

	return b.String()
}

// UPDATE
func createResponseUpdate(w http.ResponseWriter,
	r *http.Request,
	h *DBExplorer,
	rEntity string,
	rId string) (string, error) {
	qParams := map[string]interface{}{}
	if err := json.NewDecoder(r.Body).Decode(&qParams); err != nil {
		h.errorLogger.Println("JSON: Decode() err: " + err.Error())
	}

	if err := validateUpdateParams(w, h.cacheDBSchema, rEntity, qParams); err != nil {
		return "", err
	}

	q, args := buildQueryUpdate(rEntity, rId, h.getPK(rEntity), qParams)
	result, err := h.db.Exec(q, args...)
	if err != nil {
		h.errorLogger.Println(dbExecErr + err.Error())
	}
	affected, err := result.RowsAffected()
	if err != nil {
		h.errorLogger.Println("DB: RowsAffected() err: " + err.Error())
	}

	return rJsonOpen + `"updated": ` + strconv.FormatInt(affected, 10) + rJsonClose, nil
}

func validateUpdateParams(w http.ResponseWriter,
	cacheDBSchema map[string][]map[string]string,
	rEntity string,
	params map[string]interface{}) (err error) {
	if !inSlice(getMapKeys(cacheDBSchema), rEntity) {
		w.WriteHeader(http.StatusNotFound)
		return &jsonResponseError{Body: `{"error": "unknown table"}`}
	}

mainLoop:
	for pKey, pVal := range params {
		var argType string

		switch pVal.(type) {
		case string:
			argType = tString
		case float64:
			argType = tFloat64
		case int:
			argType = tInt
		case nil:
			argType = tNil
		default:
			argType = "undefined type"
		}

		for _, e := range cacheDBSchema[rEntity] {
			if e["fName"] == pKey {
				if argType == tString && (e["fType"] == tVarchar || e["fType"] == tText) {
					continue
				}
				if argType == tNil && e["nullable"] == "YES" {
					continue
				}

				if e["fType"] != argType || e["extra"] == "auto_increment" {
					w.WriteHeader(http.StatusBadRequest)
					err = &jsonResponseError{Body: `{"error": "field ` + e["fName"] + ` have invalid type"}`}
					break mainLoop
				}
			}
		}
	}

	return
}

func buildQueryUpdate(rEntity string, rId string, PK string, params map[string]interface{}) (string, []interface{}) {
	b := strings.Builder{}
	b.WriteString(qUpdate + bt + rEntity + bt + qSpace + qSet)

	args := make([]interface{}, len(params), len(params))
	i := 0
	for pName, pVal := range params {
		b.WriteString(bt + pName + bt + qEqual + "?")
		if i != len(params)-1 {
			b.WriteString(", ")
		}
		args[i] = pVal
		i++
	}

	return b.String() + qSpace + qWhere + bt + PK + bt + qSpace + qEqual + rId, args
}

// DELETE
func createResponseDelete(w http.ResponseWriter,
	h *DBExplorer,
	rEntity string,
	rId string) (string, error) {

	if err := validateDeleteParams(w,
		map[string]*string{"rEntity": &rEntity, "rId": &rId},
		getMapKeys(h.cacheDBSchema)); err != nil {
		return "", err
	}

	result, err := h.db.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE id = ?", rEntity), rId)
	if err != nil {
		h.errorLogger.Println(dbExecErr + err.Error())
		return "", err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		h.errorLogger.Println("DB: RowsAffected() err: " + err.Error())
		return "", err
	}

	return rJsonOpen + `"deleted": ` + strconv.FormatInt(affected, 10) + rJsonClose, nil
}

func validateDeleteParams(w http.ResponseWriter, params map[string]*string, currTabs []string) error {
	for pKey, pVal := range params {
		if *pVal == "" {
			continue
		}
		if pKey == "rEntity" && !inSlice(currTabs, *pVal) {
			w.WriteHeader(http.StatusNotFound)
			return &jsonResponseError{Body: `{"error": "unknown table"}`}
		}

		if _, err := strconv.Atoi(*pVal); err != nil {
			switch pKey {
			case "rId":
				*pVal = "1"
			}
			continue
		}

	}

	return nil
}

// utils
func inSlice(h []string, n string) bool {
	for _, t := range h {
		if t == n {
			return true
		}
	}
	return false
}

func getMapKeys(m interface{}) (keys []string) {
	switch t := m.(type) {
	case map[string][]map[string]string:
		keys = make([]string, len(t), len(t))
		i := 0
		for k := range t {
			keys[i] = k
			i++
		}
	}

	return keys
}

func jsonStringEscape(value string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch c {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}

	return b.String()
}
