package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	decodeError = "decode error: "
	token       = "12345"
)

var globalCase int

type TestCaseError struct {
	TestRequest SearchRequest
	ResultError string
}

type TestCase struct {
	TestRequest SearchRequest
	Result      *SearchResponse
}

func MockSearchServerRequestHandler(w http.ResponseWriter, r *http.Request) {
	searchResult, err := handleSearchRequest(r)

	switch {
	case err != nil && err.Error == "request_timeout":
		time.Sleep(time.Nanosecond * 2)
		w.WriteHeader(http.StatusRequestTimeout)
		io.WriteString(w, `{"status": 408, "error": "Request Timeout"}`)
	case err != nil && err.Error == "token_error":
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"status": 401, "error": "Unauthorized"}`)
	case err != nil && err.Error == "internal_server_error":
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"status": 500, "error": "Internal Server Error"}`)
	case err != nil && err.Error == "case_error_6":
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"status": 400, "error": invalid json struct}`)
	case err != nil && err.Error == "wrong_order_field":
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"status": 400, "error": "ErrorBadOrderField"}`)
	case err != nil && err.Error == "case_error_8":
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"error": "Unknown Bad Request"}`)
	case err != nil && err.Error == "case_error_9":
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"id": user_json_invalid}`)
	case searchResult != nil && err == nil:
		xmlData, _ := json.Marshal(searchResult.Users)
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, string(xmlData))
	}
}

func handleSearchRequest(r *http.Request) (sr *SearchResponse, error *SearchErrorResponse) {
	tokenHeader := r.Header.Get("AccessToken")
	limit, _ := strconv.Atoi(r.FormValue("limit"))
	if limit == 1 {
		limit = 0
	}
	offset, _ := strconv.Atoi(r.FormValue("offset"))
	query := r.FormValue("query")
	orderField := r.FormValue("order_field")
	orderBy, _ := strconv.Atoi(r.FormValue("order_by"))
	error = new(SearchErrorResponse)

	if tokenHeader != token {
		error.Error = "token_error"
	} else if !(orderBy == OrderByAsc || orderBy == OrderByAsIs || orderBy == OrderByDesc) {
		error.Error = "internal_server_error"
	} else if offset == 1000000000 {
		error.Error = "request_timeout"
	} else if !(orderField == "id" || orderField == "age" || orderField == "name" || orderField == "") {
		error.Error = "wrong_order_field"
	} else if globalCase == 6 {
		error.Error = "case_error_6"
	} else if globalCase == 8 {
		error.Error = "case_error_8"
	} else if globalCase == 9 {
		error.Error = "case_error_9"
	}

	if len(error.Error) > 0 {
		return
	}

	xmlFile, err := os.Open("./dataset.xml")
	if err != nil {
		panic(err)
	}
	defer xmlFile.Close()

	decoder := xml.NewDecoder(xmlFile)
	sr = new(SearchResponse)
	user := User{}

XmlLoop:
	for {
		tok, tokenErr := decoder.Token()
		if tokenErr != nil && tokenErr != io.EOF {
			fmt.Println("error: ", tokenErr)
			break
		} else if tokenErr == io.EOF || tok == nil {
			break
		}

		switch tok := tok.(type) {
		case xml.StartElement:
			if tok.Name.Local == "id" {
				var userId int
				if err := decoder.DecodeElement(&userId, &tok); err != nil {
					fmt.Println(decodeError, err)
				}
				user.Id = userId
			}
			if tok.Name.Local == "first_name" {
				var fName string
				if err := decoder.DecodeElement(&fName, &tok); err != nil {
					fmt.Println(decodeError, err)
				}
				user.Name = fName
			}
			if tok.Name.Local == "last_name" {
				var lName string
				if err := decoder.DecodeElement(&lName, &tok); err != nil {
					fmt.Println(decodeError, err)
				}
				user.Name += " " + lName
			}
			if tok.Name.Local == "age" {
				var age int
				if err := decoder.DecodeElement(&age, &tok); err != nil {
					fmt.Println(decodeError, err)
				}
				user.Age = age
			}
			if tok.Name.Local == "gender" {
				var gender string
				if err := decoder.DecodeElement(&gender, &tok); err != nil {
					fmt.Println(decodeError, err)
				}
				user.Gender = gender
			}
			if tok.Name.Local == "about" {
				var about string
				if err := decoder.DecodeElement(&about, &tok); err != nil {
					fmt.Println(decodeError, err)
				}
				user.About = about
			}
		case xml.EndElement:
			if tok.Name.Local == "row" {
				if query != "" && (strings.Contains(user.Name, query) || strings.Contains(user.About, query)) {
					sr.Users = append(sr.Users, user)
					user = User{}
				} else if query == "" {
					sr.Users = append(sr.Users, user)
					user = User{}
				}
				if len(sr.Users) == limit {
					break XmlLoop
				}
			}
		}
	}

	switch orderField {
	case "id":
		sort.SliceStable(sr.Users, func(i, j int) bool {
			if orderBy == OrderByDesc {
				return sr.Users[i].Id < sr.Users[j].Id
			} else if orderBy == OrderByAsc {
				return sr.Users[i].Id > sr.Users[j].Id
			} else {
				return sr.Users[i].Id == sr.Users[j].Id
			}
		})
	case "age":
		sort.SliceStable(sr.Users, func(i, j int) bool {
			if orderBy == OrderByDesc {
				return sr.Users[i].Age < sr.Users[j].Age
			} else if orderBy == OrderByAsc {
				return sr.Users[i].Age > sr.Users[j].Age
			} else {
				return sr.Users[i].Age == sr.Users[j].Age
			}
		})
	default:
		sort.SliceStable(sr.Users, func(i, j int) bool {
			if orderBy == OrderByDesc {
				return sr.Users[i].Name < sr.Users[j].Name
			} else if orderBy == OrderByAsc {
				return sr.Users[i].Name > sr.Users[j].Name
			} else {
				return sr.Users[i].Name == sr.Users[j].Name
			}
		})
	}

	if offset != 0 && offset < len(sr.Users) {
		sr.Users = sr.Users[offset:]
	}

	if error.Error == "" {
		error = nil
	}

	return
}

func TestSearchClient_FindUsersLimit(t *testing.T) {
	testCaseLimit := TestCase{
		TestRequest: SearchRequest{Limit: 2,},
		Result:      &SearchResponse{},
	}

	ts := httptest.NewServer(http.HandlerFunc(MockSearchServerRequestHandler))
	mockUrl := ts.URL

	sc := &SearchClient{AccessToken: token, URL: mockUrl}
	result, _ := sc.FindUsers(testCaseLimit.TestRequest)

	if result != nil && len(result.Users) != testCaseLimit.TestRequest.Limit {
		t.Errorf("unexpected error: %v", result)
	}
}

func TestSearchClient_FindUserByQueryNoLimit(t *testing.T) {
	testCaseLimit := TestCase{
		TestRequest: SearchRequest{Query: "Everett Dillard"},
		Result:      &SearchResponse{},
	}

	ts := httptest.NewServer(http.HandlerFunc(MockSearchServerRequestHandler))
	mockUrl := ts.URL

	sc := &SearchClient{AccessToken: token, URL: mockUrl}
	result, _ := sc.FindUsers(testCaseLimit.TestRequest)

	if result == nil || len(result.Users) != 0 {
		t.Errorf("unexpected error: %v", result)
	}
}

func TestSearchClient_FindOneUser(t *testing.T) {
	testCaseLimit := TestCase{
		TestRequest: SearchRequest{Limit: 1, Query: "Everett Dillard"},
		Result: &SearchResponse{Users: []User{
			{
				Id: 3,
			},
		}},
	}

	ts := httptest.NewServer(http.HandlerFunc(MockSearchServerRequestHandler))
	mockUrl := ts.URL

	sc := &SearchClient{AccessToken: token, URL: mockUrl}
	result, _ := sc.FindUsers(testCaseLimit.TestRequest)

	if result == nil || len(result.Users) > 1 || result.Users[0].Id != testCaseLimit.Result.Users[0].Id {
		t.Errorf("unexpected error: %v", result)
	}
}

func TestSearchClient_FindUsersErrors(t *testing.T) {
	testCasesError := []TestCaseError{
		{
			TestRequest: SearchRequest{Limit: -1,},
			ResultError: "limit must be > 0",
		},
		{
			TestRequest: SearchRequest{Offset: -1,},
			ResultError: "offset must be > 0",
		},
		{
			TestRequest: SearchRequest{Limit: 26,},
			ResultError:
			"unknown error Get ?limit=26&offset=0&order_by=0&order_field=&query=: unsupported protocol scheme \"\"",
		},
		{
			TestRequest: SearchRequest{Offset: 1000000000},
			ResultError:
			"timeout for limit=1&offset=1000000000&order_by=0&order_field=&query=",
		},
		{
			TestRequest: SearchRequest{},
			ResultError: "Bad AccessToken",
		},
		{
			TestRequest: SearchRequest{OrderField: "gender"},
			ResultError: "OrderFeld gender invalid",
		},
		{
			TestRequest: SearchRequest{},
			ResultError: "cant unpack error json: invalid character 'i' looking for beginning of value",
		},
		{
			TestRequest: SearchRequest{OrderBy: 2},
			ResultError: "SearchServer fatal error",
		},
		{
			TestRequest: SearchRequest{},
			ResultError: "unknown bad request error: Unknown Bad Request",
		},
		{
			TestRequest: SearchRequest{},
			ResultError: "cant unpack result json: invalid character 'u' looking for beginning of value",
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(MockSearchServerRequestHandler))

	for caseNum, item := range testCasesError {
		globalCase = caseNum
		mockUrl := ts.URL
		token := token
		client.Timeout = time.Second

		switch caseNum {
		case 2:
			mockUrl = ""
		case 3:
			client.Timeout = time.Nanosecond
		case 4:
			token = ""
		}

		sc := &SearchClient{AccessToken: token, URL: mockUrl}
		_, err := sc.FindUsers(item.TestRequest)

		if err == nil || err.Error() != item.ResultError {
			t.Errorf("[%d] unexpected error: %v", caseNum, err)
		}
	}
}
