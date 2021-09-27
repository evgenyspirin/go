package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/mailru/easyjson"
	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func FastSearch(out io.Writer) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	r := regexp.MustCompile("@")
	seenBrowsers := []string{}
	foundUsersBuilder := strings.Builder{}
	excludeFields := []string{"company", "country", "job", "phone"}

	var bytesPool = sync.Pool{
		New: func() interface{} {
			return []byte{}
		},
	}

	line := -1
	for scanner.Scan() {
		line++

		reuseByte := bytesPool.Get().([]byte)
		reuseByte = append(reuseByte, scanner.Text()...)
		user := new(User)

		err := user.UnmarshalJSON(reuseByte, excludeFields)

		if err != nil {
			panic(err)
		}

		reuseByte = reuseByte[:0]
		bytesPool.Put(reuseByte)

		isAndroid := false
		isMSIE := false

		for _, browserRaw := range user.Browsers {
			android := strings.Contains(browserRaw, "Android")
			MSIE := strings.Contains(browserRaw, "MSIE")

			notSeenBefore := true
			if android || MSIE {
				if android {
					isAndroid = true
				} else if MSIE {
					isMSIE = true
				}

				for _, item := range seenBrowsers {
					if item == browserRaw {
						notSeenBefore = false
					}
				}

				if notSeenBefore {
					// log.Printf("SLOW New browser: %s, first seen: %s", browser, user["name"])
					seenBrowsers = append(seenBrowsers, browserRaw)
				}
			}
		}

		if !(isAndroid && isMSIE) {
			continue
		}

		email := r.ReplaceAllString(user.Email, " [at] ")
		prepareStr := "[" + strconv.Itoa(line) + "] " + user.Name + " <" + email + ">" + "\n"
		foundUsersBuilder.WriteString(prepareStr)
	}
	if err := scanner.Err(); err != nil {
		//log.Fatal(err)
	}

	fmt.Fprintln(out, "found users:\n"+foundUsersBuilder.String())
	fmt.Fprintln(out, "Total unique browsers", len(seenBrowsers))
}

//easyjson:json
type User struct {
	Browsers []string `json:"browsers"`
	Company  string   `json:"company"`
	Country  string   `json:"country"`
	Email    string   `json:"email"`
	Job      string   `json:"job"`
	Name     string   `json:"name"`
	Phone    string   `json:"phone"`
}

// suppress unused package warning
var (
	_ *json.RawMessage
	_ *jlexer.Lexer
	_ *jwriter.Writer
	_ easyjson.Marshaler
)

func easyjson9e1087fdDecodeSandboxHw3BenchData(in *jlexer.Lexer, out *User, excludeFields []string) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		skip := false
		for _, item := range excludeFields {
			if item == key {
				skip = true
			}
		}
		in.WantColon()
		if in.IsNull() || skip {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "browsers":
			if in.IsNull() {
				in.Skip()
				out.Browsers = nil
			} else {
				in.Delim('[')
				if out.Browsers == nil {
					if !in.IsDelim(']') {
						out.Browsers = make([]string, 0, 4)
					} else {
						out.Browsers = []string{}
					}
				} else {
					out.Browsers = (out.Browsers)[:0]
				}
				for !in.IsDelim(']') {
					var v1 string
					v1 = string(in.String())
					out.Browsers = append(out.Browsers, v1)
					in.WantComma()
				}
				in.Delim(']')
			}
		case "company":
			out.Company = string(in.String())
		case "country":
			out.Country = string(in.String())
		case "email":
			out.Email = string(in.String())
		case "job":
			out.Job = string(in.String())
		case "name":
			out.Name = string(in.String())
		case "phone":
			out.Phone = string(in.String())
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjson9e1087fdEncodeSandboxHw3BenchData(out *jwriter.Writer, in User) {
	out.RawByte('{')
	{
		const prefix string = ",\"browsers\":"
		out.RawString(prefix[1:])
		if in.Browsers == nil && (out.Flags&jwriter.NilSliceAsEmpty) == 0 {
			out.RawString("null")
		} else {
			out.RawByte('[')
			for v2, v3 := range in.Browsers {
				if v2 > 0 {
					out.RawByte(',')
				}
				out.String(string(v3))
			}
			out.RawByte(']')
		}
	}
	{
		const prefix string = ",\"company\":"
		out.RawString(prefix)
		out.String(string(in.Company))
	}
	{
		const prefix string = ",\"country\":"
		out.RawString(prefix)
		out.String(string(in.Country))
	}
	{
		const prefix string = ",\"email\":"
		out.RawString(prefix)
		out.String(string(in.Email))
	}
	{
		const prefix string = ",\"job\":"
		out.RawString(prefix)
		out.String(string(in.Job))
	}
	{
		const prefix string = ",\"name\":"
		out.RawString(prefix)
		out.String(string(in.Name))
	}
	{
		const prefix string = ",\"phone\":"
		out.RawString(prefix)
		out.String(string(in.Phone))
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v User) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjson9e1087fdEncodeSandboxHw3BenchData(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v User) MarshalEasyJSON(w *jwriter.Writer) {
	easyjson9e1087fdEncodeSandboxHw3BenchData(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *User) UnmarshalJSON(data []byte, excludeFields []string) error {
	r := jlexer.Lexer{Data: data}
	easyjson9e1087fdDecodeSandboxHw3BenchData(&r, v, excludeFields)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *User) UnmarshalEasyJSON(l *jlexer.Lexer, excludeFields []string) {
	easyjson9e1087fdDecodeSandboxHw3BenchData(l, v, excludeFields)
}
