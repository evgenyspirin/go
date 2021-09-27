package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {
	out := os.Stdout
	if !(len(os.Args) == 2 || len(os.Args) == 3) {
		panic("usage go run main.go . [-f]")
	}

	path := os.Args[1]
	printFiles := len(os.Args) == 3 && os.Args[2] == "-f"
	err := dirTree(out, path, printFiles)
	if err != nil {
		panic(err.Error())
	}
}

func dirTree(output io.Writer, path string, includeFiles bool) error {
	var result string
	recurs(path, includeFiles, "", &result)
	fmt.Fprintln(output, strings.TrimSuffix(result, "\n"))
	return nil
}

func recurs(path string, includeFiles bool, space string, result *string) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	if !includeFiles {
		tmp := files[:0]
		for _, file := range files {
			if file.IsDir() {
				tmp = append(tmp, file)
			}
		}
		files = tmp
	}

	for key, file := range files {
		prefix := "│\t"
		size := ""
		if !includeFiles && !file.IsDir() {
			continue
		}

		if key == len(files)-1 {
			*result += space + "└───"
			prefix = "\t"
		} else {
			*result += space + "├───"
		}

		if !file.IsDir() && (file.Size() < 1) {
			size = " (empty)"
		} else if !file.IsDir() {
			size = " (" + strconv.FormatInt(file.Size(), 10) + "b)"
		}

		*result += file.Name() + size + "\n"
		if file.IsDir() {
			recurs(path+"/"+file.Name(), includeFiles, space+prefix, result)
		}
	}
}
