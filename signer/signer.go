package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func SingleHash(in, out chan interface{}) {
	singleWG, singleMu := &sync.WaitGroup{}, &sync.Mutex{}

	for val := range in {

		singleWG.Add(1)
		go func(val string) {
			defer singleWG.Done()

			singleMu.Lock()
			md5Data := DataSignerMd5(val)
			singleMu.Unlock()

			crc32Chan := make(chan string)
			go func(data string, crc32Chan chan string) {
				crc32Chan <- DataSignerCrc32(data)
				close(crc32Chan)
			}(val, crc32Chan)

			md5DataInCrc32 := DataSignerCrc32(md5Data)
			crc32Data := <-crc32Chan

			resultSingleHash := crc32Data + "~" + md5DataInCrc32

			fmt.Printf("%v SingleHash data %v \n", val, val)
			fmt.Printf("%v SingleHash md5(data) %v \n", val, md5Data)
			fmt.Printf("%v SingleHash crc32(md5(data)) %v \n", val, md5DataInCrc32)
			fmt.Printf("%v SingleHash crc32(data) %v \n", val, crc32Data)
			fmt.Printf("%v SingleHash result %v \n", val, resultSingleHash)

			out <- resultSingleHash
		}(strconv.Itoa(val.(int)))
	}
	singleWG.Wait()
}

func MultiHash(in, out chan interface{}) {
	multiWG := &sync.WaitGroup{}

	for val := range in {

		multiWG.Add(1)
		go func(val string) {
			defer multiWG.Done()

			mltRslts, multiCalcWGCrc32 := make([]string, 6), &sync.WaitGroup{}

			for th := 0; th <= 5; th++ {
				multiCalcWGCrc32.Add(1)
				go func(th int) {
					defer multiCalcWGCrc32.Done()

					prepareVal := strconv.Itoa(th) + val
					crc32result := DataSignerCrc32(prepareVal)

					mltRslts[th] = crc32result

					fmt.Printf("%v MultiHash: crc32(th+step1)) %v %v\n", val, th, crc32result)
				}(th)
			}
			multiCalcWGCrc32.Wait()
			resultString := strings.Join(mltRslts, "")

			fmt.Printf("%v MultiHash: result: %v\n", val, resultString)

			out <- resultString
		}(val.(string))
	}

	multiWG.Wait()
}

func CombineResults(in, out chan interface{}) {
	var values []string

	for val := range in {
		values = append(values, val.(string))
	}

	sort.Strings(values)
	result := strings.Join(values, "_")

	out <- result
}

func ExecutePipeline(jobs ...job) {
	in := make(chan interface{})
	var executeWG = &sync.WaitGroup{}

	for _, job := range jobs {
		out := make(chan interface{})

		executeWG.Add(1)
		go jobAdapter(job, in, out, executeWG)

		in = out
	}

	executeWG.Wait()
}

func jobAdapter(job job, in, out chan interface{}, executeWG *sync.WaitGroup) {
	defer executeWG.Done()

	job(in, out)
	close(out)
}
