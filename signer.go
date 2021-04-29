package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

// сюда писать код
func ExecutePipeline(jobs ...job) {
	in := make(chan interface{})
	wg := &sync.WaitGroup{}

	for _, job1 := range jobs {
		wg.Add(1)
		out := make(chan interface{})
		go func(waitGroup *sync.WaitGroup, job1 job, in, out chan interface{}) {
			job1(in, out)
			close(out)
			waitGroup.Done()
		}(wg, job1, in, out)
		in = out
	}

	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	for unit := range in {
		wg.Add(1)
		go func(in interface{}, out chan interface{}, wg *sync.WaitGroup, mu *sync.Mutex) {
			defer wg.Done()
			data := strconv.Itoa(in.(int))

			mu.Lock()
			md5Data := DataSignerMd5(data)
			mu.Unlock()

			crc32ch := make(chan string)
			go func(data string, out chan string) {
				out <- DataSignerCrc32(data)
			}(data, crc32ch)

			crc32 := <-crc32ch
			crc32md := DataSignerCrc32(md5Data)

			out <- crc32 + "~" + crc32md
		}(unit, out, wg, mu)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	const TH int = 6
	wg := &sync.WaitGroup{}

	for unit := range in {
		wg.Add(1)
		go func(in string, out chan interface{}, th int, wg *sync.WaitGroup) {
			defer wg.Done()

			wg1 := &sync.WaitGroup{}
			thData := make([]string, th)

			for i := 0; i < th; i++ {
				wg1.Add(1)
				data := strconv.Itoa(i) + in

				go func(acc []string, index int, data string, jobWg *sync.WaitGroup) {
					defer jobWg.Done()
					data = DataSignerCrc32(data)
					acc[index] = data
				}(thData, i, data, wg1)
			}

			wg1.Wait()
			out <- strings.Join(thData, "")
		}(unit.(string), out, TH, wg)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var data []string

	for unit := range in {
		data = append(data, unit.(string))
	}

	sort.Strings(data)
	out <- strings.Join(data, "_")
}
