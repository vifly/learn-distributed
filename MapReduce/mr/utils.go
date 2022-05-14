package mr

import (
	"errors"
	"io/ioutil"
	"strconv"
	"strings"
)

var LenError = errors.New("Len is less than 1")

func pop[T any](alist *[]T) (*T, error) {
	f := len(*alist)
	if f <= 0 {
		return nil, LenError
	}
	rv := (*alist)[f-1]
	*alist = (*alist)[:f-1]
	return &rv, nil
}

func getIntermediatePath(taskId int, reduceTaskId int) string {
	return "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(reduceTaskId)
}

func getIntermediatePathsById(reduceTaskId int) []string {
	result := make([]string, 0)

	files, _ := ioutil.ReadDir("./")
	for _, file := range files {
		splitResult := strings.Split(file.Name(), "-")
		if len(splitResult) < 3 {
			continue
		}
		if splitResult[2] == strconv.Itoa(reduceTaskId) && !file.IsDir() {
			result = append(result, file.Name())
		}
	}
	return result
}

func getReduceOutputPath(taskId int) string {
	return "mr-out-" + strconv.Itoa(taskId)
}
