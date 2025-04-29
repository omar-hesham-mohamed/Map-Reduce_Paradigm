package mapreduce

import (
	"hash/fnv"
	"os"
	"io"
	"encoding/json"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file that was assigned to this task
	nReduce int, // The number of reduce tasks
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	// ToDo: Write this function. See the description in the assignment.

	file, err := os.Open(inputFile)
    checkError(err)
	defer file.Close()

	data, err := io.ReadAll(file)
	checkError(err)
	
	values := mapFn(inputFile, string(data))
	
	files := make([]*os.File, nReduce) // to close files later
	encoders := make([]*json.Encoder, nReduce) // each encoder has only one output stream or file, so one for each file
	// fmt.Println(nReduce)


	for i := 0; i < nReduce; i++ {
		fileName := getIntermediateName(jobName, mapTaskIndex, i)

		outputFile, err := os.Create(fileName)
		// fmt.Println("Creating intermediate file:", fileName)
		checkError(err)

		files[i] = outputFile
		encoders[i] = json.NewEncoder(outputFile)
	}

	for _, kv := range values {
		fileIndex := int(hash32(kv.Key)) % nReduce

		err := encoders[fileIndex].Encode(&kv)
		checkError(err)
	}

	for _, f := range files {
        if f != nil {
            f.Close()
        }
    }

}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks
	reduceFn func(key string, values []string) string,
) {
	// ToDo: Write this function. See the description in the assignment.

	
	values := make(map[string][]string)


	// fmt.Println(nMap)
	for i := 0; i < nMap; i++ {
		fileName := getIntermediateName(jobName, i, reduceTaskIndex)
		// fmt.Println("Opening intermediate file:", fileName)

		file, err := os.Open(fileName)
		if err != nil {
			if os.IsNotExist(err) {
				// fmt.Println("File does not exist:", fileName)
				continue
			}else {
				checkError(err)
			}
			
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		var kv KeyValue

		// fmt.Printf("Reading from file %v \n", fileName)
		for {
			err := decoder.Decode(&kv)
			if err != nil{
				if err == io.EOF {
					break
				}else {
					checkError(err)
				}
			}

			values[kv.Key] = append(values[kv.Key], kv.Value)

		}

	}

	outName := getReduceOutName(jobName, reduceTaskIndex)

	// fmt.Println("Opening output file:", outName)

	outFile, err := os.Create(outName)
	checkError(err)
	defer outFile.Close()

	// fmt.Println("Writing in output file:", outName)

	for key, value := range values {
		output := reduceFn(key, value)

		encoder := json.NewEncoder(outFile)

		kv := KeyValue{Key: key, Value: output}

		err := encoder.Encode(&kv)
		checkError(err)
	}

}
