package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	asl "github.com/aerospike/aerospike-client-go/logger"
	"github.com/aerospike/aerospike-client-go/v6"
	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/aerospike/aerospike-client-go/v6/types"
)

var (
	host              = "172.17.0.2"
	port              = 3000
	namespace         = "bar"
	set               = "myset"
	key               int
	bins              string
	user              string
	password          string
	authMode          string
	servicesAlternate bool
	indexBin          string
	indexVal          int
	doUdf             bool
	doQuery           bool
	shortQuery        bool
	clientChan        chan int
	sleepTimer        time.Duration
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func main() {
	asl.Logger.SetLevel(asl.DEBUG)
	// Set local directory

	as.SetLuaPath("/opt/praxis/udf/")
	// arguments
	flag.StringVar(&host, "h", host, "Remote host")
	flag.IntVar(&port, "p", port, "Remote port")
	flag.StringVar(&namespace, "n", namespace, "Namespace")
	flag.StringVar(&set, "s", set, "Set name")
	flag.IntVar(&key, "k", 1000, "Upper primary key range for single read/write. Default: 1000")
	flag.StringVar(&bins, "b", "", "Bins to set encapsulated in quotes separated by commas: Example: 'bin1:hello,bin2:24,color:green'")
	flag.StringVar(&user, "U", "data", "ClientPolicy user for authentication")
	flag.StringVar(&password, "P", "data", "ClientPolicy password for authentication")
	flag.StringVar(&authMode, "A", "internal", "AuthMode for Aerospike (needs to be implemented, hardcoded internal currently): Default: internal")
	flag.BoolVar(&servicesAlternate, "sa", false, "Enable --alternate-services: Default false")
	flag.StringVar(&indexBin, "iB", "", "Index Bin name for SI query")
	flag.IntVar(&indexVal, "iV", 0, "Index Bin value for SI query")
	flag.BoolVar(&doUdf, "u", false, "TODO: make configurable udf executions from /udf/ dir ")
	flag.BoolVar(&doQuery, "q", false, "SI runQuery: performs SI query (Default: long - use -qS to make short) and Aggregate Query; default false")
	flag.BoolVar(&shortQuery, "qS", false, "Make runQuery perform short queries. Default: false")
	clientChanSize := flag.Int("chan", 500, "Size of channel for goroutines. Default: 500")
	flag.DurationVar(&sleepTimer, "sT", time.Second, "Time to sleep in between executions of workload. Default: 1s (can also be 1000ms)")

	flag.Parse()

	log.Printf("%v", sleepTimer)

	if *clientChanSize < 50 {
		log.Fatal("Chansize cannot be less than 50 or functions may not run")
	}

	clientChan = make(chan int, *clientChanSize)
	client := initClient()
	registerUdfs(client)
	createAggrRecords(client)
	createSindexs(client)

	doJob(client)
	defer client.Close()

}

func createSindexs(client *as.Client) {
	idxtask, err := client.CreateComplexIndex(nil, namespace, set, "globalBktDefIDX", "globalBktDef", as.STRING, as.ICT_DEFAULT, as.CtxListIndex(0), as.CtxMapKey(as.StringValue("name")))
	if err != nil {
		log.Println(err)
	}
	<-idxtask.OnComplete()

	idxtask, err = client.CreateComplexIndex(nil, namespace, set, "mapBinIDX", "mapBin", as.NUMERIC, as.ICT_MAPKEYS)
	if err != nil {
		log.Println(err)
	}
	<-idxtask.OnComplete()
	log.Println("Completed creating all sindexes!")
}

func createAggrRecords(client *as.Client) {
	// Create batch of records
	batchRecords := []aerospike.BatchRecordIfc{}

	for i := 0; i < 1000; i++ {
		key, err := as.NewKey("test", "test", i+1)
		if err != nil {
			log.Fatal(err)
		}

		binArray := []map[string]string{}
		mBin := map[string]string{
			"initValue":    "123456789",
			"modifiedBy":   "Colton",
			"name":         "MONTHLY_CBU_DATA_BUCKET_OF2",
			"modifiedDate": time.Now().String(),
		}
		tBin := map[string]interface{}{
			"tariff": map[string]interface{}{
				"id":   "CL_MONTHLY_CBU_D_50MBB_OF_1000001_ChargingRules",
				"name": "ChargingRules",
				"rules": map[string]interface{}{
					"actions": []map[string]interface{}{
						{
							"attributeInfo": map[string]interface{}{
								"name":          "Bucket-Selection",
								"resultContext": "RATING",
							},
							"parameters": []map[string]interface{}{
								{
									"name": "Data",
									"value": map[string]interface{}{
										"data": map[string]interface{}{
											"type":  0,
											"value": "MONTHLY_CBU_DATA_BUCKET_OF",
										},
									},
								},
							},
						},
					},
					"condContainer": map[string]interface{}{
						"operator": 0,
					},
					"modifiedDate":  1682446313691,
					"rulename":      "rule1",
					"schemaVersion": 0,
				},
			},
		}

		binArray = append(binArray, mBin)
		globalBktDefBin := as.NewBin("globalBktDef", binArray)
		globalBktDefOp := as.PutOp(globalBktDefBin)
		tarriffBin := as.NewBin("tarriff", tBin)
		tarriffOp := as.PutOp(tarriffBin)

		record := as.NewBatchWrite(nil, key, globalBktDefOp, tarriffOp)
		batchRecords = append(batchRecords, record)
	}

	err := client.BatchOperate(nil, batchRecords)
	if err != nil {
		log.Fatal(err)
	}
}

func registerUdfs(client *as.Client) {
	// Register the UDF
	// Error on Language arg
	task, err := client.RegisterUDFFromFile(nil, "./udf/example.lua", "example.lua", as.LUA)
	if err != nil {
		log.Fatal(err)
	}

	// Wait for the task to complete
	<-task.OnComplete()
	task, err = client.RegisterUDFFromFile(nil, "./udf/dsc-query.lua", "dsc-query.lua", as.LUA)
	if err != nil {
		log.Fatal(err)
	}

	// Wait for the task to complete
	<-task.OnComplete()
	log.Println("Registered UDFs!")
}

func doJob(client *as.Client) {
	log.Print("Starting job.")
	for {
		// Inserts and Writes

		for i := 0; i < cap(clientChan)/100; i++ {
			rand.Seed(time.Now().UnixMicro())
			pKey := rand.Intn(key)
			bins := "junk:str:8,randInt:int:200,color:blue"
			clientChan <- 1
			go singleWriteRecord(client, pKey, bins)
		}

		for i := 0; i < cap(clientChan)/100; i++ {
			rand.Seed(time.Now().UnixMicro())
			pKey := rand.Intn(key)
			bins := "junk:str:8,randInt:int:200,color:red"
			clientChan <- 1
			go singleWriteRecord(client, pKey, bins)
		}

		for i := 0; i < cap(clientChan)/100; i++ {
			rand.Seed(time.Now().UnixMicro())
			pKey := rand.Intn(key)
			bins := "junk:str:8,randInt:int:200,color:green"
			clientChan <- 1
			go singleWriteRecord(client, pKey, bins)
		}

		for i := 0; i < cap(clientChan)/50; i++ {
			rand.Seed(time.Now().UnixMicro())
			pKey := rand.Intn(key)

			timestamp := time.Now().Unix()

			testMap := map[interface{}]interface{}{
				timestamp + 360: map[string]interface{}{
					"timestamp": time.Now().Format("2006-01-02"),
					"foo":       "bar",
				},
				timestamp + 120: map[string]interface{}{
					"name":      "John Doe",
					"staticEnv": "STATIC",
				},
				8675309: map[string]interface{}{
					"song":     "Jenny",
					"artist":   "Tommy Tutone",
					"released": "1981",
					"genre":    "Classic Rock",
				},
			}

			clientChan <- 1
			go oldWrite(client, pKey, testMap)
		}

		if doUdf {
			for i := 0; i < cap(clientChan)/10; i++ {
				rand.Seed(time.Now().UnixMicro())
				pKey := rand.Intn(key)
				clientChan <- 1
				go runUdf(client, pKey)
			}
		}

		//Reads
		for i := 0; i < cap(clientChan)/50; i++ {
			rand.Seed(time.Now().UnixMicro())
			pKey := rand.Intn(key)
			clientChan <- 1
			go singleReadRecord(client, pKey)
		}

		// SI Query
		if doQuery {
			for i := 0; i < cap(clientChan)/150; i++ {
				clientChan <- 1
				go runQuery(client, shortQuery)
			}
			for i := 0; i < cap(clientChan)/100; i++ {
				clientChan <- 1
				//go runAggrQuery(client)
				go runAggrQuery2(client, "MONTHLY_CBU_DATA_BUCKET_OF")
			}
		}
		time.Sleep(sleepTimer)
	}

}

func initClient() *as.Client {
	log.Print("Initializing client and policy.")
	clientPolicy := as.NewClientPolicy()
	clientPolicy.AuthMode = as.AuthModeInternal
	clientPolicy.User = user
	clientPolicy.Password = password
	clientPolicy.UseServicesAlternate = servicesAlternate
	clientPolicy.LoginTimeout = time.Second * 2
	clientPolicy.Timeout = time.Second * 2
	clientPolicy.ConnectionQueueSize = 3000
	clientPolicy.MinConnectionsPerNode = 300
	clientPolicy.MaxErrorRate = 0
	clientPolicy.ErrorRateWindow = 0
	client, err := as.NewClientWithPolicy(clientPolicy, host, port)

	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	log.Print("Connected!")
	log.Print("Warming up connections.")
	client.WarmUp(1000)

	return client
}

func runAggrQuery2(client *as.Client, bin string) {
	defer func() { <-clientChan }()

	stmt := as.NewStatement(namespace, set)
	stmt.SetFilter(as.NewEqualFilter("globalBktDef", bin, as.CtxListIndex(0), as.CtxMapKey(as.NewValue("name"))))

	queryPolicy := as.NewQueryPolicy()
	queryPolicy.SleepBetweenRetries = 300

	_, err := client.QueryAggregate(queryPolicy, stmt, "dsc-query", "genericQuery") //, as.NewStringValue("0"))

	if err != nil {
		fmt.Println(err)
	}
}

func runAggrQuery(client *as.Client) {
	defer func() { <-clientChan }()

	stmt := as.NewStatement(namespace, set)
	stmt.SetFilter(as.NewContainsFilter("mapBin", as.ICT_MAPKEYS, as.NewValue(8675309)))

	queryPolicy := as.NewQueryPolicy()
	queryPolicy.SleepBetweenRetries = 300

	_, err := client.QueryAggregate(queryPolicy, stmt, "dsc-query", "genericQuery")

	if err != nil {
		fmt.Println(err)
	}

}

func runUdf(client *as.Client, k int) {
	defer func() { <-clientChan }()

	key, err := as.NewKey(namespace, set, k)

	if err != nil {
		log.Print("Unable to generate digest")
		return
	}

	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.SendKey = true
	writePolicy.Expiration = 1440
	writePolicy.TotalTimeout = time.Second * 2
	writePolicy.SocketTimeout = time.Second * 2
	writePolicy.MaxRetries = 0

	result, err := client.Execute(writePolicy, key, "sonic-functions", "fetchOrCreate2", as.NewStringValue("color"), as.NewStringValue("BLACK"), as.NewIntegerValue(10000))

	if err != nil {
		log.Print("Unable to write single record", err)
	}

	//fmt.Println(rec)
	log.Print("UDF", result)
}

func runQuery(client *as.Client, isShort bool) {
	defer func() { <-clientChan }()

	queryPolicy := as.NewQueryPolicy()
	if isShort {
		queryPolicy.ShortQuery = true
	} else {
		queryPolicy.ShortQuery = false
	}
	queryPolicy.MaxRetries = 2
	//queryPolicy.MaxRecords = 5000

	filter := as.NewContainsFilter("mapBin", as.ICT_MAPKEYS, as.NewValue(8675309))

	stmt := as.NewStatement(namespace, set)
	stmt.Filter = filter

	records, err := client.Query(queryPolicy, stmt)

	if err != nil {
		log.Print(err)
		return
	}

	count := 0
	for res := range records.Results() {
		if res.Err != nil {
			// handle error here
			log.Print("res.Err: ", res.Err)
		} else {
			count++
			currentTime := time.Now().Unix()
			updateRecord := false
			v, ok := res.Record.Bins["mapBin"].(map[interface{}]interface{})
			if ok {
				for k := range v {
					if i64, ok := k.(int64); ok {
						if i64 != 8675309 && i64 < currentTime {
							updateRecord = true
						}
					}

				}
				if updateRecord {
					go updateMapRecord(client, res.Record.Key, v)
				}

			}

		}
	}
	records.Close()

}

var maxUpdate = make(chan int, 500)

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func parseBinsToPutOperations(bins string) []*as.Operation {
	var binOps []*as.Operation

	parsedBins := strings.Split(bins, ",")
	for _, bins := range parsedBins {
		bin := strings.Split(bins, ":")
		if len(bin) == 3 {
			binName, binType, binLength := bin[0], bin[1], bin[2]
			switch binType {
			case "string", "str", "s":
				if binLength, err := strconv.Atoi(binLength); err == nil {
					rand.Seed(time.Now().UnixMicro())
					randStr := RandStringBytes(binLength)
					newBin := as.NewBin(binName, randStr)
					binOp := as.PutOp(newBin)
					binOps = append(binOps, binOp)
				} else {
					log.Printf("Unable to parse bin: %v. Ignoring bin.\n", bins)
				}
			case "int", "i":
				maxMinRange := strings.Split(binLength, "-")
				if maxMinRange[0] == "" {
					maxMinRange = nil
				}

				switch len(maxMinRange) {

				case 1:
					if max, err := strconv.Atoi(maxMinRange[0]); err == nil {
						randInt := rand.Intn(max)
						newBin := as.NewBin(binName, randInt)
						binOp := as.PutOp(newBin)
						binOps = append(binOps, binOp)
					}
				case 2:
					log.Println(len(maxMinRange), maxMinRange)
					if max, err := strconv.Atoi(maxMinRange[1]); err == nil {
						if min, err := strconv.Atoi(maxMinRange[0]); err == nil {
							randInt := rand.Intn(max-min) + min
							newBin := as.NewBin(binName, randInt)
							binOp := as.PutOp(newBin)
							binOps = append(binOps, binOp)
							log.Println(randInt)
						} else {
							log.Printf("Unable to parse bin: %v. Ignoring bin.\n", bins)
						}
					} else {
						log.Printf("Unable to parse bin: %v. Ignoring bin.\n", bins)
					}
				}
			}

		} else if len(bin) == 2 {
			binName, binVal := bin[0], bin[1]
			if binValInt, err := strconv.Atoi(binVal); err == nil {
				newBin := as.NewBin(binName, binValInt)
				binOp := as.PutOp(newBin)
				binOps = append(binOps, binOp)
			} else {
				newBin := as.NewBin(binName, binVal)
				binOp := as.PutOp(newBin)
				binOps = append(binOps, binOp)
			}
		} else {
			log.Printf("Unable to parse bin: %v. Ignoring bin.\n", bins)
		}

	}

	return binOps
}

func singleReadRecord(client *as.Client, k int) *as.Record {
	defer func() { <-clientChan }()

	key, err := as.NewKey(namespace, set, k)

	if err != nil {
		log.Print("Unable to generate digest")
		return nil
	}

	readPolicy := as.NewPolicy()
	readPolicy.TotalTimeout = 5 * time.Second
	readPolicy.SocketTimeout = 5 * time.Second
	readPolicy.MaxRetries = 0
	rec, err := client.Get(readPolicy, key)

	if err != nil {
		if !err.Matches(types.KEY_NOT_FOUND_ERROR) {
			log.Println("Unable to retrieve single record", err)
		}

	}

	return rec
}

func singleWriteRecord(client *as.Client, k int, bins string) {
	defer func() { <-clientChan }()

	key, err := as.NewKey(namespace, set, k)

	if err != nil {
		log.Print("Unable to generate digest")
		return
	}
	var binOps []*as.Operation
	if len(bins) > 0 {
		binOps = parseBinsToPutOperations(bins)
	} else {
		bins = "num:123"
		binOps = parseBinsToPutOperations(bins)
	}

	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.SendKey = true
	writePolicy.Expiration = 360
	writePolicy.TotalTimeout = time.Second * 2
	writePolicy.SocketTimeout = time.Second * 2

	rec, err := client.Operate(writePolicy, key, binOps...)

	if err != nil {
		log.Print("Unable to write single record ", err)
	}
	_ = rec
}

func updateMapRecord(client *as.Client, key *as.Key, mapContents map[interface{}]interface{}) {
	defer func() { <-clientChan }()
	defer func() { <-maxUpdate }()

	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.SendKey = true
	writePolicy.Expiration = 3600
	writePolicy.TotalTimeout = time.Second * 2
	writePolicy.SocketTimeout = time.Second * 2

	mapPolicy := as.NewMapPolicy(as.MapOrder.UNORDERED, as.MapWriteMode.CREATE_ONLY)

	mapPutOps := []*as.Operation{}

	timestamp := time.Now().Unix()

	for mapKey, value := range mapContents {
		mapKey, ok := mapKey.(int64)
		if ok {
			if mapKey < timestamp {
				mapPutOps = append(mapPutOps, as.MapRemoveByKeyOp("mapBin", mapKey, as.MapReturnType.NONE))
				mapPutOps = append(mapPutOps, as.MapPutOp(mapPolicy, "mapBin", as.NewValue(mapKey+360), as.NewValue(value)))
			}
		} else {
			return
		}

	}

	rec, err := client.Operate(writePolicy, key, mapPutOps...)

	if err != nil {
		log.Print("Unable to write single record", err)
	}

	_ = rec
}

func oldWrite(client *as.Client, k int, mapContents map[interface{}]interface{}) {
	defer func() { <-clientChan }()

	key, err := as.NewKey(namespace, set, k)

	if err != nil {
		log.Print("Unable to generate digest")
		return
	}

	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.SendKey = true
	writePolicy.Expiration = 360
	writePolicy.TotalTimeout = time.Second * 2
	writePolicy.SocketTimeout = time.Second * 2

	mapPolicy := as.NewMapPolicy(as.MapOrder.UNORDERED, as.MapWriteMode.UPDATE)

	rec, err := client.Operate(writePolicy, key, as.MapPutOp(mapPolicy, "mapBin", 8675309, "new value 2nd client"))

	if err != nil {
		log.Print("Unable to write single record", err)
	}

	_ = rec
}
