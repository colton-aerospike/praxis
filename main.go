package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	asl "github.com/aerospike/aerospike-client-go/logger"
	as "github.com/aerospike/aerospike-client-go/v6"
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
	indexVal          string
	runQuery          bool
	clientChan        chan int
	sleepTimer        time.Duration
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func main() {
	asl.Logger.SetLevel(asl.DEBUG)
	// arguments
	flag.StringVar(&host, "h", host, "Remote host")
	flag.IntVar(&port, "p", port, "Remote port")
	flag.StringVar(&namespace, "n", namespace, "Namespace")
	flag.StringVar(&set, "s", set, "Set name")
	flag.IntVar(&key, "k", 1000, "Upper primary key range for single read/write. Default: 1000")
	flag.StringVar(&bins, "b", "", "Bins to set encapsulated in quotes separated by commas 'bin1:hello,bin2:24,color:green'")
	flag.StringVar(&user, "U", "data", "ClientPolicy user for authentication")
	flag.StringVar(&password, "P", "data", "ClientPolicy password for authentication")
	flag.StringVar(&authMode, "A", "internal", "AuthMode for Aerospike (needs to be implemented, hardcoded internal currently)")
	flag.BoolVar(&servicesAlternate, "sa", false, "Enable --alternate-services; default false")
	flag.StringVar(&indexBin, "iB", "", "Index Bin name for SI query")
	flag.StringVar(&indexVal, "iV", "", "Index Bin value for SI query")
	flag.BoolVar(&runQuery, "q", false, "Run short SI query; default false")
	clientChanSize := flag.Int("chan", 500, "Size of channel for goroutines")
	flag.DurationVar(&sleepTimer, "sT", time.Second, "Time to sleep in between executions of workload")

	flag.Parse()

	log.Printf("%v", sleepTimer)

	if *clientChanSize < 50 {
		log.Fatal("Chansize cannot be less than 50 or functions may not run")
	}

	clientChan = make(chan int, *clientChanSize)
	client := initClient()
	doJob(client)
	defer client.Close()

}

func doJob(client *as.Client) {
	log.Print("Starting job.")
	for {
		// Inserts and Writes
		for i := 0; i < cap(clientChan)/5; i++ {
			rand.Seed(time.Now().UnixMicro())
			pKey := rand.Intn(key)
			bins := "junk:str:8,randInt:int:200,color:blue"
			clientChan <- 1
			go singleWriteRecord(client, pKey, bins)
		}

		for i := 0; i < cap(clientChan)/5; i++ {
			rand.Seed(time.Now().UnixMicro())
			pKey := rand.Intn(key)
			bins := "junk:str:8,randInt:int:200,color:red"
			clientChan <- 1
			go singleWriteRecord(client, pKey, bins)
		}

		for i := 0; i < cap(clientChan)/10; i++ {
			rand.Seed(time.Now().UnixMicro())
			pKey := rand.Intn(key)
			bins := "junk:str:8,randInt:int:200,color:green"
			clientChan <- 1
			go singleWriteRecord(client, pKey, bins)
		}

		// Reads
		for i := 0; i < cap(clientChan)/5; i++ {
			rand.Seed(time.Now().UnixMicro())
			pKey := rand.Intn(key)
			clientChan <- 1
			go singleReadRecord(client, pKey)
		}

		// SI Query
		for i := 0; i < cap(clientChan)/50; i++ {
			clientChan <- 1
			go runShortQuery(client)
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
	clientPolicy.ConnectionQueueSize = 1000
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

func runShortQuery(client *as.Client) {
	defer func() { <-clientChan }()

	queryPolicy := as.NewQueryPolicy()
	queryPolicy.ShortQuery = true
	queryPolicy.MaxRetries = 0

	filter := as.NewEqualFilter(indexBin, indexVal)

	stmt := as.NewStatement(namespace, set)
	stmt.Filter = filter

	records, err := client.Query(queryPolicy, stmt)

	if err != nil {
		log.Print(err)
		return
	}

	for res := range records.Results() {
		if res.Err != nil {
			// handle error here
			log.Print("res.Err: ", res.Err)
		} else {
			// process record here
			//fmt.Println(res)
		}
	}

}

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
		log.Println("Unable to retrieve single record", err)
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
		bins = "age:24"
		binOps = parseBinsToPutOperations(bins)
	}

	writePolicy := as.NewWritePolicy(0, 0)
	writePolicy.SendKey = true
	writePolicy.Expiration = 360
	writePolicy.TotalTimeout = time.Second * 2
	writePolicy.SocketTimeout = time.Second * 2

	rec, err := client.Operate(writePolicy, key, binOps...)

	if err != nil {
		log.Print("Unable to write single record", err)
	}

	//fmt.Println(rec)
	_ = rec
}
