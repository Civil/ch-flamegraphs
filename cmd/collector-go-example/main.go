package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/Civil/ch-flamegraphs/tracer"
	redigo "github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	//	"github.com/stackimpact/stackimpact-go"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	goredis "gopkg.in/redis.v5"
)

func useCPU(duration int, usage int) {
	for j := 0; j < duration; j++ {
		go func() {
			for i := 0; i < usage*80000; i++ {
				str := "str" + strconv.Itoa(i)
				str = str + "a"
			}
		}()

		time.Sleep(1 * time.Second)
	}
}

func simulateCPUUsage() {
	// sumulate CPU usage anomaly - every 45 minutes
	cpuAnomalyTicker := time.NewTicker(45 * time.Minute)
	go func() {
		for {
			select {
			case <-cpuAnomalyTicker.C:
				// for 60 seconds produce generate 50% CPU usage
				useCPU(60, 50)
			}
		}
	}()

	// generate constant ~10% CPU usage
	useCPU(math.MaxInt64, 10)
}

func leakMemory(duration int, size int) {
	mem := make([]string, 0)

	for j := 0; j < duration; j++ {
		go func() {
			for i := 0; i < size; i++ {
				mem = append(mem, string(i))
			}
		}()

		time.Sleep(1 * time.Second)
	}
}

func simulateMemoryLeak() {
	// simulate memory leak - constantly
	constantTicker := time.NewTicker(2 * 3600 * time.Second)
	go func() {
		for {
			select {
			case <-constantTicker.C:
				leakMemory(2*3600, 1000)
			}
		}
	}()

	go leakMemory(2*3600, 1000)
}

func simulateChannelWait() {
	for {
		done := make(chan bool)

		go func() {
			wait := make(chan bool)

			go func() {
				time.Sleep(500 * time.Millisecond)

				wait <- true
			}()

			<-wait

			done <- true
		}()

		<-done

		time.Sleep(500 * time.Millisecond)
	}
}

func simulateNetworkWait() {
	// start HTTP server
	go func() {
		http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
			done := make(chan bool)

			go func() {
				time.Sleep(time.Duration(rand.Intn(400)) * time.Millisecond)
				done <- true
			}()
			<-done

			fmt.Fprintf(w, "OK")
		})

		if err := http.ListenAndServe(":5000", nil); err != nil {
			log.Fatal(err)
			return
		}
	}()

	requestTicker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-requestTicker.C:
			res, err := http.Get("http://localhost:5000/test")
			if err == nil {
				res.Body.Close()
			}
		}
	}
}

func simulateSyscallWait() {
	for {
		done := make(chan bool)

		go func() {
			_, err := exec.Command("sleep", "1").Output()
			if err != nil {
				fmt.Println(err)
			}

			done <- true
		}()

		time.Sleep(1 * time.Second)

		<-done
	}
}

func simulateLockWait() {
	for {
		done := make(chan bool)

		lock := &sync.Mutex{}
		lock.Lock()

		go func() {
			lock.Lock()

			done <- true
		}()

		go func() {
			time.Sleep(500 * time.Millisecond)
			lock.Unlock()
		}()

		<-done

		time.Sleep(500 * time.Millisecond)
	}
}

func simulateSQL() {
	db, err := sql.Open("mysql", "test:test@/test")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	stmtIns, err := db.Prepare("INSERT INTO test_table VALUES( ?, ? )")
	if err != nil {
		fmt.Println(err)
	}
	defer stmtIns.Close()

	stmtOut, err := db.Prepare("SELECT name FROM test_table WHERE id = ?")
	if err != nil {
		fmt.Println(err)
	}
	defer stmtOut.Close()

	for {
		_, err = db.Exec("DELETE FROM test_table")
		if err != nil {
			fmt.Println(err)
		}

		for i := 1; i <= 25; i++ {
			_, err = stmtIns.Exec(i, fmt.Sprintf("text%v", i))
			if err != nil {
				fmt.Println(err)
			}
		}

		var text string

		err = stmtOut.QueryRow(10).Scan(&text)
		if err != nil {
			fmt.Println(err)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

type TestDoc struct {
	ID   string `bson:"id"`
	Text string `bson:"text"`
}

func simulateMongo() {
	session, err := mgo.Dial("mongodb://localhost")
	if err != nil {
		fmt.Println(err)
	}
	defer session.Close()

	collection := session.DB("test").C("testcol")

	for {
		_, err = collection.RemoveAll(bson.M{})
		if err != nil {
			fmt.Println(err)
		}

		_, err = collection.UpsertId("1", &TestDoc{"1", "text1"})
		if err != nil {
			fmt.Println(err)
		}

		var testDocs []TestDoc
		err := collection.Find(nil).All(&testDocs)
		if err != nil {
			fmt.Println(err)
		}

		time.Sleep(300 * time.Millisecond)
	}
}

func simulateRedigo() {
	client, err := redigo.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	for {
		client.Do("SET", "key1", "1")
		client.Do("GET", "key1")
		client.Send("SET", "key1", "2")
		client.Send("GET", "key1")
		client.Flush()
		client.Receive()
		client.Receive()

		time.Sleep(400 * time.Millisecond)
	}
}

func simulateGoredis() {
	client := goredis.NewClient(&goredis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	for {
		client.Set("key1", "1", 0).Err()

		pipe := client.Pipeline()
		pipe.Incr("key1")
		pipe.Exec()

		txPipe := client.TxPipeline()
		txPipe.Get("key1")
		txPipe.Exec()

		time.Sleep(400 * time.Millisecond)
	}
}

func main() {
	cfg := tracer.Config{
		Application:        "example",
		Instance:           "i1",
		Endpoint:           "http://localhost:8080",
		ProfileDuration:    5 * time.Second,
		CollectionInterval: 60 * time.Second,
		Debug:              false,
	}
	agent := tracer.NewStackTracer(cfg)
	agent.Start()
	/*
		// StackImpact initialization
		agent = stackimpact.NewAgent()
		agent.Start(stackimpact.Options{
			AgentKey:         os.Getenv("AGENT_KEY"),
			AppName:          "ExampleGoApp",
			AppVersion:       "1.0.0",
			DashboardAddress: os.Getenv("DASHBOARD_ADDRESS"), // on-premises only
			Debug:            true,
		})
		// end StackImpact initialization
	*/

	go simulateCPUUsage()
	go simulateMemoryLeak()
	go simulateChannelWait()
	go simulateNetworkWait()
	go simulateSyscallWait()
	go simulateLockWait()
	// go simulateSegments()
	// go simulateHandlerSegments()
	// go simulateErrors()
	//go simulateSQL()
	// go simulateMongo()
	// go simulateRedigo()
	// go simulateGoredis()

	log.Print("Started")
	select {}
}
