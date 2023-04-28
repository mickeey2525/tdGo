package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/mickeey2525/tdGo"
)

func main() {
	resClient := resty.New()
	resClient.SetTimeout(1 * time.Minute)
	apikey := os.Getenv("TD_API_KEY")
	log := tdGo.NewLevelLogger(tdGo.DEBUG, log.Default())
	client, err := tdGo.NewClient(apikey, "https://api.treasuredata.com", tdGo.WithHttpClient(resClient), tdGo.WithLogger(log))
	if err != nil {
		log.Fatalln(err)
	}

	ctx := context.Background()

	jobOption := tdGo.JobOption{
		Query:      "select 1",
		Priority:   0,
		ResultUrl:  "",
		RetryLimit: 0,
		PoolName:   "",
	}
	job, err := client.CreateJob(ctx, tdGo.Presto, "sample_datasets", jobOption)
	if err != nil {
		log.Fatalf("job create fail: %s", err)
	}

	js, err := json.Marshal(job)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(string(js))

	/*
			resultSetting := fmt.Sprintf("td://%s@api.treasuredata.com/tachibana_s3_testdb/sample_data", apikey)
			resp, err := client.SetResultExport(context.Background(), 1748136125, resultSetting)
			if err != nil {
				log.Fatalln(err)
			}
			js, err = json.Marshal(resp)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(string(js))

			fmt.Println()

		connectorConfig := `
			{
			  "config": {
			    "in": {
			      "type": "s3_v2",
			      "td_authentication_id": 12345,
			      "bucket": "your_bucket",
			      "path_prefix": "path/to/your/data",
			    },
			    "out": {
			      "mode": "append"
			    }
			  }
			}`
		guess, err := client.GuessConfig(ctx, connectorConfig)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(guess)
		issue, err := client.BulkLoadIssue(ctx, "your_database", "tdgo_sample", *guess)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(issue)

		option := tdGo.BulkLoadOption{
			Name:       "test_bulk_load",
			Cron:       "",
			Timezone:   "UTC",
			Delay:      0,
			TimeColumn: "",
		}

		source, err := client.BulkLoadCreate(ctx, "your_database", "tdgo_test", *guess, option)
		if err != nil {
			log.Fatalln(err)
		}

		print(source)

	*/
}
