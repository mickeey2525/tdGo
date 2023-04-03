package tdGo

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestClient_CreateJob(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"job":"123456","job_id":"123456","database":"sample_datasets"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	jo := JobOption{
		Query:      "select 1",
		Priority:   0,
		PoolName:   "",
		RetryLimit: 0,
		ResultUrl:  "",
	}
	got, err := client.CreateJob(context.Background(), Presto, "sample_datasets", jo)
	if err != nil {
		log.Fatalln(err)
	}
	want := &JobInfo{
		Job:      "123456",
		JobId:    "123456",
		Database: "sample_datasets",
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetJobList(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
{
  "count": 1,
  "from": 0,
  "to": 0,
  "jobs": [
    {
      "job_id": "123456",
      "cpu_time": null,
      "created_at": "2023-03-23 01:58:01 UTC",
      "duration": 1,
      "end_at": "2023-03-23 01:58:02 UTC",
      "num_records": 10000,
      "result_size": 771821,
      "start_at": "2023-03-23 01:58:01 UTC",
      "status": "success",
      "updated_at": "2023-03-23 01:58:03 UTC",
      "database": "sample_datasets",
      "hive_result_schema": "[[\"page_type\", \"varchar\"], [\"td_title\", \"varchar\"], [\"td_browser\", \"varchar\"], [\"td_color\", \"varchar\"], [\"td_path\", \"varchar\"], [\"td_ip\", \"varchar\"], [\"td_version\", \"varchar\"], [\"td_client_id\", \"varchar\"], [\"td_os_version\", \"varchar\"], [\"td_browser_version\", \"varchar\"], [\"td_viewport\", \"varchar\"], [\"td_charset\", \"varchar\"], [\"td_os\", \"varchar\"], [\"td_screen\", \"varchar\"], [\"td_referrer\", \"varchar\"], [\"td_url\", \"varchar\"], [\"td_host\", \"varchar\"], [\"td_language\", \"varchar\"], [\"td_global_id\", \"varchar\"], [\"td_foo\", \"varchar\"], [\"td_platform\", \"varchar\"], [\"td_user_agent\", \"varchar\"], [\"member_id\", \"varchar\"], [\"goods_id\", \"varchar\"], [\"category\", \"varchar\"], [\"sub_category\", \"varchar\"], [\"ship_date\", \"varchar\"], [\"amount\", \"varchar\"], [\"price\", \"varchar\"], [\"_col23\", \"varchar\"], [\"time\", \"bigint\"]]",
      "linked_result_export_job_id": null,
      "organization": null,
      "priority": 0,
      "query": "select * from access_log",
      "result": "",
      "result_export_target_job_id": null,
      "retry_limit": 0,
      "type": "presto",
      "url": "https://console.treasuredata.com/app/jobs/123456",
      "user_name": "user_name"
    }
  ]
}
`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.GetJobList(context.Background(), 0, 0, All)
	if err != nil {
		log.Fatalln(err)
	}
	want := &JobList{
		Count: 1,
		From:  0,
		To:    0,
		Jobs: []Job{
			{
				JobId:                   "123456",
				CpuTime:                 nil,
				CreatedAt:               "2023-03-23 01:58:01 UTC",
				Duration:                1,
				EndAt:                   "2023-03-23 01:58:02 UTC",
				NumRecords:              10000,
				ResultSize:              771821,
				StartAt:                 "2023-03-23 01:58:01 UTC",
				Status:                  "success",
				UpdatedAt:               "2023-03-23 01:58:03 UTC",
				Database:                "sample_datasets",
				HiveResultSchema:        "[[\"page_type\", \"varchar\"], [\"td_title\", \"varchar\"], [\"td_browser\", \"varchar\"], [\"td_color\", \"varchar\"], [\"td_path\", \"varchar\"], [\"td_ip\", \"varchar\"], [\"td_version\", \"varchar\"], [\"td_client_id\", \"varchar\"], [\"td_os_version\", \"varchar\"], [\"td_browser_version\", \"varchar\"], [\"td_viewport\", \"varchar\"], [\"td_charset\", \"varchar\"], [\"td_os\", \"varchar\"], [\"td_screen\", \"varchar\"], [\"td_referrer\", \"varchar\"], [\"td_url\", \"varchar\"], [\"td_host\", \"varchar\"], [\"td_language\", \"varchar\"], [\"td_global_id\", \"varchar\"], [\"td_foo\", \"varchar\"], [\"td_platform\", \"varchar\"], [\"td_user_agent\", \"varchar\"], [\"member_id\", \"varchar\"], [\"goods_id\", \"varchar\"], [\"category\", \"varchar\"], [\"sub_category\", \"varchar\"], [\"ship_date\", \"varchar\"], [\"amount\", \"varchar\"], [\"price\", \"varchar\"], [\"_col23\", \"varchar\"], [\"time\", \"bigint\"]]",
				LinkedResultExportJobId: nil,
				Organization:            nil,
				Priority:                0,
				Query:                   "select * from access_log",
				Result:                  "",
				ResultExportTargetJobId: nil,
				RetryLimit:              0,
				Type:                    "presto",
				Url:                     "https://console.treasuredata.com/app/jobs/123456",
				UserName:                "user_name",
			},
		},
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}

}

func TestClient_ShowJob(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
{
      "job_id": "123456",
      "cpu_time": null,
      "created_at": "2023-03-23 01:58:01 UTC",
      "duration": 1,
      "end_at": "2023-03-23 01:58:02 UTC",
      "num_records": 10000,
      "result_size": 771821,
      "start_at": "2023-03-23 01:58:01 UTC",
      "status": "success",
      "updated_at": "2023-03-23 01:58:03 UTC",
      "database": "sample_datasets",
      "hive_result_schema": "[[\"page_type\", \"varchar\"], [\"td_title\", \"varchar\"], [\"td_browser\", \"varchar\"], [\"td_color\", \"varchar\"], [\"td_path\", \"varchar\"], [\"td_ip\", \"varchar\"], [\"td_version\", \"varchar\"], [\"td_client_id\", \"varchar\"], [\"td_os_version\", \"varchar\"], [\"td_browser_version\", \"varchar\"], [\"td_viewport\", \"varchar\"], [\"td_charset\", \"varchar\"], [\"td_os\", \"varchar\"], [\"td_screen\", \"varchar\"], [\"td_referrer\", \"varchar\"], [\"td_url\", \"varchar\"], [\"td_host\", \"varchar\"], [\"td_language\", \"varchar\"], [\"td_global_id\", \"varchar\"], [\"td_foo\", \"varchar\"], [\"td_platform\", \"varchar\"], [\"td_user_agent\", \"varchar\"], [\"member_id\", \"varchar\"], [\"goods_id\", \"varchar\"], [\"category\", \"varchar\"], [\"sub_category\", \"varchar\"], [\"ship_date\", \"varchar\"], [\"amount\", \"varchar\"], [\"price\", \"varchar\"], [\"_col23\", \"varchar\"], [\"time\", \"bigint\"]]",
      "linked_result_export_job_id": null,
      "organization": null,
      "priority": 0,
      "query": "select * from access_log",
      "result": "",
      "result_export_target_job_id": null,
      "retry_limit": 0,
      "type": "presto",
      "url": "https://console.treasuredata.com/app/jobs/123456",
      "user_name": "user_name",
      "debug": {
        "cmdout": "started at 2023-03-23T05:22:00Z\npresto version: 350\nexecuting query: select * from access_log\n**\n** WARNING: time index filtering is not set on\n  - kohki_test.access_log\n** This query could be very slow as a result.\n** Please see https://docs.treasuredata.com/display/public/PD/Leveraging+Time-Based+Partitioning\n**\n2023-03-23 05:22:01 -- peak memory: 622K, queued time:00:00:00.000\n20230323_052201_54564_z5ga4                              00:00:00.570   rows  bytes bytes/sec done   total \n[0] output <- tablefinish <- exchange <- remotesource[1] FINISHED          8  1.5MB   772MB/s    5 /     5 \n [1] tablewriter <- exchange <- tablescan[*] FullScan    FINISHED     10,000  586KB   714KB/s    8 /     8 \n10000 rows.\n2023-03-23 05:22:01 -- peak memory: 622K, queued time:00:00:00.000\n20230323_052201_54564_z5ga4                              00:00:00.570   rows  bytes bytes/sec done   total \n[0] output <- tablefinish <- exchange <- remotesource[1] FINISHED          8  1.5MB   772MB/s    5 /     5 \n [1] tablewriter <- exchange <- tablescan[*] FullScan    FINISHED     10,000  586KB   714KB/s    8 /     8 \n\nQuery plan:\nFragment 0 [COORDINATOR_ONLY]\n    CPU: 1.24ms, Scheduled: 2.06ms, Input: 8 rows (1.47MB); per task: avg.: 8.00 std.dev.: 0.00, Output: 1 row (9B)\n    Output layout: [rows]\n    Output partitioning: SINGLE []\n    Stage Execution Strategy: UNGROUPED_EXECUTION\n    Output[rows]\n    │   Layout: [rows:bigint]\n    │   CPU: 0.00ns (?%), Scheduled: 0.00ns (0.00%), Output: 1 row (9B)\n    │   Input avg.: 1.00 rows, Input std.dev.: 0.00%\n    └─ TableCommit[td-result:com.treasuredata.presto.connector.result.TDResultOutputTableHandle@6d8d04f7]\n       │   Layout: [rows:bigint]\n       │   CPU: 0.00ns (?%), Scheduled: 1.00ms (0.11%), Output: 1 row (9B)\n       │   Input avg.: 0.00 rows, Input std.dev.: ?%\n       └─ LocalExchange[SINGLE] ()\n          │   Layout: [partialrows:bigint, fragment:varbinary]\n          │   CPU: 0.00ns (?%), Scheduled: 0.00ns (0.00%), Output: 8 rows (1.47MB)\n          │   Input avg.: 2.00 rows, Input std.dev.: 122.47%\n          └─ RemoteSource[1]\n                 Layout: [partialrows:bigint, fragment:varbinary]\n                 CPU: 0.00ns (?%), Scheduled: 0.00ns (0.00%), Output: 8 rows (1.47MB)\n                 Input avg.: 2.00 rows, Input std.dev.: 122.47%\n\nFragment 1 [SOURCE]\n    CPU: 189.70ms, Scheduled: 821.25ms, Input: 10000 rows (4.47MB); per task: avg.: 2500.00 std.dev.: 926.65, Output: 8 rows (1.47MB)\n    Output layout: [partialrows, fragment]\n    Output partitioning: SINGLE []\n    Stage Execution Strategy: UNGROUPED_EXECUTION\n    TableWriter\n    │   Layout: [partialrows:bigint, fragment:varbinary]\n    │   CPU: 0.00ns (?%), Scheduled: 738.00ms (82.09%), Output: 8 rows (1.47MB)\n    │   Input avg.: 0.00 rows, Input std.dev.: ?%\n    │   page_type := page_type_0\n    │   td_title := td_title_1\n    │   td_browser := td_browser_2\n    │   td_color := td_color_3\n    │   td_path := td_path_4\n    │   td_ip := td_ip_5\n    │   td_version := td_version_6\n    │   td_client_id := td_client_id_7\n    │   td_os_version := td_os_version_8\n    │   td_browser_version := td_browser_version_9\n    │   td_viewport := td_viewport_10\n    │   td_charset := td_charset_11\n    │   td_os := td_os_12\n    │   td_screen := td_screen_13\n    │   td_referrer := td_referrer_14\n    │   td_url := td_url_15\n    │   td_host := td_host_16\n    │   td_language := td_language_17\n    │   td_global_id := td_global_id_18\n    │   td_foo := td_foo_19\n    │   td_platform := td_platform_20\n    │   td_user_agent := td_user_agent_21\n    │   member_id := member_id_22\n    │   goods_id := goods_id_23\n    │   category := category_24\n    │   sub_category := sub_category_25\n    │   ship_date := ship_date_26\n    │   amount := amount_27\n    │   price := price_28\n    │   _col23 := _col23_29\n    │   time := time_30\n    └─ LocalExchange[SINGLE] ()\n       │   Layout: [page_type_0:varchar, td_title_1:varchar, td_browser_2:varchar, td_color_3:varchar, td_path_4:varchar, td_ip_5:varchar, td_version_6:varchar, td_client_id_7:varchar, td_os_version_8:varchar, td_browser_version_9:varchar, td_viewport_10:varchar, td_charset_11:varchar, td_os_12:varchar, td_screen_13:varchar, td_referrer_14:varchar, td_url_15:varchar, td_host_16:varchar, td_language_17:varchar, td_global_id_18:varchar, td_foo_19:varchar, td_platform_20:varchar, td_user_agent_21:varchar, member_id_22:varchar, goods_id_23:varchar, category_24:varchar, sub_category_25:varchar, ship_date_26:varchar, amount_27:varchar, price_28:varchar, _col23_29:varchar, time_30:bigint]\n       │   CPU: 0.00ns (?%), Scheduled: 1.00ms (0.11%), Output: 10000 rows (4.47MB)\n       │   Input avg.: 2500.00 rows, Input std.dev.: 37.07%\n       └─ TableScan[td-presto:kohki_test.access_log, grouped = false]\n              Layout: [page_type_0:varchar, td_title_1:varchar, td_browser_2:varchar, td_color_3:varchar, td_path_4:varchar, td_ip_5:varchar, td_version_6:varchar, td_client_id_7:varchar, td_os_version_8:varchar, td_browser_version_9:varchar, td_viewport_10:varchar, td_charset_11:varchar, td_os_12:varchar, td_screen_13:varchar, td_referrer_14:varchar, td_url_15:varchar, td_host_16:varchar, td_language_17:varchar, td_global_id_18:varchar, td_foo_19:varchar, td_platform_20:varchar, td_user_agent_21:varchar, member_id_22:varchar, goods_id_23:varchar, category_24:varchar, sub_category_25:varchar, ship_date_26:varchar, amount_27:varchar, price_28:varchar, _col23_29:varchar, time_30:bigint]\n              CPU: 0.00ns (?%), Scheduled: 159.00ms (17.69%), Output: 10000 rows (4.47MB)\n              Input avg.: 2500.00 rows, Input std.dev.: 37.07%\n              goods_id_23 := goods_id\n              amount_27 := amount\n              td_client_id_7 := td_client_id\n              td_platform_20 := td_platform\n              td_host_16 := td_host\n              td_browser_version_9 := td_browser_version\n              category_24 := category\n              td_screen_13 := td_screen\n              sub_category_25 := sub_category\n              td_charset_11 := td_charset\n              _col23_29 := _col23\n              page_type_0 := page_type\n    td_path_4 := td_path\n              td_language_17 := td_language\n              td_title_1 := td_title\n              time_30 := time\n              td_color_3 := td_color\n              td_url_15 := td_url\n              price_28 := price\n              td_os_12 := td_os\n              td_referrer_14 := td_referrer\n              td_global_id_18 := td_global_id\n              td_user_agent_21 := td_user_agent\n              ship_date_26 := ship_date\n              td_ip_5 := td_ip\n              td_os_version_8 := td_os_version\n              td_foo_19 := td_foo\n              td_version_6 := td_version\n              td_viewport_10 := td_viewport\n              td_browser_2 := td_browser\n              member_id_22 := member_id\n\nStage-0 FINISHED driver=5 time=S(2.06ms)/B(1.75s)/C(1.24ms) mem=0B row=8/8/1\n - Task-0 FINISHED 172.18.135.193 driver=5 time=Q(380.45us)/S(2.06ms)/B(1.75s)/T(352.99ms) mem=0B row=8/8/1 gc=0/0.00ns\nStage-1 FINISHED driver=8 time=S(821.25ms)/B(318.03ms)/C(189.70ms) mem=0B row=10000/10000/8\n - Task-0 FINISHED 172.18.137.78 driver=2 time=Q(5.26ms)/S(118.71ms)/B(68.35ms)/T(152.72ms) mem=0B row=2048/2048/2 gc=0/0.00ns\n - Task-2 FINISHED 172.18.131.40 driver=2 time=Q(4.13ms)/S(246.22ms)/B(103.33ms)/T(346.72ms) mem=0B row=1808/1808/2 gc=0/0.00ns\n - Task-1 FINISHED 172.18.136.141 driver=2 time=Q(454.78us)/S(225.76ms)/B(49.77ms)/T(235.09ms) mem=0B row=4096/4096/2 gc=0/0.00ns\n - Task-3 FINISHED 172.18.132.83 driver=2 time=Q(428.87us)/S(230.56ms)/B(96.58ms)/T(336.87ms) mem=0B row=2048/2048/2 gc=0/0.00ns\n\nfinished at 2023-03-23T05:22:01Z\n",
        "stderr": null
      }
}
`)

		if err != nil {
			return
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.ShowJob(context.Background(), 123456)
	if err != nil {
		log.Fatal(err)
	}
	want := &JobDetails{
		Job: Job{
			JobId:                   "123456",
			CpuTime:                 nil,
			CreatedAt:               "2023-03-23 01:58:01 UTC",
			Duration:                1,
			EndAt:                   "2023-03-23 01:58:02 UTC",
			NumRecords:              10000,
			ResultSize:              771821,
			StartAt:                 "2023-03-23 01:58:01 UTC",
			Status:                  "success",
			UpdatedAt:               "2023-03-23 01:58:03 UTC",
			Database:                "sample_datasets",
			HiveResultSchema:        "[[\"page_type\", \"varchar\"], [\"td_title\", \"varchar\"], [\"td_browser\", \"varchar\"], [\"td_color\", \"varchar\"], [\"td_path\", \"varchar\"], [\"td_ip\", \"varchar\"], [\"td_version\", \"varchar\"], [\"td_client_id\", \"varchar\"], [\"td_os_version\", \"varchar\"], [\"td_browser_version\", \"varchar\"], [\"td_viewport\", \"varchar\"], [\"td_charset\", \"varchar\"], [\"td_os\", \"varchar\"], [\"td_screen\", \"varchar\"], [\"td_referrer\", \"varchar\"], [\"td_url\", \"varchar\"], [\"td_host\", \"varchar\"], [\"td_language\", \"varchar\"], [\"td_global_id\", \"varchar\"], [\"td_foo\", \"varchar\"], [\"td_platform\", \"varchar\"], [\"td_user_agent\", \"varchar\"], [\"member_id\", \"varchar\"], [\"goods_id\", \"varchar\"], [\"category\", \"varchar\"], [\"sub_category\", \"varchar\"], [\"ship_date\", \"varchar\"], [\"amount\", \"varchar\"], [\"price\", \"varchar\"], [\"_col23\", \"varchar\"], [\"time\", \"bigint\"]]",
			LinkedResultExportJobId: nil,
			Organization:            nil,
			Priority:                0,
			Query:                   "select * from access_log",
			Result:                  "",
			ResultExportTargetJobId: nil,
			RetryLimit:              0,
			Type:                    "presto",
			Url:                     "https://console.treasuredata.com/app/jobs/123456",
			UserName:                "user_name",
		},
		Debug: Debug{
			Cmdout: "started at 2023-03-23T05:22:00Z\npresto version: 350\nexecuting query: select * from access_log\n**\n** WARNING: time index filtering is not set on\n  - kohki_test.access_log\n** This query could be very slow as a result.\n** Please see https://docs.treasuredata.com/display/public/PD/Leveraging+Time-Based+Partitioning\n**\n2023-03-23 05:22:01 -- peak memory: 622K, queued time:00:00:00.000\n20230323_052201_54564_z5ga4                              00:00:00.570   rows  bytes bytes/sec done   total \n[0] output <- tablefinish <- exchange <- remotesource[1] FINISHED          8  1.5MB   772MB/s    5 /     5 \n [1] tablewriter <- exchange <- tablescan[*] FullScan    FINISHED     10,000  586KB   714KB/s    8 /     8 \n10000 rows.\n2023-03-23 05:22:01 -- peak memory: 622K, queued time:00:00:00.000\n20230323_052201_54564_z5ga4                              00:00:00.570   rows  bytes bytes/sec done   total \n[0] output <- tablefinish <- exchange <- remotesource[1] FINISHED          8  1.5MB   772MB/s    5 /     5 \n [1] tablewriter <- exchange <- tablescan[*] FullScan    FINISHED     10,000  586KB   714KB/s    8 /     8 \n\nQuery plan:\nFragment 0 [COORDINATOR_ONLY]\n    CPU: 1.24ms, Scheduled: 2.06ms, Input: 8 rows (1.47MB); per task: avg.: 8.00 std.dev.: 0.00, Output: 1 row (9B)\n    Output layout: [rows]\n    Output partitioning: SINGLE []\n    Stage Execution Strategy: UNGROUPED_EXECUTION\n    Output[rows]\n    │   Layout: [rows:bigint]\n    │   CPU: 0.00ns (?%), Scheduled: 0.00ns (0.00%), Output: 1 row (9B)\n    │   Input avg.: 1.00 rows, Input std.dev.: 0.00%\n    └─ TableCommit[td-result:com.treasuredata.presto.connector.result.TDResultOutputTableHandle@6d8d04f7]\n       │   Layout: [rows:bigint]\n       │   CPU: 0.00ns (?%), Scheduled: 1.00ms (0.11%), Output: 1 row (9B)\n       │   Input avg.: 0.00 rows, Input std.dev.: ?%\n       └─ LocalExchange[SINGLE] ()\n          │   Layout: [partialrows:bigint, fragment:varbinary]\n          │   CPU: 0.00ns (?%), Scheduled: 0.00ns (0.00%), Output: 8 rows (1.47MB)\n          │   Input avg.: 2.00 rows, Input std.dev.: 122.47%\n          └─ RemoteSource[1]\n                 Layout: [partialrows:bigint, fragment:varbinary]\n                 CPU: 0.00ns (?%), Scheduled: 0.00ns (0.00%), Output: 8 rows (1.47MB)\n                 Input avg.: 2.00 rows, Input std.dev.: 122.47%\n\nFragment 1 [SOURCE]\n    CPU: 189.70ms, Scheduled: 821.25ms, Input: 10000 rows (4.47MB); per task: avg.: 2500.00 std.dev.: 926.65, Output: 8 rows (1.47MB)\n    Output layout: [partialrows, fragment]\n    Output partitioning: SINGLE []\n    Stage Execution Strategy: UNGROUPED_EXECUTION\n    TableWriter\n    │   Layout: [partialrows:bigint, fragment:varbinary]\n    │   CPU: 0.00ns (?%), Scheduled: 738.00ms (82.09%), Output: 8 rows (1.47MB)\n    │   Input avg.: 0.00 rows, Input std.dev.: ?%\n    │   page_type := page_type_0\n    │   td_title := td_title_1\n    │   td_browser := td_browser_2\n    │   td_color := td_color_3\n    │   td_path := td_path_4\n    │   td_ip := td_ip_5\n    │   td_version := td_version_6\n    │   td_client_id := td_client_id_7\n    │   td_os_version := td_os_version_8\n    │   td_browser_version := td_browser_version_9\n    │   td_viewport := td_viewport_10\n    │   td_charset := td_charset_11\n    │   td_os := td_os_12\n    │   td_screen := td_screen_13\n    │   td_referrer := td_referrer_14\n    │   td_url := td_url_15\n    │   td_host := td_host_16\n    │   td_language := td_language_17\n    │   td_global_id := td_global_id_18\n    │   td_foo := td_foo_19\n    │   td_platform := td_platform_20\n    │   td_user_agent := td_user_agent_21\n    │   member_id := member_id_22\n    │   goods_id := goods_id_23\n    │   category := category_24\n    │   sub_category := sub_category_25\n    │   ship_date := ship_date_26\n    │   amount := amount_27\n    │   price := price_28\n    │   _col23 := _col23_29\n    │   time := time_30\n    └─ LocalExchange[SINGLE] ()\n       │   Layout: [page_type_0:varchar, td_title_1:varchar, td_browser_2:varchar, td_color_3:varchar, td_path_4:varchar, td_ip_5:varchar, td_version_6:varchar, td_client_id_7:varchar, td_os_version_8:varchar, td_browser_version_9:varchar, td_viewport_10:varchar, td_charset_11:varchar, td_os_12:varchar, td_screen_13:varchar, td_referrer_14:varchar, td_url_15:varchar, td_host_16:varchar, td_language_17:varchar, td_global_id_18:varchar, td_foo_19:varchar, td_platform_20:varchar, td_user_agent_21:varchar, member_id_22:varchar, goods_id_23:varchar, category_24:varchar, sub_category_25:varchar, ship_date_26:varchar, amount_27:varchar, price_28:varchar, _col23_29:varchar, time_30:bigint]\n       │   CPU: 0.00ns (?%), Scheduled: 1.00ms (0.11%), Output: 10000 rows (4.47MB)\n       │   Input avg.: 2500.00 rows, Input std.dev.: 37.07%\n       └─ TableScan[td-presto:kohki_test.access_log, grouped = false]\n              Layout: [page_type_0:varchar, td_title_1:varchar, td_browser_2:varchar, td_color_3:varchar, td_path_4:varchar, td_ip_5:varchar, td_version_6:varchar, td_client_id_7:varchar, td_os_version_8:varchar, td_browser_version_9:varchar, td_viewport_10:varchar, td_charset_11:varchar, td_os_12:varchar, td_screen_13:varchar, td_referrer_14:varchar, td_url_15:varchar, td_host_16:varchar, td_language_17:varchar, td_global_id_18:varchar, td_foo_19:varchar, td_platform_20:varchar, td_user_agent_21:varchar, member_id_22:varchar, goods_id_23:varchar, category_24:varchar, sub_category_25:varchar, ship_date_26:varchar, amount_27:varchar, price_28:varchar, _col23_29:varchar, time_30:bigint]\n              CPU: 0.00ns (?%), Scheduled: 159.00ms (17.69%), Output: 10000 rows (4.47MB)\n              Input avg.: 2500.00 rows, Input std.dev.: 37.07%\n              goods_id_23 := goods_id\n              amount_27 := amount\n              td_client_id_7 := td_client_id\n              td_platform_20 := td_platform\n              td_host_16 := td_host\n              td_browser_version_9 := td_browser_version\n              category_24 := category\n              td_screen_13 := td_screen\n              sub_category_25 := sub_category\n              td_charset_11 := td_charset\n              _col23_29 := _col23\n              page_type_0 := page_type\n    td_path_4 := td_path\n              td_language_17 := td_language\n              td_title_1 := td_title\n              time_30 := time\n              td_color_3 := td_color\n              td_url_15 := td_url\n              price_28 := price\n              td_os_12 := td_os\n              td_referrer_14 := td_referrer\n              td_global_id_18 := td_global_id\n              td_user_agent_21 := td_user_agent\n              ship_date_26 := ship_date\n              td_ip_5 := td_ip\n              td_os_version_8 := td_os_version\n              td_foo_19 := td_foo\n              td_version_6 := td_version\n              td_viewport_10 := td_viewport\n              td_browser_2 := td_browser\n              member_id_22 := member_id\n\nStage-0 FINISHED driver=5 time=S(2.06ms)/B(1.75s)/C(1.24ms) mem=0B row=8/8/1\n - Task-0 FINISHED 172.18.135.193 driver=5 time=Q(380.45us)/S(2.06ms)/B(1.75s)/T(352.99ms) mem=0B row=8/8/1 gc=0/0.00ns\nStage-1 FINISHED driver=8 time=S(821.25ms)/B(318.03ms)/C(189.70ms) mem=0B row=10000/10000/8\n - Task-0 FINISHED 172.18.137.78 driver=2 time=Q(5.26ms)/S(118.71ms)/B(68.35ms)/T(152.72ms) mem=0B row=2048/2048/2 gc=0/0.00ns\n - Task-2 FINISHED 172.18.131.40 driver=2 time=Q(4.13ms)/S(246.22ms)/B(103.33ms)/T(346.72ms) mem=0B row=1808/1808/2 gc=0/0.00ns\n - Task-1 FINISHED 172.18.136.141 driver=2 time=Q(454.78us)/S(225.76ms)/B(49.77ms)/T(235.09ms) mem=0B row=4096/4096/2 gc=0/0.00ns\n - Task-3 FINISHED 172.18.132.83 driver=2 time=Q(428.87us)/S(230.56ms)/B(96.58ms)/T(336.87ms) mem=0B row=2048/2048/2 gc=0/0.00ns\n\nfinished at 2023-03-23T05:22:01Z\n",
			Stderr: nil,
		},
	}
	if !reflect.DeepEqual(want.Job, got.Job) && !reflect.DeepEqual(want.Debug, got.Debug) {
		t.Errorf("want.Job %+v, but got.Job %+v \n want.Debug %+v but got.Debug %+v", want.Job, got.Job, want.Debug, got.Debug)
	}
}
