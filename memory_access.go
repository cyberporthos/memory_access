package memory_access

import (
    "fmt"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    _ "github.com/nakagami/firebirdsql"
    "log"
    "encoding/json"
    "net/http"
    "io/ioutil"
    "os"
    "bytes"
    "strconv"
)


// MEMORY_URL="http://claudio-mg.notainteligente.dev:3000/memory_integration"
// MEMORY_DB_ADAPTER="mysql"
// MEMORY_DB_URL="root:@/issintel_3_vicosa"
// MEMORY_TOKEN="123456-dev-token"

var timer_seconds int = 10
var change_timer_chan chan int = make(chan int, 1)

func GetTimerSeconds() (int, chan int) {
    return timer_seconds, change_timer_chan
}

func SetTimerSeconds(new_value int) {
    time_changed := (timer_seconds != new_value)
    timer_seconds = new_value
    if time_changed {
        change_timer_chan <- timer_seconds
    }
}

func GetFirebirdAccessInfo() (string, string) {
    // user:password@servername[:port_number]/database_name_or_file (user:password@servername/foo/bar.fdb)
    // return "firebirdsql", "user:password@servername[:port_number]/database_name_or_file", nil
    return os.Getenv("MEMORY_DB_ADAPTER"), os.Getenv("MEMORY_DB_URL")
}

// returns something like {"token":"123456-dev-token"} to be sent on requests as identification/authentication
func GetTokenAsJson() string {
    jsonData, _ := json.Marshal(struct{Token string `json:"token"`}{Token: GetToken()})
    return string(jsonData)
}

func GetToken() string {
  return os.Getenv("MEMORY_TOKEN")
}

func GetSql() ([]map[string]string, error)  {
  url := os.Getenv("MEMORY_URL")
  m   := []map[string]string{}


  var jsonToken = []byte(GetTokenAsJson())
  fmt.Println(GetTokenAsJson())
  req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonToken))
  // req.Header.Set("X-Custom-Header", "myvalue")
  req.Header.Set("Content-Type", "application/json")

  client := &http.Client{}
  resp, err := client.Do(req)
  if err != nil {
      return m, err
  }
  defer resp.Body.Close()

  // fmt.Println("response Status:", resp.Status)
  // fmt.Println("response Headers:", resp.Header)
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return m, err
  }

  err = json.Unmarshal([]byte(body), &m)
  if err != nil {
    return m, err
  }

  return m, nil
}

type ResultWrapper struct {
  Token string `json:"token"`
  QueryId uint64 `json:"query_id"`
  Results []interface{} `json:"results"`
}

type RunSqlQueryResult struct {
    LastInsertedId uint64 `json:"last_inserted_id"`
    RowsAffected  uint64 `json:"rows_affected"`
}

func RunSqlExec(query_sql string, query_id uint64) (string, error) {
    db_driver, access_info  := GetFirebirdAccessInfo()

    conn, err := sql.Open(db_driver, access_info)

    if err != nil {
        return "", err
    }
    defer conn.Close()

    query_result, err := conn.Exec(query_sql)
    if err != nil {
        return "", err
    }

    last_inserted_id, _ := query_result.LastInsertId()
    rows_affected, _ := query_result.RowsAffected()
    exec_result := RunSqlQueryResult{}
    exec_result.LastInsertedId = uint64(last_inserted_id)
    exec_result.RowsAffected = uint64(rows_affected)


    wrapper := ResultWrapper{
      Token: GetToken(),
      QueryId: query_id,
      Results: []interface{}{exec_result},
    }

    jsonData, err := json.Marshal(wrapper)
    return string(jsonData), err
}

func RunSqlQuery(query_sql string, query_id uint64) (string, error) {
    db_driver, access_info := GetFirebirdAccessInfo()

    conn, err := sql.Open(db_driver, access_info)

    if err != nil {
        return "", err
    }
    defer conn.Close()

    rows, err := conn.Query(query_sql)
    if err != nil {
        return "", err
    }
    defer rows.Close()

    // Get column names
    columns, err := rows.Columns()
    if err != nil {
        return "", err
    }

    count := len(columns)
    tableData := make([]interface{}, 0)
    values := make([]interface{}, count)
    valuePtrs := make([]interface{}, count)
    for rows.Next() {
        for i := 0; i < count; i++ {
            valuePtrs[i] = &values[i]
        }
        rows.Scan(valuePtrs...)
        entry := make(map[string]interface{})
        for i, col := range columns {
            var v interface{}
            val := values[i]
            b, ok := val.([]byte)
            if ok {
                v = string(b)
            } else {
                v = val
            }
            entry[col] = v
        }
        tableData = append(tableData, entry)
    }

    wrapper := ResultWrapper{
      Token: GetToken(),
      QueryId: query_id,
      Results: tableData,
    }

    jsonData, err := json.Marshal(wrapper)
    if err != nil {
        return "", err
    }
    return string(jsonData), nil
}

func Feedback(err error) {
  log.Println(err)
  // os.Exit(0)
}

func GetQueryId(query_sql_result map[string]string) uint64 {
    var query_id uint64

    query_id_str, has_query_id := query_sql_result["id"]
    if has_query_id == false {
        query_id = 0
        // Feedback(errors.New("has_query_id key not found"))
    } else {
        query_id_int, err := strconv.Atoi(query_id_str)
        if (err != nil) {
          query_id = 0
        } else {
          query_id = uint64(query_id_int)
        }
    }
    return query_id
}


func RunInstruction(query_sql_results []map[string]string) {
    change_timer_interval := 0
    var err error

    for _, query_sql_result := range query_sql_results {
      interval, has_interval := query_sql_result["set_interval"]
      if has_interval {
          change_timer_interval, err = strconv.Atoi(interval)
          if (err != nil) {
              change_timer_interval = 0
          }
      }
      query_sql, has_sql := query_sql_result["sql"]
      // if has_sql == false {
      //     Feedback(errors.New("sql key not found"))
      // }
      query_id := GetQueryId(query_sql_result)
      type_sql, has_type := query_sql_result["type"]
      // if has_type == false {
      //     Feedback(errors.New("type key not found"))
      // }
      var query_result string
      if has_sql && has_type && type_sql == "query" {
          query_result, err = RunSqlQuery(query_sql, query_id)
      } else if has_sql && has_type && type_sql == "exec" {
          query_result, err = RunSqlExec(query_sql, query_id)
      }
      if err != nil {
          Feedback(err)
      }
      if has_sql && has_type && query_result != "" {
          fmt.Println(query_result)
      }
    }
    if change_timer_interval > 0 {
      SetTimerSeconds(change_timer_interval)
    }
}

func Run() {
    query_sql_results, err := GetSql()
    if err != nil {
      Feedback(err)
    } else {
      RunInstruction(query_sql_results)
    }

}
