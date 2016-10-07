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
    "errors"
    "os"
    "bytes"
)


// MEMORY_URL="http://claudio-mg.notainteligente.dev:3000/memory_integration"
// MEMORY_DB_ADAPTER="mysql"
// MEMORY_DB_URL="root:@/issintel_3_vicosa"
// MEMORY_TOKEN="123456-dev-token"

func GetFirebirdAccessInfo() (string, string) {
    // user:password@servername[:port_number]/database_name_or_file (user:password@servername/foo/bar.fdb)
    // return "firebirdsql", "user:password@servername[:port_number]/database_name_or_file", nil
    return os.Getenv("MEMORY_DB_ADAPTER"), os.Getenv("MEMORY_DB_URL")
}

// returns something like {"token":"123456-dev-token"} to be sent on requests as identification/authentication
func GetTokenAsJson() string {
    jsonData, _ := json.Marshal(struct{Token string `json:"token"`}{Token: os.Getenv("MEMORY_TOKEN")})
    return string(jsonData)
}

func GetSql() ([]map[string]string, error)  {
  url := os.Getenv("MEMORY_URL")
  m   := []map[string]string{}


  var jsonToken = []byte(GetTokenAsJson())
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
  Results []interface{} `json:"results"`
}

type RunSqlQueryResult struct {
    LastInsertedId uint64 `json:"last_inserted_id"`
    RowsAffected  uint64 `json:"rows_affected"`
}

func RunSqlExec(query_sql string) (string, error) {
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


    wrapper := ResultWrapper{Results: []interface{}{exec_result}}

    jsonData, err := json.Marshal(wrapper)
    return string(jsonData), err
}

func RunSqlQuery(query_sql string) (string, error) {
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

    wrapper := ResultWrapper{Results: tableData}

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

func Run() {
    query_sql_results, err := GetSql()
    if err != nil {
      Feedback(err)
    }

    for _, query_sql_result := range query_sql_results {
      query_sql, ok := query_sql_result["sql"]
      if ok == false {
        Feedback(errors.New("sql key not found"))
      }
      type_sql, ok := query_sql_result["type"]
      if ok == false {
        Feedback(errors.New("type key not found"))
      }
      var query_result string
      if type_sql == "query" {
        query_result, err = RunSqlQuery(query_sql)
      } else if type_sql == "exec" {
        query_result, err = RunSqlExec(query_sql)
      } else {
        Feedback(errors.New(fmt.Sprintf("type not recognized: %s", type_sql)))
      }
      if err != nil {
        Feedback(err)
      }
      fmt.Println(query_result)
    }
}
