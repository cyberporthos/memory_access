package main

import (
    "fmt"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "log"
    "encoding/json"
    "net/http"
    "io/ioutil"
    "errors"
)

//    _ "github.com/nakagami/firebirdsql"

func getFirebirdAccessInfo() (string, string, error) {
    // return -1, errors.New("can't work with 42")
    // user:password@servername[:port_number]/database_name_or_file (user:password@servername/foo/bar.fdb)
    // return "firebirdsql", "user:password@servername[:port_number]/database_name_or_file", nil
    return "mysql", "root:@/issintel_3_vicosa", nil
}


func getSql() ([]map[string]string, error)  {
  url := "http://claudio-mg.notainteligente.dev:3000/memory_integration"
  m   := []map[string]string{}

  resp, err := http.Get(url)
  if err != nil {
    return m, err
  }
  defer resp.Body.Close()
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

func runSqlExec(query_sql string) (string, error) {
    db_driver, access_info, err  := getFirebirdAccessInfo()
    if err != nil {
        return "", err
    }

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

func runSqlQuery(query_sql string) (string, error) {
    db_driver, access_info, err  := getFirebirdAccessInfo()
    if err != nil {
        return "", err
    }

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

    // wrapper := make(map[string]interface{})
    // wrapper["results"] = tableData

    jsonData, err := json.Marshal(wrapper)
    if err != nil {
        return "", err
    }
    return string(jsonData), nil
}

func Feedback(err error) {
  log.Fatal(err)
  // os.Exit(0)
}

func main() {

    query_sql_results, err := getSql()
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
        query_result, err = runSqlQuery(query_sql)
      } else if type_sql == "exec" {
        query_result, err = runSqlExec(query_sql)
      } else {
        Feedback(errors.New(fmt.Sprintf("type not recognized: %s", type_sql)))
      }
      if err != nil {
        Feedback(err)
      }
      fmt.Println(query_result)
    }
}
