package memory_access

import (
    "fmt"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    _ "github.com/cyberporthos/firebirdsql"
    "log"
    "encoding/json"
    "net/http"
    "io/ioutil"
    "os"
    "bytes"
    "strconv"
    "golang.org/x/text/encoding"
    "golang.org/x/text/encoding/charmap"
)


var timer_seconds  int = 10
var not_running   bool = true
var feedback_mode bool = true
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

func GetInstructions(post_data_as_json string) ([]map[string]string, error)  {
  url := os.Getenv("MEMORY_URL")
  m   := []map[string]string{}

  if feedback_mode {
    fmt.Println(post_data_as_json)
  }

  req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(post_data_as_json)))
  req.Header.Set("Content-Type", "application/json")

  client := &http.Client{}
  resp, err := client.Do(req)
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
  Token string `json:"token"`
  QueryId uint64 `json:"query_id"`
  Results []interface{} `json:"results"`
}

type RunSqlQueryResult struct {
    LastInsertedId uint64 `json:"last_inserted_id"`
    RowsAffected  uint64 `json:"rows_affected"`
}

type RunSqlErrorResult struct {
    Error string `json:"error"`
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

func ClientCharset() string {
    return os.Getenv("FB_CLIENT_CHARSET")
}

func ChartMap() encoding.Encoding {
    switch ClientCharset() {
    default:
        return nil
    case "CodePage437":
        return charmap.CodePage437
    case "CodePage850":
        return charmap.CodePage850
    case "CodePage852":
        return charmap.CodePage852
    case "CodePage855":
        return charmap.CodePage855
    case "CodePage858":
        return charmap.CodePage858
    case "CodePage860":
        return charmap.CodePage860
    case "CodePage862":
        return charmap.CodePage862
    case "CodePage863":
        return charmap.CodePage863
    case "CodePage865":
        return charmap.CodePage865
    case "CodePage866":
        return charmap.CodePage866
    case "ISO8859_1":
        return charmap.ISO8859_1
    case "ISO8859_2":
        return charmap.ISO8859_2
    case "ISO8859_3":
        return charmap.ISO8859_3
    case "ISO8859_4":
        return charmap.ISO8859_4
    case "ISO8859_5":
        return charmap.ISO8859_5
    case "ISO8859_6":
        return charmap.ISO8859_6
    case "ISO8859_6E":
        return charmap.ISO8859_6E
    case "ISO8859_6I":
        return charmap.ISO8859_6I
    case "ISO8859_7":
        return charmap.ISO8859_7
    case "ISO8859_8":
        return charmap.ISO8859_8
    case "ISO8859_8E":
        return charmap.ISO8859_8E
    case "ISO8859_8I":
        return charmap.ISO8859_8I
    case "ISO8859_10":
        return charmap.ISO8859_10
    case "ISO8859_13":
        return charmap.ISO8859_13
    case "ISO8859_14":
        return charmap.ISO8859_14
    case "ISO8859_15":
        return charmap.ISO8859_15
    case "ISO8859_16":
        return charmap.ISO8859_16
    case "KOI8R":
        return charmap.KOI8R
    case "KOI8U":
        return charmap.KOI8U
    case "Macintosh":
        return charmap.Macintosh
    case "MacintoshCyrillic":
        return charmap.MacintoshCyrillic
    case "Windows874":
        return charmap.Windows874
    case "Windows1250":
        return charmap.Windows1250
    case "Windows1251":
        return charmap.Windows1251
    case "Windows1252":
        return charmap.Windows1252
    case "Windows1253":
        return charmap.Windows1253
    case "Windows1254":
        return charmap.Windows1254
    case "Windows1255":
        return charmap.Windows1255
    case "Windows1256":
        return charmap.Windows1256
    case "Windows1257":
        return charmap.Windows1257
    case "Windows1258":
        return charmap.Windows1258
    }
}

func ConvertToCharsetIfRequired(utf_str string) string {
    if ClientCharset() == "" {
        return utf_str
    }

    utf_str_writer_buffer := new(bytes.Buffer)
    encoder_writer        := ChartMap().NewEncoder().Writer(utf_str_writer_buffer)

    fmt.Fprintf(encoder_writer, utf_str)

    return fmt.Sprintf("%s", utf_str_writer_buffer.Bytes())
}

func RunSqlQuery(query_sql string, query_id uint64) (string, error) {
    db_driver, access_info := GetFirebirdAccessInfo()

    conn, err := sql.Open(db_driver, access_info)

    if err != nil {
        return "", err
    }
    defer conn.Close()

    query_sql_converted := ConvertToCharsetIfRequired(query_sql)

    rows, err := conn.Query(query_sql_converted)
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
  if feedback_mode {
    log.Println(err)
  }
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

func GetEmptyQueryResult(query_id uint64) string {
    empty_array := make([]interface{}, 0)

    result_wrapper := ResultWrapper{ Token: GetToken(), QueryId: query_id, Results: empty_array }
    empty_query_result, _ := json.Marshal(result_wrapper)
    return string(empty_query_result)
}

func GetErrorQueryResult(query_id uint64, e error) string {
    error_result := RunSqlErrorResult{}
    error_result.Error = e.Error()

    error_array := []interface{}{error_result}

    result_wrapper         := ResultWrapper{ Token: GetToken(), QueryId: query_id, Results: error_array }
    result_wrapper_json, _ := json.Marshal(result_wrapper)
    return string(result_wrapper_json)
}

func RunInstruction(query_sql_results []map[string]string) []string {
    var change_timer_interval int = 0
    var err error
    var query_results []string

    for _, query_sql_result := range query_sql_results {
      interval, has_interval := query_sql_result["set_interval"]
      if has_interval {
          converted_int, convert_err := strconv.Atoi(interval)
          if (convert_err != nil) {
              change_timer_interval = 0
          } else {
            change_timer_interval   = converted_int
          }
      }
      query_sql, has_sql := query_sql_result["sql"]
      query_id := GetQueryId(query_sql_result)
      type_sql, has_type := query_sql_result["type"]
      var query_result string
      if has_sql && has_type && type_sql == "query" {
          query_result, err = RunSqlQuery(query_sql, query_id)
      } else if has_sql && has_type && type_sql == "exec" {
          query_result, err = RunSqlExec(query_sql, query_id)
      }
      if has_sql && has_type && query_result != "" {
        query_results = append(query_results, query_result)
      } else if query_id > 0 {
        if err != nil {
          query_results = append(query_results, GetErrorQueryResult(query_id, err))
        } else {
          query_results = append(query_results, GetEmptyQueryResult(query_id))
        }
      }
    }

    if change_timer_interval > 0 {
      SetTimerSeconds(change_timer_interval)
    }

    return query_results
}

func RunWith(post_data string) {
    query_sql_results, err := GetInstructions(post_data)
    if err != nil {
      Feedback(err)
    } else {
      query_results := RunInstruction(query_sql_results)
      for _, query_result := range query_results {
        RunWith(query_result)
      }
      // when we are posting results, the server returns no instructions
      // so the if below ensures that it only runs when we have received instructions, evaluated all
      // of them and sent theirs results, so we immediately ask for new instructions
      if (len(query_results) > 0) {
        RunWith(GetTokenAsJson())
      }
    }
}

func NotRunning() bool {
  return not_running
}

func Run() {
    if NotRunning() {
        not_running = false
        RunWith(GetTokenAsJson())
        not_running = true
    }
}
func SetNoFeedback() {
  feedback_mode = false
}
