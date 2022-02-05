package gosqltools

import (
	"database/sql"
	"os"
	"testing"
)

// TestHelloName calls greetings.Hello with a name, checking
// for a valid return value.
func TestQueryToStruct(t *testing.T) {
	db, err := sql.Open("oracle", os.Getenv("TEST_DB_URL"))
	if err != nil {
		t.Fatalf(`Open("") = %v, want "", error`, err)
	}

	ds := SqlDataSource{DB: db}
	type Sysdate struct {
		Sysdate string `json:"Sysdate" db:"SYSDATE"`
	}
	var sysdate Sysdate
	msg, results, err := ds.QueryToStruct("SELECT SYSDATE FROM DUAL WHERE 1 = :1", sysdate, "2")
	if msg != 0 || err != nil {
		t.Fatalf(`QueryToStruct("") = %d, %v, want "", error`, msg, err)
	}
	t.Log(results)
}
