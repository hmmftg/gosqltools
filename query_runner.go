package gosqltools

import "database/sql"

type SqlDataSource struct {
	DB *sql.DB
}

func (ds SqlDataSource) QueryRunner(querySql string, args ...interface{}) (int, []interface{}, error) {
	stmt, err := ds.DB.Prepare(querySql)
	finalRows := []interface{}{}
	errorData := map[string]interface{}{}
	if err != nil {
		errorData["step"] = "prepare"
		finalRows = append(finalRows, errorData)
		return -1, finalRows, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	if err != nil {
		errorData["step"] = "query"
		finalRows = append(finalRows, errorData)
		return -2, finalRows, err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()

	if err != nil {
		errorData["step"] = "column types"
		finalRows = append(finalRows, errorData)
		return -3, finalRows, err
	}

	count := len(columnTypes)

	for rows.Next() {

		scanArgs := make([]interface{}, count)

		for i, v := range columnTypes {

			switch v.DatabaseTypeName() {
			case "NCHAR", "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
				scanArgs[i] = new(sql.NullString)
			case "BOOL":
				scanArgs[i] = new(sql.NullBool)
			case "INT4":
				scanArgs[i] = new(sql.NullInt64)
			default:
				scanArgs[i] = new(sql.NullString)
			}
		}

		err := rows.Scan(scanArgs...)

		if err != nil {
			errorData["step"] = "column scan"
			finalRows = append(finalRows, errorData)
			return -3, finalRows, err
		}

		masterData := map[string]interface{}{}

		for i, v := range columnTypes {

			if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
				masterData[v.Name()] = z.Bool
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				masterData[v.Name()] = z.String
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				masterData[v.Name()] = z.Int64
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				masterData[v.Name()] = z.Float64
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				masterData[v.Name()] = z.Int32
				continue
			}

			masterData[v.Name()] = scanArgs[i]
		}

		finalRows = append(finalRows, masterData)
	}
	//resp, _ := json.Marshal(finalRows)
	return 0, finalRows, nil
}
