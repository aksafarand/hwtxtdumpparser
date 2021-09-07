package processes

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	logStd "log"

	"github.com/aksafarand/hwtxtdumpparser/structs"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

func MainProcess(sourceDir string, resultDir string, skipDoubleSlash bool, techNeName string, isAccess bool, dbName string, isLogOut bool) {
	tables := make(map[string]*structs.Table)

	files, err := ioutil.ReadDir(sourceDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if path.Ext(file.Name()) == ".txt" && strings.Contains(strings.ToLower(file.Name()), techNeName) {
			fullName := filepath.Join(sourceDir, file.Name())
			logStd.Println(fullName)
			f, err := os.Open(fullName)
			if err != nil {
				panic(err)
			}
			neName := ""
			scanner := bufio.NewScanner(f)
			it := 1
			for scanner.Scan() {
				it++
				if it < 10 {
					continue
				}
				line := scanner.Text()
				if strings.TrimSpace(line) == "" {
					continue
				}

				if line[:2] == "//" && skipDoubleSlash {
					continue
				}

				if !skipDoubleSlash {
					line = strings.ReplaceAll(scanner.Text(), "//", "")
				}

				arrStr := strings.Split(line, ":")
				if len(arrStr) < 2 {
					continue
				}

				tblName := strings.TrimSpace(arrStr[0])
				if len(tblName) < 1 {
					continue
				}

				if _, ok := tables[tblName]; !ok {
					table, err := MakeNewTable(tblName, resultDir)
					if err != nil {
						panic(err)
					}

					tables[tblName] = table
				}

				table := tables[tblName]
				arrStr[1] = strings.ReplaceAll(arrStr[1], ";", "")
				keyVals := strings.Split(arrStr[1], ",")
				row := make([]string, len(table.Header))
				for _, kv := range keyVals {
					keyVal := strings.Split(kv, "=")
					key := strings.TrimSpace(keyVal[0])
					val := ""
					if len(keyVal) > 1 {
						val = keyVal[1]
					}

					if key == "SYSOBJECTID" {

						neName = val
						if len(table.Buffer.String()) > 0 {
							content := append([]byte(neName+","), table.Buffer.Bytes()...)
							content = bytes.ReplaceAll(content, []byte("\n,"), []byte("\n"+neName+","))
							if _, err := table.File.Write(content); err != nil {
								panic(err)
							}
						}
						table.Buffer.Reset()
					}

					if idx, ok := table.HeaderMap[key]; !ok {
						table.HeaderMap[key] = int64(len(table.Header))
						table.Header = append(table.Header, key)
						row = append(row, val)

					} else {
						row[idx] = val
					}

					if neName == "" {
						for _, r := range row {
							table.Buffer.WriteString(r)
						}
					}
				}

				if neName != "" {
					content := append([]byte(neName), []byte(strings.Join(row, ",")+"\n")...)
					if _, err := table.File.Write(content); err != nil {
						panic(err)
					}
				}
			}

		}

	}
	for _, table := range tables {
		table.File.Close()
		content, err := os.ReadFile(table.Fpath)
		if err != nil {
			panic(err)
		}

		buffer := new(bytes.Buffer)
		buffer.Write([]byte(strings.Join(table.Header, ",") + "\n"))
		buffer.Write(content)

		if err := os.WriteFile(table.Fpath, buffer.Bytes(), 0666); err != nil {
			panic(err)
		}
	}

	if isAccess {
		ExportAccess(tables, dbName, resultDir, isLogOut)

		if err := os.RemoveAll(resultDir); err != nil {
			panic(err)
		}
	}
}

func MakeNewTable(name string, resultDir string) (*structs.Table, error) {
	fpath := filepath.Join(resultDir, name+".csv")
	f, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}

	return &structs.Table{
		Name:   name,
		Fpath:  fpath,
		Header: []string{"NE NAME"},
		HeaderMap: map[string]int64{
			"NE NAME": 0,
		},
		Buffer: new(bytes.Buffer),
		File:   f,
	}, nil
}

func ExportAccess(tables map[string]*structs.Table, dbName string, resultDir string, isLogOut bool) {
	pvd := fmt.Sprintf(`DRIVER=Microsoft Access Driver (*.mdb, *.accdb);UID=admin;DBQ=%s;`, dbName)
	db, err := sqlx.Open("odbc", pvd)
	if err != nil && isLogOut {
		log.Errorf("open db %s err %s", dbName, err.Error())
		return
	}
	defer db.Close()
	for _, table := range tables {
		qry := fmt.Sprintf(`SELECT file.* INTO [%s] FROM [Text;FMT=Delimited(,);HDR=YES;DATABASE=%s].[%s] as file`, table.Name, resultDir, table.Name+`.csv`)
		if isLogOut {
			log.Info(qry)
		}
		tx, err := db.Exec(qry)
		if err != nil && isLogOut {
			log.Warnf("Error Inserting %s Retry With Text Data Type", table.Name)
			createTableCol := []string{}

			for _, s := range table.Header {
				createTableCol = append(createTableCol, fmt.Sprintf(`[%s] longtext`, s))
			}

			newQry := fmt.Sprintf(`CREATE TABLE [%s] (%s)`, table.Name, strings.Join(createTableCol, ","))
			_, _ = db.Exec(newQry)
			if isLogOut {
				log.Info(qry)
			}
			if err != nil && isLogOut {
				log.Warnf("Error Creating Table %s Maybe Already Exists, Trying to Insert Values", table.Name)
			}
			qry := fmt.Sprintf(`INSERT INTO [%s] SELECT * FROM [Text;FMT=Delimited(,);HDR=YES;DATABASE=%s].[%s] as file`, table.Name, resultDir, table.Name+`.csv`)
			tx, err = db.Exec(qry)
			if isLogOut {
				log.Info(qry)
			}
			if err != nil && isLogOut {
				log.Error("Error Inserting %s - Skipping", table.Name)
				continue
			}
			if isLogOut {
				rowsInserted, _ := tx.RowsAffected()
				log.Infof(`Inserted %s row(s) to [%s]`, strconv.FormatInt(rowsInserted, 10), table.Name)
			}

			continue

		}
		if isLogOut {
			rowsInserted, _ := tx.RowsAffected()
			log.Infof(`Inserted %s row(s) to [%s]`, strconv.FormatInt(rowsInserted, 10), table.Name)
		}
	}
}
