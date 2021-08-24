package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/alexbrainman/odbc"
	"github.com/jmoiron/sqlx"
)

func printInfo() {
	fmt.Println("HW DUMP (txt) To Access - v0.0a")
	fmt.Println("Kukuh Wikartomo - 2021")
	fmt.Println("--------------------------------------------------------")
	fmt.Println("Parse HW DUMP (txt), Requires ODBC for Access Installed")
	fmt.Println("--------------------------------------------------------")
}

var resultDir = "_dumpresult"
var outputDir = "_dumpoutput"

type Table struct {
	Name      string
	Fpath     string
	Header    []string
	HeaderMap map[string]int
	File      io.WriteCloser
}

func main() {
	printInfo()
	flagPtr := flag.String("path", "", "Source Path")
	flagOtr := flag.String("out", "", "Output Path")
	flagAccess := flag.Bool("access", false, "Is Export To Access")
	flagTech := flag.String("tech", "", "Technology 2g/3g")
	flagSkippedComment := flag.Bool("skip-comment", true, "Skipped // Lines")
	flag.Parse()
	pathName := *flagPtr
	isAccess := *flagAccess
	techName := *flagTech
	skipDoubleSlash := *flagSkippedComment
	outputPath := *flagOtr

	if pathName == "" || outputPath == "" {
		log.Fatalf("No Source Path / Output Path Provided")
	}

	if techName == "" {
		log.Fatalf("Technolofy not defined")
	}

	pathName = strings.Replace(pathName, `\`, `/`, -1)

	if err := os.MkdirAll(resultDir, 0666); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(filepath.Join(outputPath, outputDir), 0666); err != nil {
		panic(err)
	}

	files, err := ioutil.ReadDir(pathName)
	if err != nil {
		log.Fatalf(`Error %s`, err.Error())
	}

	dbName := ""
	if strings.ToLower(techName) == "2g" && isAccess {
		input, err := ioutil.ReadFile(`./EMPTY.accdb`)
		if err != nil {
			log.Println(err)
			return
		}

		err = ioutil.WriteFile(filepath.Join(outputPath, outputDir, techName+"_CFGMML.accdb"), input, 0644)
		if err != nil {
			log.Println("Error creating", filepath.Join(outputPath, outputDir, techName+"_CFGMML.accdb"))
			log.Println(err)
			return
		}
		dbName = filepath.Join(outputPath, outputDir, techName+"_CFGMML.accdb")
	}

	if strings.ToLower(techName) == "3g" && isAccess {
		input, err := ioutil.ReadFile(`./EMPTY.accdb`)
		if err != nil {
			log.Println(err)
			return
		}

		err = ioutil.WriteFile(filepath.Join(outputPath, outputDir, techName+"_CFGMML.accdb"), input, 0644)
		if err != nil {
			log.Println("Error creating", filepath.Join(outputPath, outputDir, techName+"_CFGMML.accdb"))
			log.Println(err)
			return
		}
		dbName = filepath.Join(outputPath, outputDir, techName+"_CFGMML.accdb")
	}

	if strings.ToLower(techName) == "2g" {
		timeStartTotal := time.Now()
		for _, f := range files {

			timeStart := time.Now()
			fileName := strings.TrimSuffix(f.Name(), path.Ext(f.Name()))
			if strings.Contains(strings.ToLower(fileName), "bsc") && (filepath.Ext(f.Name()) == ".txt") {
				log.Println("Processing", f.Name())
				filePath := filepath.Join(pathName, f.Name())
				filePath = strings.Replace(filePath, `\`, `\\`, -1)
				neName, tables, err := ReadFile(filePath, skipDoubleSlash)
				if err != nil {
					panic(err)
				}
				for _, table := range tables {
					err := ReCreateFile(table, neName)
					if err != nil {
						log.Println("Error ReCreating File for", table.Name)
					}
				}
				if isAccess {
					err := ExportAccess(tables, dbName)
					if err != nil {
						fmt.Println(err)
					}
				}

			}

			log.Println("Done", f.Name())
			log.Println("Elapsed In", time.Since(timeStart))
			timeStart = time.Now()
		}
		fmt.Println("--------------------------------------------------------")
		log.Println("Total Elapsed In", time.Since(timeStartTotal))
		fmt.Println("--------------------------------------------------------")
	}

	if strings.ToLower(techName) == "3g" {
		timeStartTotal := time.Now()
		for _, f := range files {

			timeStart := time.Now()
			fileName := strings.TrimSuffix(f.Name(), path.Ext(f.Name()))
			if strings.Contains(strings.ToLower(fileName), "huawei") && (filepath.Ext(f.Name()) == ".txt") {
				log.Println("Processing", f.Name())
				filePath := filepath.Join(pathName, f.Name())
				filePath = strings.Replace(filePath, `\`, `\\`, -1)
				neName, tables, err := ReadFile(filePath, skipDoubleSlash)
				if err != nil {
					panic(err)
				}
				for _, table := range tables {
					err := ReCreateFile(table, neName)
					if err != nil {
						log.Println("Error ReCreating File for", table.Name)
					}
				}
				if isAccess {
					err := ExportAccess(tables, dbName)
					if err != nil {
						fmt.Println(err)
					}
				}

			}

			log.Println("Done", f.Name())
			log.Println("Elapsed In", time.Since(timeStart))
			timeStart = time.Now()
		}
		log.Println("Done")
		log.Println("Total Elapsed In", time.Since(timeStartTotal))
	}
	if err := os.RemoveAll(resultDir); err != nil {
		panic(err)
	}
}

func MakeNewTable(name string) (*Table, error) {
	fpath := filepath.Join(resultDir, name+".csv")
	// log.Println("Creating file", fpath)
	f, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}

	return &Table{
		Name:   name,
		Fpath:  fpath,
		Header: []string{"NE NAME"},
		HeaderMap: map[string]int{
			"NE NAME": 0,
		},
		File: f,
	}, nil
}

func ReCreateFile(table *Table, neName string) error {
	table.File.Close()
	content, err := os.ReadFile(table.Fpath)
	if err != nil {
		panic(err)
	}

	content = append([]byte(neName), content...)
	content = bytes.ReplaceAll(content, []byte("\n,"), []byte("\n"+neName+","))

	buffer := new(bytes.Buffer)
	buffer.Write([]byte(strings.Join(table.Header, ",") + "\n"))
	buffer.Write(content)

	if err := os.WriteFile(table.Fpath, buffer.Bytes(), 0666); err != nil {
		return err
	}
	return nil
}

func ReadFile(fName string, skipDoubleSlash bool) (string, map[string]*Table, error) {

	f, err := os.Open(fName)
	if err != nil {
		return "", nil, err
	}
	tables := make(map[string]*Table)
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
			table, err := MakeNewTable(tblName)
			if err != nil {
				return "", nil, err
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
			}

			if idx, ok := table.HeaderMap[key]; !ok {
				table.HeaderMap[key] = len(table.Header)
				table.Header = append(table.Header, key)
				row = append(row, val)
			} else {
				row[idx] = val
			}
		}
		if _, err := table.File.Write([]byte(strings.Join(row, ",") + "\n")); err != nil {
			return "", nil, err
		}
	}
	return neName, tables, nil
}

func ExportAccess(tables map[string]*Table, dbName string) error {
	pvd := fmt.Sprintf("DRIVER=Microsoft Access Driver (*.mdb, *.accdb);UID=admin;DBQ=%s;", dbName)
	db, err := sqlx.Open("odbc", pvd)
	if err != nil {
		log.Println("open db error ", err.Error())
		return err
	}
	defer db.Close()
	for _, table := range tables {
		qry := fmt.Sprintf(`SELECT file.* INTO [%s] FROM [Text;FMT=Delimited(,);HDR=YES;DATABASE=%s].[%s] as file`, table.Name, `C:/Users/kwx1036441/Documents/Weekly/go/parseDump/result`, table.Name+`.csv`)
		_, err := db.Exec(qry)
		if err != nil {
			// log.Println("Error Inserting", table.Name, "Retry With Text Data Type")
			createTableCol := []string{}

			for _, s := range table.Header {
				createTableCol = append(createTableCol, fmt.Sprintf(`[%s] longtext`, s))
			}

			newQry := fmt.Sprintf(`CREATE TABLE [%s] (%s)`, table.Name, strings.Join(createTableCol, ","))
			_, _ = db.Exec(newQry)
			// if err != nil {
			// 	// log.Println("Error Creating Table", table.Name, "Maybe Already Exists, Trying to Insert Values")
			// }
			qry := fmt.Sprintf(`INSERT INTO [%s] SELECT * FROM [Text;FMT=Delimited(,);HDR=YES;DATABASE=%s].[%s] as file`, table.Name, `C:/Users/kwx1036441/Documents/Weekly/go/parseDump/result`, table.Name+`.csv`)
			_, err = db.Exec(qry)
			if err != nil {
				// log.Println("Error Inserting", table.Name, " Skipping")
				continue
			}
			// rowsInserted, _ := tx.RowsAffected()
			// log.Printf(`Inserted %s row(s) to [%s]`, strconv.Itoa(int(rowsInserted)), table.Name)
			continue

		}

		// rowsInserted, _ := tx.RowsAffected()
		// log.Printf(`Inserted %s row(s) to [%s]`, strconv.Itoa(int(rowsInserted)), table.Name)
	}
	return nil
}
