package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aksafarand/hwtxtdumpparser/processes"
	_ "github.com/alexbrainman/odbc"
)

func printInfo() {
	fmt.Println("HW DUMP (txt) To Access - v0.1")
	fmt.Println("Kukuh Wikartomo - 2021")
	fmt.Println("--------------------------------------------------------")
	fmt.Println("Parse HW DUMP (txt), Requires ODBC for Access Installed")
	fmt.Println("--------------------------------------------------------")
}

var resultDir = "_dumpresult"
var outputDir = "_dumpoutput"
var techNeName string

func main() {
	printInfo()
	flagPtr := flag.String("path", "", "Source Path")
	flagOtr := flag.String("out", "", "Output Path")
	flagAccess := flag.Bool("access", true, "Is Export To Access")
	flagTech := flag.String("tech", "", "Technology 2g/3g")
	flagSkippedComment := flag.Bool("skip-comment", true, "Skipped // Lines")
	flagLogOut := flag.Bool("log-out", false, "Output When Export to Access")
	flag.Parse()
	pathName := *flagPtr
	isAccess := *flagAccess
	techName := *flagTech
	skipDoubleSlash := *flagSkippedComment
	outputPath := *flagOtr
	isLogOut := *flagLogOut

	if _, err := os.Stat("./EMPTY.accdb"); os.IsNotExist(err) {
		log.Fatalf("No 'EMPTY.accdb' Found")
	}

	if pathName == "" || outputPath == "" {
		log.Fatalf("No Source Path / Output Path Provided")
	}

	if techName == "" {
		log.Fatalf("Technolofy not defined")
	}

	if strings.ToLower(techName) == "2g" {
		techNeName = "bsc"
	} else {
		techNeName = "huawei"
	}

	pathName = strings.Replace(pathName, `\`, `/`, -1)

	if err := os.MkdirAll(resultDir, 0666); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(filepath.Join(outputPath, outputDir), 0666); err != nil {
		panic(err)
	}

	// files, err := ioutil.ReadDir(pathName)
	// if err != nil {
	// 	log.Fatalf(`Error %s`, err.Error())
	// }

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

	timeStartTotal := time.Now()
	if strings.ToLower(techName) == "2g" {
		processes.MainProcess(pathName, resultDir, skipDoubleSlash, techNeName, isAccess, dbName, isLogOut)
		fmt.Println("--------------------------------------------------------")
		log.Println("Total Elapsed In", time.Since(timeStartTotal))
		if isAccess {
			log.Println("Output File In", filepath.Join(outputPath, outputDir, techName+"_CFGMML.accdb"))
		} else {
			log.Println("Output File In", filepath.Join(resultDir))
		}
		fmt.Println("--------------------------------------------------------")
	}

	if strings.ToLower(techName) == "3g" {
		processes.MainProcess(pathName, resultDir, skipDoubleSlash, techNeName, isAccess, dbName, isLogOut)
		fmt.Println("--------------------------------------------------------")
		log.Println("Total Elapsed In", time.Since(timeStartTotal))
		if isAccess {
			log.Println("Output File In", filepath.Join(outputPath, outputDir, techName+"_CFGMML.accdb"))
		} else {
			log.Println("Output File In", filepath.Join(resultDir))
		}
		fmt.Println("--------------------------------------------------------")
	}

}
