package report

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/qiniu/pandora-go-sdk/base/config"

	. "github.com/qiniu/pandora-go-sdk/base"
	. "github.com/qiniu/pandora-go-sdk/report"
)

var (
	cfg      *config.Config
	client   ReportAPI
	region   = os.Getenv("REGION")
	endpoint = os.Getenv("REPORT_HOST")
	ak       = os.Getenv("ACCESS_KEY")
	sk       = os.Getenv("SECRET_KEY")
	logger   Logger
)

func prepare() {
	var err error

	if region == "" {
		region = "nb"
	}

	if endpoint == "" {
		endpoint = "http://10.200.20.39:9996"
	}

	if ak == "" || sk == "" {
		err = fmt.Errorf("ak/sk should not be empty")
		log.Println(err)
		return
	}

	logger = NewDefaultLogger()
	cfg = NewConfig().
		WithEndpoint(endpoint).
		WithAccessKeySecretKey(ak, sk).
		WithLogger(logger).
		WithLoggerLevel(LogDebug)

	client, err = New(cfg)
	if err != nil {
		logger.Error("new report client failed, err: %v", err)
	}
}

func TestRepo(t *testing.T) {
	prepare()
	// test activate
	userInfo, err := client.ActivateUser(&UserActivateInput{})
	if err != nil {
		t.Fatal("activate user fail,err:", err)
	}
	t.Log(userInfo)

	//create database
	DatabaseName := "report_sdk_test_database"
	err = client.CreateDatabase(&CreateDatabaseInput{
		DatabaseName: DatabaseName,
		Region:       region,
	})
	if err != nil {
		t.Errorf("create database fail: ", err)
	}

	//ensure database created
	database, err := client.ListDatabases(&ListDatabasesInput{})
	if err != nil {
		t.Errorf("get repo fail: ", err)
	}
	databases := []string(*database)
	if len(databases) != 1 {
		t.Errorf("database not equal to 1")
	}
	if databases[0] != DatabaseName {
		t.Errorf("DatabaseName does not match")
	}

	// test create table
	tableName := "test_report_table"
	err = client.CreateTable(&CreateTableInput{
		DatabaseName: DatabaseName,
		TableName:    tableName,
		CMD:          fmt.Sprintf("create table %s (id TEXT);", tableName),
	})
	if err != nil {
		t.Errorf("create table fail")
	}

	// test list tables
	table, err := client.ListTables(&ListTablesInput{
		DatabaseName: DatabaseName,
	})
	if err != nil {
		t.Errorf("list table fail")
	}
	tables := []string(*table)
	if len(tables) != 1 {
		t.Errorf("list table fail")
	}
	if tables[0] != tableName {
		t.Errorf("table name not match")
	}

	tableInfo, err := client.GetTable(&GetTableInput{
		DatabaseName: DatabaseName,
		TableName:    tableName,
	})
	if err != nil {
		t.Errorf("get table fail")
	}
	tableInfoExpected := &GetTableOutput{
		Field:   "id",
		Type:    "text",
		Null:    "YES",
		Key:     nil,
		Default: nil,
	}
	if tableInfo.Field != tableInfoExpected.Field || tableInfo.Type != tableInfoExpected.Type {
		t.Errorf("get table detail not match\nexpect: %v\n got:%v\n", tableInfoExpected, tableInfo)
	}

	// test delete table
	err = client.DeleteTable(&DeleteTableInput{
		DatabaseName: DatabaseName,
		TableName:    tableName,
	})
	if err != nil {
		t.Errorf("delete table fail")
	}

	//test delete database
	err = client.DeleteDatabase(&DeleteDatabaseInput{
		DatabaseName: DatabaseName,
	})
	if err != nil {
		t.Errorf("delete repo fail: ", err)
	}
}
