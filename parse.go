package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
)

func readCSVFile(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		return "", err
	}

	var data []string
	for _, record := range records {
		data = append(data, strings.Join(record, ","))
	}

	return strings.Join(data, "|"), nil
}
