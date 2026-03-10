package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"

	"feedprovider/config"
	"feedprovider/models"
)

func main() {
	csvPath := flag.String("file", "instruments.csv", "path to instruments csv file")
	flag.Parse()

	config.LoadConfig()
	config.ConnectDatabase()
	defer func() {
		if err := config.CloseDatabase(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}()

	if err := config.DB.AutoMigrate(&models.Instrument{}); err != nil {
		log.Fatalf("failed to run migration for instruments: %v", err)
	}

	var existingCount int64
	if err := config.DB.Model(&models.Instrument{}).Count(&existingCount).Error; err != nil {
		log.Fatalf("failed to check existing instruments: %v", err)
	}

	if existingCount > 0 {
		fmt.Printf("Skipping import: instruments already present in DB (count=%d).\n", existingCount)
		return
	}

	resolvedPath, err := filepath.Abs(*csvPath)
	if err != nil {
		log.Fatalf("failed to resolve csv path: %v", err)
	}

	result, err := models.ImportInstrumentsFromCSV(config.DB, resolvedPath)
	if err != nil {
		log.Fatalf("failed to import instruments: %v", err)
	}

	fmt.Printf("Import completed from %s\n", resolvedPath)
	fmt.Printf("Total rows read: %d\n", result.TotalRows)
	fmt.Printf("Imported rows:   %d\n", result.Imported)
	fmt.Printf("Skipped rows:    %d\n", result.Skipped)
}
