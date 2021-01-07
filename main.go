package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"sync"
	// "time"

	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	DB *gorm.DB
	wg sync.WaitGroup
)

func ConnectDB() error {
	viper.SetConfigFile("config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("Cannot load config %s", err)
	}

	username := viper.GetString("database_username")
	password := viper.GetString("database_password")
	dbName := viper.GetString("database_name")
	dbHost := viper.GetString("database_host")
	dbPort := viper.GetString("database_port")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		username, password, dbHost, dbPort, dbName)
	var err error
	
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			LogLevel:      logger.Info, 
			Colorful:      true,   
		},
	)
	
	DB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: newLogger,
	})

	return err
}

func UpdateRecord(firstId int, secondId int, transactionId int) {

	defer wg.Done()
	
	DB.Transaction(func(tx *gorm.DB) error {
	
		if err := tx.Exec("Select 1 from seats where id = ? FOR UPDATE", firstId).Error; err != nil {
			log.Println(err)
			return err
		}

		fmt.Printf("Transaction %d locked record with ID = %d \n", transactionId, firstId)

		if err := tx.Exec("Update seats SET occupied = 1 where id = ? ", firstId).Error; err != nil {
			log.Println(err)
			return err
		}

		time.Sleep(1 * time.Second)

		// if err := tx.Exec("Select 1 from seats where id = ? FOR UPDATE", secondId).Error; err != nil {
		// 	log.Println(err)
		// 	return err
		// }

		fmt.Printf("Transaction %d start to access record with ID = %d \n", transactionId, secondId)

		if err := tx.Exec("Update seats SET occupied = 1 where id = ? ", secondId).Error; err != nil {
			log.Println(err)
			return err
		}
		return nil
	})
}

func main() {
	err := ConnectDB()

	if err != nil {
		log.Fatalf("Cannot connect to datbase: %v", err)
		return
	}

	wg.Add(2)

	go UpdateRecord(6, 7, 1)
	go UpdateRecord(7, 6, 2)

	wg.Wait()
}
