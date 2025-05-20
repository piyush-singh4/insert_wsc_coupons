package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/tealeg/xlsx"
)

// var fileName string = flag.String("filename", "data1.xlsx", "file name of issues script.")
var user, pass, port, host string

func init() {
	err := godotenv.Load()
	if err != nil {
		fmt.Printf("Unable to get env variables %s", err)
		return
	}
	user = os.Getenv("DB_USER")
	host = os.Getenv("DB_HOST")
	pass = os.Getenv("DB_PASSWORD")
	port = os.Getenv("DB_PORT")
	// fmt.Println("Hello")
	fmt.Println(user)
	fmt.Println(host)
	fmt.Println(pass)
	fmt.Println(port)
}
func ConnectDB() (*sql.DB, error) {
	db, errdb := sql.Open("postgres", fmt.Sprintf("dbname=db_abw user='%s' password='%s' host='%s' port=%s", user, pass, host, port))
	if errdb != nil {
		return nil, errdb
	}
	err := db.Ping()
	if err != nil {
		return nil, err
	}
	fmt.Println("Db connected")
	return db, nil
}

func ReadColumn() ([]string, error) {
	xlFile, err := xlsx.OpenFile("data1.xlsx")
	if err != nil {
		return nil, err
	}
	var data []string
	for _, sheet := range xlFile.Sheets {
		for i, row := range sheet.Rows {
			if i == 0 {
				continue // Skip header row
			}
			if len(row.Cells) > 0 {
				text := row.Cells[0].String() // Read the first column
				data = append(data, text)
			}
		}
	}
	return data, nil
}
func InsertData(db *sql.DB, values []string) error {
	// timeExpiry := time.Date(2025, time.May, 31, 0, 0, 0, 0, time.UTC)
	var partner_id string
	var offer_id string
	db.QueryRow(`SELECT partner_id FROM wsc.partners WHERE partner_name=$1`, "Metropolis").Scan(&partner_id)
	db.QueryRow(`SELECT offer_id FROM wsc.offers WHERE partner_id=$1`, partner_id).Scan(&offer_id)
	query := `INSERT INTO wsc.coupons (offer_id,created_timestamp,coupon_code) VALUES ($1,$2,$3)`
	fmt.Printf("Partner_id %s\n", partner_id)
	fmt.Printf("Offer_id %s\n", offer_id)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, value := range values {
		_, err := stmt.Exec(offer_id, time.Now(), value)
		fmt.Printf("Inserting coupon code %s\n", value)
		if err != nil {
			log.Printf("Failed to insert value '%s': %v\n", value, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	fmt.Println("All rows inserted successfully!")
	return nil
}
func main() {
	db, err := ConnectDB()
	if err != nil {
		fmt.Println("err  \n", err)
	}
	data, err := ReadColumn()
	if err != nil {
		fmt.Println("Error in fetching data", data)
	}
	// fmt.Println(data)
	err = InsertData(db, data)
	if err != nil {
		fmt.Println("Error in inserting data", err)

	}
}
