package models

import "time"

type OrderStatus string

const (
	StatusPending OrderStatus = "pending"
	StatusConfirmed OrderStatus = "confirmed"
	StatusFailed OrderStatus = "failed"
)

type Order struct {
	ID 			string 		`json:"id"`
	UserID 		string 		`json:"user_id"`
	Item 		string 		`json:"item"`
	Quantity 	int 		`json:"quantity"`
	Amount 		float64 	`json:"amount"`
	Status 		OrderStatus `json:"status"`
	CreatedAt 	time.Time 	`json:"created_at"`
}

