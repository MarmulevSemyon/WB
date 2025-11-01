package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"produser/model"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	topic              = "my-learning-topic"
	kafkaBrokerAddress = "localhost:9093"
	count_json         = 50
)

func main() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   topic,
	})
	defer w.Close()
	for i := range count_json {
		order := genOrder(i)
		sendMsgToKafka(w, order)
	}

	fmt.Println("Producer finished.")
}
func sendMsgToKafka(w *kafka.Writer, order model.Order) {
	order_json, err := json.Marshal(order)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	log.Printf("json c id= %s сгенерирван успешно\n", order.OrderUID)
	msg := kafka.Message{
		Key:   []byte(order.OrderUID),
		Value: []byte(order_json),
		Time:  time.Now(),
	}

	err_w := w.WriteMessages(context.Background(), msg)
	if err_w != nil {
		log.Printf("Ошибка отправки сообщения в Кафку id: '%s': %v\n", order.OrderUID, err_w)
	} else {
		log.Printf("Отправленное в Кафку сообшение:\tid: '%s'\n", order.OrderUID)
	}
}

func genOrder(i int) model.Order {
	order_id := fmt.Sprintf("order-%d", i)
	order := model.Order{
		OrderUID:    order_id,
		TrackNumber: "WBILMTESTTRACK",
		Entry:       "WBIL",
		Delivery: model.Delivery{
			OrderID: order_id,
			Name:    "Test Testov",
			Phone:   "+9720000000",
			Zip:     "2639809",
			City:    "Kiryat Mozkin",
			Address: "Ploshad Mira 15",
			Region:  "Kraiot",
			Email:   "test@gmail.com",
		},
		Payment: model.Payment{
			OrderID:      order_id,
			Transaction:  fmt.Sprintf("transaction-%d", i), // UNIQUE
			RequestID:    "",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       1817,
			PaymentDT:    1637907727,
			Bank:         "alpha",
			DeliveryCost: 1500,
			GoodsTotal:   317,
			CustomFee:    0,
		},
		Items: []model.Item{{
			OrderID:     order_id,
			ChrtID:      9934930,
			TrackNumber: "WBILMTESTTRACK",
			Price:       453,
			RID:         "ab4219087a764ae0btest",
			Name:        "Mascaras",
			Sale:        30,
			Size:        "0",
			TotalPrice:  317,
			NmID:        2389212,
			Brand:       "Vivienne Sabo",
			Status:      202,
		}},
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        "test",
		DeliveryService:   "meest",
		ShardKey:          "9",
		SmID:              99,
		DateCreated:       time.Now().UTC(),
		OofShard:          "1",
	}

	return order
}
