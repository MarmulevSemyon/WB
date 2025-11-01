package main

import (
	"L0/internal/config"
	"L0/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	kafka "github.com/segmentio/kafka-go"
)

const (
	countJson = 50
)

func main() {
	cfg := config.MustLoad()
	w := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	gofakeit.Seed(time.Now().UnixNano())

	log.Printf("producer → topic=%s brokers=%v count=%d", cfg.KafkaTopic, cfg.KafkaBrokers, countJson)

	for range countJson {
		time.Sleep(500 * time.Millisecond)
		order := genOrder()
		if err := sendMsgToKafka(w, order); err != nil {
			continue
		}
	}

	fmt.Println("Producer finished.")
}
func sendMsgToKafka(w *kafka.Writer, order model.Order) error {
	orderJson, err := json.Marshal(order)
	if err != nil {
		log.Fatalf("marshal: %v", err)
		return err
	}

	log.Printf("json c id= %s сгенерирван успешно\n", order.OrderUID)
	msg := kafka.Message{
		Key:   []byte(order.OrderUID),
		Value: []byte(orderJson),
		Time:  time.Now(),
	}

	errWrite := w.WriteMessages(context.Background(), msg)
	if errWrite != nil {
		log.Printf("Ошибка отправки сообщения в Кафку id: '%s': %v\n", order.OrderUID, errWrite)
		return errWrite
	} else {
		log.Printf("Отправленное в Кафку сообшение:\tid: '%s'\n", order.OrderUID)
	}
	return nil
}
func genOrder() model.Order {
	orderID := gofakeit.UUID()
	track := fmt.Sprintf("WB%s", gofakeit.LetterN(10))

	nItems := gofakeit.Number(1, 3)
	items := make([]model.Item, 0, nItems)
	var goodsTotal int

	for j := 0; j < nItems; j++ {
		price := gofakeit.Number(200, 5000)
		sale := gofakeit.Number(0, 50)
		total := price - price*sale/100
		if total < 0 {
			total = 0
		}

		item := model.Item{
			OrderID:     orderID,
			ChrtID:      int64(gofakeit.Number(100000, 9999999)),
			TrackNumber: track,
			Price:       price,
			RID:         gofakeit.UUID(),
			Name:        gofakeit.ProductName(),
			Sale:        sale,
			Size:        gofakeit.RandomString([]string{"XS", "S", "M", "L", "XL", "0"}),
			TotalPrice:  total,
			NmID:        int64(gofakeit.Number(100000, 9999999)),
			Brand:       gofakeit.Company(),
			Status:      202,
		}
		items = append(items, item)
		goodsTotal += total
	}

	deliveryCost := gofakeit.Number(0, 2000)
	customFee := 0
	amountInt := goodsTotal + deliveryCost + customFee

	return model.Order{
		OrderUID:    orderID,
		TrackNumber: track,
		Entry:       "WBIL",

		Delivery: model.Delivery{
			OrderID: orderID,
			Name:    gofakeit.Name(),
			Phone:   gofakeit.Phone(),
			Zip:     gofakeit.Zip(),
			City:    gofakeit.City(),
			Address: gofakeit.Address().Address,
			Region:  gofakeit.State(),
			Email:   gofakeit.Email(),
		},

		Payment: model.Payment{
			OrderID:      orderID,
			Transaction:  gofakeit.UUID(),
			RequestID:    "",
			Currency:     gofakeit.RandomString([]string{"RUB", "USD", "EUR"}),
			Provider:     gofakeit.RandomString([]string{"wbpay", "bank", "visa", "mc"}),
			Amount:       float64(amountInt),
			PaymentDT:    time.Now().Unix(),
			Bank:         gofakeit.RandomString([]string{"alpha", "sber", "tinkoff", "vtb"}),
			DeliveryCost: deliveryCost,
			GoodsTotal:   goodsTotal,
			CustomFee:    customFee,
		},

		Items:             items,
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        gofakeit.Username(),
		DeliveryService:   gofakeit.RandomString([]string{"meest", "cdek", "dpd", "ups"}),
		ShardKey:          fmt.Sprintf("%d", gofakeit.Number(1, 10)),
		SmID:              gofakeit.Number(1, 200),
		DateCreated:       time.Now().UTC(),
		OofShard:          fmt.Sprintf("%d", gofakeit.Number(1, 10)),
	}
}
