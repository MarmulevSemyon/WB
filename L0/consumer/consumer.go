package main

import (
	"L0/client"
	"L0/model"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	kafka "github.com/segmentio/kafka-go"
)

const (
	topic         = "my-learning-topic"
	brokerAddress = "localhost:9093"
	groupID       = "my-learning-go-group"

	host      = "127.0.0.1"
	port      = 5432
	user      = "l0"
	password  = "L0"
	dbname    = "l0_wb"
	workers   = 4
	queueSize = 4
)

type task struct {
	value []byte
	key   []byte
}

func main() {
	// канал для завершения канала чтения из кафки
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM) // для сигнала от системы о звершении

	///////////////////////настройка консьюмера
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: groupID,
	})
	defer r.Close()
	log.Printf("Консьюмер подписан на топик '%s' в группе '%s'\n\n", topic, groupID)
	ctx, cancel := context.WithCancel(context.Background())

	////////////////настройка бд
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatal("Ошибка конфигурации пула: ", err)
	}
	config.MaxConns = 10 // Устанавливаем максимальное число соединений в пуле
	config.MinConns = 2
	config.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Fatal("Ошибка создания пула: ", err)
	}
	defer pool.Close()
	log.Println("Пул соединений успешно настроен")

	//////////////////параллельно пишем в бд
	tasks := make(chan task, queueSize)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(id int) {
			defer wg.Done()
			for t := range tasks {
				log.Printf("горутина %d приступает к парсингу сообщения с id = %s:\n", id, t.key)

				err := parsJsonToBd(ctx, pool, t.value)
				if err != nil {
					log.Printf("ошибка записи cooбщения id = %s в бд: %v\n", t.key, err)
				}
			}
		}(i + 1)
	}

	/////////////читаем из кафки и кладем в канал
	wg.Add(1)
	go readMsgs(ctx, r, tasks, &wg)
	wg.Add(1)

	go client.Run(pool)
	wg.Done()
	<-signalChan // пришел сигнал о завершении
	fmt.Println("закрываю канал")
	close(tasks)
	fmt.Println("отменяю контекст")
	cancel()
	fmt.Println("закрываю горутины")
	wg.Wait()
	fmt.Println("стопаю сигн")
	signal.Stop(signalChan)

}

func readMsgs(ctx context.Context, r *kafka.Reader, out chan<- task, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		default:
		}

		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("ошибка чтения из Kafka: %v\n", err)
			continue
		}
		log.Printf("Сообщение key = %s прочитано из Kafka:\n", m.Key)

		if !json.Valid(m.Value) {
			log.Printf("пропуск невалидного JSON: partition=%d offset=%d", m.Partition, m.Offset)
			continue
		}

		out <- task{value: m.Value, key: m.Key}

	}

}

func parsJsonToBd(ctx context.Context, pool *pgxpool.Pool, data []byte) error {
	var o model.Order

	// парсинг JSON
	if err := json.Unmarshal(data, &o); err != nil {
		return fmt.Errorf("bad JSON: %w", err)
	}
	log.Printf("JSON id = %s успешно распаршен\n", o.OrderUID)

	// дочерним таблицам проставляем id
	o.Delivery.OrderID = o.OrderUID
	o.Payment.OrderID = o.OrderUID
	for i := range o.Items {
		o.Items[i].OrderID = o.OrderUID
	}
	log.Printf("Дочерним таблицам проставлен id = %s\n", o.OrderUID)

	//начало транзакции
	tx, err := pool.Begin(ctx)
	if err != nil {

		return err
	}
	defer tx.Rollback(ctx)
	log.Printf("\t Начата транзакция для id = %s\n", o.OrderUID)

	//  UPSERT orders по order_uid
	if _, err := tx.Exec(ctx, `
		INSERT INTO orders (
			order_uid, track_number, entry, locale, internal_signature,
			customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`,
		o.OrderUID, o.TrackNumber, o.Entry, o.Locale, o.InternalSignature,
		o.CustomerID, o.DeliveryService, o.ShardKey, o.SmID, o.DateCreated, o.OofShard,
	); err != nil {
		return fmt.Errorf("orders upsert: %w", err)
	}
	log.Printf("\tСообщение с id = %s (orders upsert) отправлено в бд\n", o.OrderUID)

	// UPSERT deliveries (1:1, нужен UNIQUE(order_id))
	if _, err := tx.Exec(ctx, `
		INSERT INTO deliveries (order_id, name, phone, zip, city, address, region, email)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`,
		o.OrderUID, o.Delivery.Name, o.Delivery.Phone, o.Delivery.Zip, o.Delivery.City,
		o.Delivery.Address, o.Delivery.Region, o.Delivery.Email,
	); err != nil {
		return fmt.Errorf("deliveries upsert: %w", err)
	}
	log.Printf("Сообщение с id = %s (deliveries upsert) отправлено в бд\n", o.OrderUID)

	// UPSERT payments по UNIQUE(transaction)
	if _, err := tx.Exec(ctx, `
		INSERT INTO payments (
			order_id, transaction, request_id, currency, provider, amount, payment_dt,
			bank, delivery_cost, goods_total, custom_fee
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`,
		o.OrderUID, o.Payment.Transaction, o.Payment.RequestID, o.Payment.Currency, o.Payment.Provider,
		o.Payment.Amount, o.Payment.PaymentDT, o.Payment.Bank, o.Payment.DeliveryCost, o.Payment.GoodsTotal, o.Payment.CustomFee,
	); err != nil {
		return fmt.Errorf("payments upsert: %w", err)
	}
	log.Printf("Сообщение с id = %s (payments upsert) отправлено в бд\n", o.OrderUID)

	// UPSERT order_items по UNIQUE(order_id, chrt_id, rid)
	for _, it := range o.Items {
		if _, err := tx.Exec(ctx, `
			INSERT INTO order_items (
				order_id, chrt_id, track_number, price, rid, name, sale, size,
				total_price, nm_id, brand, status
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		`,
			o.OrderUID, it.ChrtID, it.TrackNumber, it.Price, it.RID, it.Name, it.Sale, it.Size,
			it.TotalPrice, it.NmID, it.Brand, it.Status,
		); err != nil {
			return fmt.Errorf("items upsert: %w", err)
		}
	}
	log.Printf("Сообщение с id = %s (items upsert) отправлено в бд\n", o.OrderUID)

	// commit
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	// конец транзакции
	log.Printf("\tТранзакция для id = %s завершена\n", o.OrderUID)
	return nil
}
