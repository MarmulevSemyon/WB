package main

import (
	"L0/internal/config"
	"L0/internal/httpapi"
	"L0/internal/model"
	"context"
	"encoding/json"
	"errors"
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

type task struct {
	value []byte
	key   []byte

	msg kafka.Message // исходное Kafka-сообщение для коммита
}

func main() {

	cfg := config.MustLoad()
	// канал для завершения канала чтения из кафки
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM) // для сигнала от системы о звершении

	///////////////////////настройка консьюмера
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,

		CommitInterval: 0, // 0 = коммитим вручную через CommitMessages

		// Таймауты сессии и ребалансов
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  30 * time.Second,
	})

	defer r.Close()
	log.Printf("Консьюмер подписан на топик '%s' в группе '%s'\n\n", cfg.KafkaTopic, cfg.KafkaGroupID)
	ctx, cancel := context.WithCancel(context.Background())

	////////////////настройка бд
	configPg, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		log.Fatal("Ошибка конфигурации пула: ", err)
	}

	configPg.MaxConns = 10 // Устанавливаем максимальное число соединений в пуле
	configPg.MinConns = 2
	configPg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, configPg)
	if err != nil {
		log.Fatal("Ошибка создания пула: ", err)
	}
	defer pool.Close()
	log.Println("Пул соединений успешно настроен")

	//////////////////параллельно пишем в бд
	tasks := make(chan task, cfg.QueueSize)
	acks := make(chan kafka.Message, cfg.QueueSize)
	var wg sync.WaitGroup

	wg.Add(cfg.Workers)
	for i := range cfg.Workers {
		go func(id int) {
			defer wg.Done()
			for t := range tasks {
				log.Printf("горутина %d приступает к парсингу сообщения с id = %s:\n", id, t.key)
				ctxDb, cancelDb := context.WithTimeout(ctx, cfg.RequestTimeout)
				err := parsJsonToDB(ctxDb, pool, t.value)
				cancelDb()
				if err != nil {
					log.Printf("ошибка записи cooбщения id = %s в бд: %v\n", t.key, err)
					continue
				}
				acks <- t.msg
			}
		}(i + 1)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-acks:
				if !ok {
					return
				}
				if err := r.CommitMessages(ctx, m); err != nil {
					log.Printf("ошибка CommitMessages: %v", err)
				}
			}
		}
	}()
	/////////////читаем из кафки и кладем в канал
	wg.Add(1)
	go readMsgs(ctx, r, tasks, &wg)

	wg.Add(1)
	go httpapi.Run(pool, cfg.HTTPAddr)
	wg.Done()

	<-signalChan // пришел сигнал о завершении

	fmt.Println("закрываю канал")

	close(tasks) // воркеры
	close(acks)  // коммиттер

	fmt.Println("отменяю контекст") // reader/fetch
	cancel()

	fmt.Println("закрываю горутины")
	wg.Wait()

	fmt.Println("стопаю сигн")
	signal.Stop(signalChan)

}

func readMsgs(ctx context.Context, r *kafka.Reader, out chan<- task, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			log.Printf("ошибка FetchMessage из Kafka: %v", err)
			continue
		}

		if !json.Valid(m.Value) {
			log.Printf("пропуск невалидного JSON: partition=%d offset=%d", m.Partition, m.Offset)
			if err := r.CommitMessages(ctx, m); err != nil {
				log.Printf("ошибка CommitMessages (bad json): %v", err)
			}
			continue
		}

		out <- task{value: m.Value, key: m.Key, msg: m}
	}
}

func parsJsonToDB(ctx context.Context, pool *pgxpool.Pool, data []byte) error {
	var o model.Order

	// парсинг JSON
	if err := json.Unmarshal(data, &o); err != nil {
		return fmt.Errorf("bad JSON: %w", err)
	}
	log.Printf("JSON id = %s успешно распаршен\n", o.OrderUID)

	if err := o.Validate(); err != nil {
		log.Printf("невалидный заказ (%s): %v", o.OrderUID, err)
		return fmt.Errorf("validate JSON failed: %w", err)
	}
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
