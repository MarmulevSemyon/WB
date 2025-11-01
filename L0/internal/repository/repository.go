package repository

import (
	"L0/duration"
	"L0/model"
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	Conn *pgxpool.Pool
	Cash map[string]model.Order
}

func (repo *Repository) GetOrderById(id string) (*model.Order, error) {

	defer duration.Duration(duration.Track("foo"))
	if o, ok := repo.Cash[id]; ok {
		log.Printf("Кэше есть заказ с id = %s\n", id)
		return &o, nil
	}
	log.Printf("Кэше нет заказа с id = %s, иду в бд\n", id)

	o, err := repo.TakeOrderFromDB(id)
	if err != nil {
		log.Printf("в БД нет заказа с id = %s\n", id)
		return o, err
	}

	log.Printf("в БД есть заказ с id = %s положил его в кэш\n", id)
	repo.Cash[id] = *o
	return o, nil

}
func (repo *Repository) TakeOrderFromDB(id string) (*model.Order, error) {
	var o model.Order
	// 1) начинаем read-only транзакцию с консистентным снимком
	tx, err := repo.Conn.BeginTx(context.Background(), pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(context.Background()) // безопасно: если Commit прошёл — Rollback вернёт nil

	// 2) orders
	if err := tx.QueryRow(context.Background(), `
		SELECT order_uid, track_number, entry, locale, internal_signature,
		       customer_id, delivery_service, shardkey, sm_id,
		       date_created, oof_shard
		FROM orders
		WHERE order_uid = $1
	`, id).Scan(
		&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature,
		&o.CustomerID, &o.DeliveryService, &o.ShardKey, &o.SmID,
		&o.DateCreated, &o.OofShard,
	); err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("order %q not found", id)
		}
		return nil, fmt.Errorf("select orders: %w", err)
	}

	// 3) delivery (может не существовать — на твой выбор: делать LEFT или ErrNoRows обрабатывать)
	if err := tx.QueryRow(context.Background(), `
		SELECT order_id, name, phone, zip, city, address, region, email
		FROM deliveries
		WHERE order_id = $1
	`, id).Scan(
		&o.Delivery.OrderID, &o.Delivery.Name, &o.Delivery.Phone,
		&o.Delivery.Zip, &o.Delivery.City, &o.Delivery.Address,
		&o.Delivery.Region, &o.Delivery.Email,
	); err != nil {
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("select deliveries: %w", err)
		}
		// если нет доставки — оставим нулевые значения структуры
	}

	// 4) payment (payment_dt как *time.Time)
	if err := tx.QueryRow(context.Background(), `
		SELECT order_id, transaction, request_id, currency, provider, amount, payment_dt,
		       bank, delivery_cost, goods_total, custom_fee
		FROM payments
		WHERE order_id = $1
	`, id).Scan(
		&o.Payment.OrderID, &o.Payment.Transaction, &o.Payment.RequestID,
		&o.Payment.Currency, &o.Payment.Provider, &o.Payment.Amount,
		&o.Payment.PaymentDT, &o.Payment.Bank, &o.Payment.DeliveryCost,
		&o.Payment.GoodsTotal, &o.Payment.CustomFee,
	); err != nil {
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("select payments: %w", err)
		}
	}

	// 5) items (массив)
	rows, err := tx.Query(context.Background(), `
		SELECT order_id, chrt_id, track_number, price, rid, name, sale, size,
		       total_price, nm_id, brand, status
		FROM order_items
		WHERE order_id = $1
	`, id)
	if err != nil {
		return nil, fmt.Errorf("select order_items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var it model.Item
		if err := rows.Scan(
			&it.OrderID, &it.ChrtID, &it.TrackNumber, &it.Price,
			&it.RID, &it.Name, &it.Sale, &it.Size,
			&it.TotalPrice, &it.NmID, &it.Brand, &it.Status,
		); err != nil {
			return nil, fmt.Errorf("scan order_item: %w", err)
		}
		o.Items = append(o.Items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows order_items: %w", err)
	}

	// 6) commit
	if err := tx.Commit(context.Background()); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return &o, nil
}
