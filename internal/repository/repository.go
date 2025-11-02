package repository

import (
	"L0/internal/model"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("order not found")

// Интерфейсы — пригодятся для тестов/моков и хэндлеров.
type OrderReader interface {
	GetOrderByID(ctx context.Context, id string) (model.Order, bool, error)
}

type OrderWriter interface {
	// Опционально, если будешь писать в БД через репозиторий
	SaveOrder(ctx context.Context, o model.Order) error
}

// Базовая реализация.
type Repository struct {
	Conn *pgxpool.Pool

	mu   sync.RWMutex
	Cash map[string]model.Order // map[order_uid]Order
}

func New(conn *pgxpool.Pool) Repository {
	return Repository{
		Conn: conn,
		Cash: make(map[string]model.Order),
	}
}

func (r *Repository) GetOrderById(ctx context.Context, id string) (model.Order, bool, error) {
	r.mu.RLock()
	if o, ok := r.Cash[id]; ok {
		r.mu.RUnlock()
		log.Printf("Кэше есть заказ с id = %s\n", id)
		return o, true, nil
	}
	r.mu.RUnlock()

	log.Printf("Кэше нет заказа с id = %s, иду в бд\n", id)

	o, err := r.TakeOrderFromDB(ctx, id)
	if err != nil {
		return model.Order{}, false, err
	}
	if o == nil {
		return model.Order{}, false, ErrNotFound
	}

	log.Printf("в БД есть заказ с id = %s положил его в кэш\n", id)

	r.mu.Lock()
	r.Cash[id] = *o
	r.mu.Unlock()

	return *o, true, nil
}
func (repo *Repository) TakeOrderFromDB(ctx context.Context, id string) (*model.Order, error) {
	var o model.Order

	tx, err := repo.Conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	//orders
	if err := tx.QueryRow(ctx, `
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

	// delivery
	if err := tx.QueryRow(ctx, `
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
	}

	//  payment
	if err := tx.QueryRow(ctx, `
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

	// items (мб не 1)
	rows, err := tx.Query(ctx, `
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

	// сommit
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return &o, nil
}
