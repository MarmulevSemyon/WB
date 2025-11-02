package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// HTTP
	HTTPAddr string // ":8081"

	// Kafka
	KafkaBrokers []string // ["localhost:9092"]
	KafkaTopic   string   // "orders"
	KafkaGroupID string   // "orders-consumer"

	// PostgreSQL
	PostgresDSN string // "postgres://user:pass@localhost:5432/l0?sslmode=disable"

	// Пулы/воркеры/каналы
	Workers        int           // 4
	QueueSize      int           // 100
	RequestTimeout time.Duration // 5s для внешних вызовов, если нужно

	// Кеш/предзагрузка
	CacheWarmupLimit int // 1000 (сколько заказов грузить в память при старте)
}

// helper: строка → int с дефолтом
func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// helper: строка → duration с дефолтом
func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// helper: строка → []string (через запятую) с тримом
func envCSV(key string, def []string) []string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		items := strings.Split(v, ",")
		out := make([]string, 0, len(items))
		for _, it := range items {
			s := strings.TrimSpace(it)
			if s != "" {
				out = append(out, s)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return def
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:         getEnv("HTTP_ADDR", ":8081"),
		KafkaBrokers:     envCSV("KAFKA_BROKERS", []string{"localhost:9093"}), //"localhost:9093"
		KafkaTopic:       getEnv("KAFKA_TOPIC", "my-learning-topic"),          //"my-learning-topic"
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "my-learning-go-group"),    //"my-learning-go-group"
		PostgresDSN:      getEnv("POSTGRES_DSN", "postgres://l0:L0@localhost:5432/l0_wb?sslmode=disable"),
		Workers:          envInt("WORKERS", 4),
		QueueSize:        envInt("QUEUE_SIZE", 100),
		RequestTimeout:   envDuration("REQUEST_TIMEOUT", 5*time.Second),
		CacheWarmupLimit: envInt("CACHE_WARMUP_LIMIT", 1000),
	}

	// Базовая проверка обязательных полей (если нужно)
	if len(cfg.KafkaBrokers) == 0 {
		return cfg, errors.New("KAFKA_BROKERS is empty")
	}
	if cfg.KafkaTopic == "" {
		return cfg, errors.New("KAFKA_TOPIC is empty")
	}
	if cfg.PostgresDSN == "" {
		return cfg, errors.New("POSTGRES_DSN is empty")
	}
	return cfg, nil
}

func MustLoad() Config {
	cfg, err := Load()
	if err != nil {
		panic(err)
	}
	return cfg
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
