package httpapi

import (
	"L0/internal/repository"
	"L0/internal/util"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct {
	repo repository.Repository
}

var orderTmpl = template.Must(template.New("order").Funcs(template.FuncMap{
	"fmtTime": func(t time.Time) string {
		if t.IsZero() {
			return "-"
		}
		return t.Local().Format("02.01.2006 15:04:05")
	},
	"money": func(v any) string {
		switch x := v.(type) {
		case int:
			return fmt.Sprintf("%d", x)
		case int64:
			return fmt.Sprintf("%d", x)
		case float64:
			return fmt.Sprintf("%.2f", x)
		default:
			return fmt.Sprintf("%v", x)
		}
	},
}).Parse(`
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Заказ {{.OrderUID}}</title>
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:24px;color:#222}
  .card{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:16px;margin:12px 0;box-shadow:0 1px 2px rgba(0,0,0,.04)}
  h1{margin:0 0 8px 0}
  h2{margin:16px 0 8px 0;font-size:18px}
  table{border-collapse:collapse;width:100%}
  th,td{border:1px solid #e5e7eb;padding:8px;text-align:left}
  th{background:#f8fafc}
  .grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
  .kv{display:flex;justify-content:space-between;gap:12px}
  .muted{color:#6b7280}
  .pill{display:inline-block;padding:2px 8px;border-radius:999px;background:#eef2ff;color:#3730a3;font-size:12px}
  a{color:#2563eb;text-decoration:none}
</style>
</head>
<body>

<div class="card">
  <h1>Заказ <span class="pill">{{.OrderUID}}</span></h1>
  <div class="muted">Создан: {{fmtTime .DateCreated}}</div>
</div>

<div class="grid">
  <div class="card">
    <h2>Основное</h2>
    <div class="kv"><span>Track</span><strong>{{.TrackNumber}}</strong></div>
    <div class="kv"><span>Entry</span><strong>{{.Entry}}</strong></div>
    <div class="kv"><span>Locale</span><strong>{{.Locale}}</strong></div>
    <div class="kv"><span>Customer</span><strong>{{.CustomerID}}</strong></div>
    <div class="kv"><span>Delivery service</span><strong>{{.DeliveryService}}</strong></div>
    <div class="kv"><span>SM ID</span><strong>{{.SmID}}</strong></div>
  </div>

  <div class="card">
    <h2>Доставка</h2>
    <div class="kv"><span>Имя</span><strong>{{.Delivery.Name}}</strong></div>
    <div class="kv"><span>Телефон</span><strong>{{.Delivery.Phone}}</strong></div>
    <div class="kv"><span>Email</span><strong>{{.Delivery.Email}}</strong></div>
    <div class="kv"><span>Адрес</span><strong>{{.Delivery.City}}, {{.Delivery.Address}}</strong></div>
    <div class="kv"><span>Регион / ZIP</span><strong>{{.Delivery.Region}} / {{.Delivery.Zip}}</strong></div>
  </div>

  <div class="card">
    <h2>Оплата</h2>
    <div class="kv"><span>Транзакция</span><strong>{{.Payment.Transaction}}</strong></div>
    <div class="kv"><span>Сумма</span><strong>{{money .Payment.Amount}}</strong></div>
    <div class="kv"><span>Валюта</span><strong>{{.Payment.Currency}}</strong></div>
    <div class="kv"><span>Провайдер</span><strong>{{.Payment.Provider}}</strong></div>
    <div class="kv"><span>Банк</span><strong>{{.Payment.Bank}}</strong></div>
    <div class="kv"><span>Доставка / Товары / Пошлина</span>
      <strong>{{.Payment.DeliveryCost}} / {{.Payment.GoodsTotal}} / {{.Payment.CustomFee}}</strong>
    </div>
  </div>
</div>

<div class="card">
  <h2>Товары ({{len .Items}})</h2>
  <table>
    <thead>
      <tr>
        <th>RID</th><th>Название</th><th>Бренд</th><th>Цена</th>
        <th>Скидка</th><th>Итого</th><th>Размер</th><th>Status</th>
      </tr>
    </thead>
    <tbody>
    {{range .Items}}
      <tr>
        <td>{{.RID}}</td>
        <td>{{.Name}}</td>
        <td>{{.Brand}}</td>
        <td>{{.Price}}</td>
        <td>{{.Sale}}</td>
        <td>{{.TotalPrice}}</td>
        <td>{{.Size}}</td>
        <td>{{.Status}}</td>
      </tr>
    {{end}}
    </tbody>
  </table>
</div>

<div class="muted">
  <a href="/form">← назад к форме</a>
</div>

</body>
</html>
`))

func (h *Handler) order(w http.ResponseWriter, r *http.Request) {
	defer util.Duration(util.Track("order"))
	orderId := r.URL.Query().Get("id")
	if orderId == "" {
		http.Error(w, "нет введённого id", http.StatusBadRequest)
		return
	}
	log.Printf("Начинаю показывать заказ с id = %s\n", orderId)

	order, ok, err := h.repo.GetOrderById(r.Context(), orderId)
	if err != nil {
		log.Printf("внутренняя ошибка при поиске заказа %s: %v", orderId, err)
		http.Error(w, "внутренняя ошибка", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "заказ не найден", http.StatusNotFound)
		return
	}

	j, err := json.Marshal(order)
	if err != nil {
		log.Printf("bad order struct with id = %s error: %v", orderId, err)
	}
	w.Write(j)
}

func (h *Handler) form(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case http.MethodGet:
		http.ServeFile(w, r, "internal/httpapi/form.html")

	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			http.Error(w, "ошибка чтения формы", http.StatusBadRequest)
			return
		}
		id := r.FormValue("orderId")
		if id == "" {
			http.Error(w, "не указан id заказа", http.StatusBadRequest)
			return
		}

		o, ok, err := h.repo.GetOrderById(r.Context(), id)
		if err != nil {
			log.Printf("внутренняя ошибка при поиске заказа %s: %v", id, err)
			http.Error(w, "внутренняя ошибка", http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "заказ не найден", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := orderTmpl.Execute(w, o); err != nil {
			http.Error(w, "template error", http.StatusInternalServerError)
			return
		}

	default:
		http.Error(w, "метод не поддерживается", http.StatusMethodNotAllowed)
	}
}

func Run(pool *pgxpool.Pool, addr string) {
	some := Handler{
		repo: repository.New(pool),
	}

	http.HandleFunc("/order", some.order)
	http.HandleFunc("/form", some.form)

	log.Printf("начинаю слушать localhost:%s", addr)

	if err := http.ListenAndServe(addr, nil); err != nil && err != http.ErrServerClosed {
		log.Printf("http server error: %v", err)
	}
}
