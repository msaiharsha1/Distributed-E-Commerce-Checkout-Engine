// Distributed E-Commerce Checkout Engine
// Normally this needs Kafka, Postgres, and Redis installed.

// This copies how an online checkout works:
// 1. Reserve the item in stock
// 2. Charge the card
// 3. Ship it
// If any step fails, undo the earlier steps (give stock back, etc).
// This undo-on-failure idea is called the Saga pattern.
//
//
// Run:
//   go run main.go
//
// Then test it (in a second terminal):
//   curl -X POST localhost:8080/checkout -d '{"user_id":"u1","item":"sku-42","qty":2,"amount":59.99}'
//   curl localhost:8080/status/<order_id>
//
// Note: ~1 in 5 orders fail on purpose, so you can see the undo logic work.

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ---------- domain types ----------

type OrderStatus string

const (
	StatusPending    OrderStatus = "PENDING"
	StatusReserved   OrderStatus = "INVENTORY_RESERVED"
	StatusPaid       OrderStatus = "PAYMENT_CAPTURED"
	StatusShipped    OrderStatus = "SHIPPED"
	StatusCompleted  OrderStatus = "COMPLETED"
	StatusFailed     OrderStatus = "FAILED"
	StatusRolledBack OrderStatus = "ROLLED_BACK"
)

type Order struct {
	ID        string      `json:"id"`
	UserID    string      `json:"user_id"`
	Item      string      `json:"item"`
	Qty       int         `json:"qty"`
	Amount    float64     `json:"amount"`
	Status    OrderStatus `json:"status"`
	FailedAt  string      `json:"failed_at,omitempty"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// events that flow across our "kafka" bus
type Event struct {
	Topic   string
	OrderID string
	Data    map[string]any
}

// ---------- fake kafka: just a fan-out pub/sub over channels ----------

type EventBus struct {
	mu   sync.RWMutex
	subs map[string][]chan Event
}

func NewEventBus() *EventBus {
	return &EventBus{subs: make(map[string][]chan Event)}
}

func (b *EventBus) Subscribe(topic string) <-chan Event {
	ch := make(chan Event, 32)
	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], ch)
	b.mu.Unlock()
	return ch
}

func (b *EventBus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs[e.Topic] {
		// don't block the publisher if a consumer is slow
		select {
		case ch <- e:
		default:
			log.Printf("[bus] dropped event on topic %s, subscriber too slow", e.Topic)
		}
	}
}

// ---------- fake postgres: map + mutex, good enough for a demo ----------

type OrderStore struct {
	mu     sync.RWMutex
	orders map[string]*Order
}

func NewOrderStore() *OrderStore {
	return &OrderStore{orders: make(map[string]*Order)}
}

func (s *OrderStore) Save(o *Order) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o.UpdatedAt = time.Now()
	s.orders[o.ID] = o
}

func (s *OrderStore) Get(id string) (*Order, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.orders[id]
	return o, ok
}

// ---------- fake redis: cache + distributed lock ----------
// tracking hits/misses just so we can print a "reduced db load" number like
// you'd want to brag about in a standup

type RedisLike struct {
	mu     sync.Mutex
	cache  map[string]*Order
	locks  map[string]bool
	hits   int
	misses int
}

func NewRedisLike() *RedisLike {
	return &RedisLike{
		cache: make(map[string]*Order),
		locks: make(map[string]bool),
	}
}

func (r *RedisLike) TryLock(orderID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.locks[orderID] {
		return false
	}
	r.locks[orderID] = true
	return true
}

func (r *RedisLike) Unlock(orderID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.locks, orderID)
}

func (r *RedisLike) CacheOrder(o *Order) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *o
	r.cache[o.ID] = &cp
}

func (r *RedisLike) GetCached(orderID string) (*Order, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o, ok := r.cache[orderID]
	if ok {
		r.hits++
	} else {
		r.misses++
	}
	return o, ok
}

func (r *RedisLike) HitRate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := r.hits + r.misses
	if total == 0 {
		return 0
	}
	return float64(r.hits) / float64(total) * 100
}

// ---------- the "microservices" ----------
// each one just listens to its topic and publishes a result event.
// failures are randomly injected so the saga has something to actually undo.

type PaymentService struct {
	bus *EventBus
}

func (p *PaymentService) Run() {
	in := p.bus.Subscribe("payment.charge")
	for e := range in {
		time.Sleep(50 * time.Millisecond) // pretend to talk to a payment gateway
		if rand.Float64() < 0.15 {
			p.bus.Publish(Event{Topic: "payment.failed", OrderID: e.OrderID, Data: e.Data})
			continue
		}
		p.bus.Publish(Event{Topic: "payment.captured", OrderID: e.OrderID, Data: e.Data})
	}
}

type InventoryService struct {
	bus   *EventBus
	mu    sync.Mutex
	stock map[string]int
}

func NewInventoryService(bus *EventBus) *InventoryService {
	return &InventoryService{
		bus: bus,
		// small stock on purpose so you can also see "out of stock" style failures
		stock: map[string]int{"sku-42": 50, "sku-7": 3, "sku-99": 100},
	}
}

func (inv *InventoryService) Run() {
	in := inv.bus.Subscribe("inventory.reserve")
	for e := range in {
		time.Sleep(30 * time.Millisecond)
		item, _ := e.Data["item"].(string)
		qty := 1
		if q, ok := e.Data["qty"].(int); ok {
			qty = q
		}

		inv.mu.Lock()
		ok := inv.stock[item] >= qty && rand.Float64() > 0.1 // small injected flakiness too
		if ok {
			inv.stock[item] -= qty
		}
		inv.mu.Unlock()

		if !ok {
			inv.bus.Publish(Event{Topic: "inventory.failed", OrderID: e.OrderID, Data: e.Data})
			continue
		}
		inv.bus.Publish(Event{Topic: "inventory.reserved", OrderID: e.OrderID, Data: e.Data})
	}
}

// compensation - give the stock back if payment fails after we already reserved it
func (inv *InventoryService) Release(item string, qty int) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.stock[item] += qty
}

type ShippingService struct {
	bus *EventBus
}

func (s *ShippingService) Run() {
	in := s.bus.Subscribe("shipping.create")
	for e := range in {
		time.Sleep(20 * time.Millisecond)
		s.bus.Publish(Event{Topic: "shipping.created", OrderID: e.OrderID, Data: e.Data})
	}
}

// ---------- saga orchestrator ----------
// order of operations: reserve inventory -> capture payment -> create shipment
// if payment fails after inventory was reserved, we release the stock back.
// that's the compensating transaction part of the saga pattern.

type Saga struct {
	bus   *EventBus
	store *OrderStore
	cache *RedisLike
	inv   *InventoryService
	done  map[string]chan OrderStatus
	mu    sync.Mutex
}

func NewSaga(bus *EventBus, store *OrderStore, cache *RedisLike, inv *InventoryService) *Saga {
	s := &Saga{
		bus:   bus,
		store: store,
		cache: cache,
		inv:   inv,
		done:  make(map[string]chan OrderStatus),
	}
	go s.listen()
	return s
}

func (s *Saga) waiter(orderID string) chan OrderStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan OrderStatus, 1)
	s.done[orderID] = ch
	return ch
}

func (s *Saga) notify(orderID string, status OrderStatus) {
	s.mu.Lock()
	ch, ok := s.done[orderID]
	if ok {
		delete(s.done, orderID)
	}
	s.mu.Unlock()
	if ok {
		ch <- status
	}
}

func (s *Saga) listen() {
	reserved := s.bus.Subscribe("inventory.reserved")
	invFailed := s.bus.Subscribe("inventory.failed")
	paid := s.bus.Subscribe("payment.captured")
	payFailed := s.bus.Subscribe("payment.failed")
	shipped := s.bus.Subscribe("shipping.created")

	for {
		select {
		case e := <-reserved:
			s.update(e.OrderID, StatusReserved)
			s.bus.Publish(Event{Topic: "payment.charge", OrderID: e.OrderID, Data: e.Data})

		case e := <-invFailed:
			s.update(e.OrderID, StatusFailed)
			s.notify(e.OrderID, StatusFailed)

		case e := <-paid:
			s.update(e.OrderID, StatusPaid)
			s.bus.Publish(Event{Topic: "shipping.create", OrderID: e.OrderID, Data: e.Data})

		case e := <-payFailed:
			// compensate: give the inventory back since payment didn't go through
			item, _ := e.Data["item"].(string)
			qty := 1
			if q, ok := e.Data["qty"].(int); ok {
				qty = q
			}
			s.inv.Release(item, qty)
			s.update(e.OrderID, StatusRolledBack)
			s.notify(e.OrderID, StatusRolledBack)

		case e := <-shipped:
			s.update(e.OrderID, StatusCompleted)
			s.notify(e.OrderID, StatusCompleted)
		}
	}
}

func (s *Saga) update(orderID string, status OrderStatus) {
	o, ok := s.store.Get(orderID)
	if !ok {
		return
	}
	o.Status = status
	s.store.Save(o)
	s.cache.CacheOrder(o)
}

// kick off a checkout - blocks until the saga finishes (or times out)
func (s *Saga) Start(o *Order) OrderStatus {
	s.store.Save(o)
	s.cache.CacheOrder(o)
	wait := s.waiter(o.ID)

	s.bus.Publish(Event{
		Topic:   "inventory.reserve",
		OrderID: o.ID,
		Data:    map[string]any{"item": o.Item, "qty": o.Qty},
	})

	select {
	case final := <-wait:
		return final
	case <-time.After(5 * time.Second):
		return StatusFailed
	}
}

// ---------- http layer ----------

type Server struct {
	store *OrderStore
	cache *RedisLike
	saga  *Saga
	lock  *RedisLike // reusing RedisLike for the distributed lock too
}

func genOrderID() string {
	return fmt.Sprintf("ord_%d_%d", time.Now().UnixNano(), rand.Intn(1000))
}

type checkoutReq struct {
	UserID string  `json:"user_id"`
	Item   string  `json:"item"`
	Qty    int     `json:"qty"`
	Amount float64 `json:"amount"`
}

func (srv *Server) handleCheckout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req checkoutReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Qty <= 0 {
		req.Qty = 1
	}

	order := &Order{
		ID:        genOrderID(),
		UserID:    req.UserID,
		Item:      req.Item,
		Qty:       req.Qty,
		Amount:    req.Amount,
		Status:    StatusPending,
		CreatedAt: time.Now(),
	}

	// grab the distributed lock so two requests for the same order id can't race
	// (in a real system this is per-user or idempotency-key based, not per-order-id,
	// since the order id doesn't exist yet - but the mechanism is the same)
	if !srv.lock.TryLock(order.ID) {
		http.Error(w, "order already processing, try again", http.StatusConflict)
		return
	}
	defer srv.lock.Unlock(order.ID)

	final := srv.saga.Start(order)

	o, _ := srv.store.Get(order.ID)
	o.Status = final
	srv.store.Save(o)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(o)
}

func (srv *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/status/")
	if id == "" {
		http.Error(w, "missing order id", http.StatusBadRequest)
		return
	}

	// check cache first, only fall back to the "db" on a miss
	if o, ok := srv.cache.GetCached(id); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		json.NewEncoder(w).Encode(o)
		return
	}

	o, ok := srv.store.Get(id)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	srv.cache.CacheOrder(o)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	json.NewEncoder(w).Encode(o)
}

func (srv *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "cache hit rate: %.1f%%\n", srv.cache.HitRate())
}

// ---------- main ----------

func main() {
	rand.Seed(time.Now().UnixNano())

	bus := NewEventBus()
	store := NewOrderStore()
	cache := NewRedisLike()
	lockRedis := NewRedisLike()

	inv := NewInventoryService(bus)
	payments := &PaymentService{bus: bus}
	shipping := &ShippingService{bus: bus}

	go inv.Run()
	go payments.Run()
	go shipping.Run()

	saga := NewSaga(bus, store, cache, inv)

	srv := &Server{store: store, cache: cache, saga: saga, lock: lockRedis}

	http.HandleFunc("/checkout", srv.handleCheckout)
	http.HandleFunc("/status/", srv.handleStatus)
	http.HandleFunc("/stats", srv.handleStats)

	addr := ":8080"
	log.Printf("checkout engine listening on %s", addr)
	log.Printf("try: curl -X POST localhost%s/checkout -d '{\"user_id\":\"u1\",\"item\":\"sku-42\",\"qty\":2,\"amount\":59.99}'", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
