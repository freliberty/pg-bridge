package models

type Order struct {
	id          int
	total       int
	state       string
	items_total int
	currency    string
	locale      string
}

func GetOrder(orderId int) (Order, error) {
	order := Order{}
	err := db.Get(
		&order,
		"SELECT id, total, state, items_total, currency, locale FROM orders WHERE id=$1",
		orderId)

	return order, err
}
