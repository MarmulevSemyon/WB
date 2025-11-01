package model

import (
	"errors"
	"fmt"
	"regexp"
)

func (o *Order) Validate() error {
	if o.OrderUID == "" {
		return errors.New("order_uid is empty")
	}
	if o.TrackNumber == "" {
		return errors.New("track_number is empty")
	}
	if o.CustomerID == "" {
		return errors.New("customer_id is empty")
	}
	if o.Payment.Amount < 0 {
		return fmt.Errorf("payment.amount is negative: %v", o.Payment.Amount)
	}
	if o.Payment.Transaction == "" {
		return errors.New("payment.transaction is empty")
	}
	if o.Delivery.Email != "" && !isValidEmail(o.Delivery.Email) {
		return fmt.Errorf("invalid email: %s", o.Delivery.Email)
	}
	for i, item := range o.Items {
		if item.Price < 0 {
			return fmt.Errorf("item[%d].price negative", i)
		}
		if item.TrackNumber == "" {
			return fmt.Errorf("item[%d].track_number empty", i)
		}
	}
	return nil
}

// очень простая проверка email
var emailRe = regexp.MustCompile(`^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`)

func isValidEmail(email string) bool {
	return emailRe.MatchString(email)
}
