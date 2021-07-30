package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"strconv"
)

var amqpURI = fmt.Sprintf("amqp://%s:%s@%s:%d/", "rabbitmq", "rabbitmq", "localhost", 5672)

func main() {
	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()

	go func() {
		fmt.Printf("closing: %s", <-connection.NotifyClose(make(chan *amqp.Error)))
	}()

	channel, err := connection.Channel()
	if err != nil {
		log.Fatal(err)
	}

	if err := channel.ExchangeDeclare(
		"user-defined-scheduler", // name
		"direct",                 // type
		true,                     // durable
		false,                    // auto-deleted
		false,                    // internal
		false,                    // noWait
		nil,                      // arguments
	); err != nil {
		log.Fatal(err)
	}

	expirationString := strconv.Itoa(int(10000))
	if err = channel.Publish(
		"",                   // publish to an exchange
		"send-email-delayed", // routing to 0 or more queues
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte("Body of Message"),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Expiration:      expirationString,
		},
	); err != nil {
		log.Fatal(err)
	} else {
		log.Println("Message published to queue...")
	}

	_, err = channel.QueueDeclare(
		"send-email-delayed", // name of the queue
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // noWait
		amqp.Table{
			"x-dead-letter-exchange":    "user-defined-scheduler",
			"x-dead-letter-routing-key": "send-email-topic",
		}, // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	queueConsumed, err := channel.QueueDeclare(
		"send-email", // name of the queue
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	if err = channel.QueueBind(
		queueConsumed.Name,       // name of the queue
		"send-email-topic",       // bindingKey
		"user-defined-scheduler", // sourceExchange
		false,                    // noWait
		nil,                      // arguments
	); err != nil {
		log.Fatal(err)
	}

	deliveries, err := channel.Consume(
		queueConsumed.Name,                // name
		"user-defined-scheduler-consumer", // consumerTag,
		false,                             // noAck
		false,                             // exclusive
		false,                             // noLocal
		false,                             // noWait
		nil,                               // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan error)
	go handle(deliveries, done)

	err = <-done
	log.Println("Message finish to be processed")
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		log.Printf(
			"got %dB delivery: [%v] %q",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)
		d.Ack(false)
	}
	log.Printf("handle: deliveries channel closed")
	done <- nil
}
