package main

import (
	"fmt"
	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"
	"log"
	"os"
	"os/signal"
)

// Make a redis pool
var redisPool = &redis.Pool{
	MaxActive: 5,
	MaxIdle: 5,
	Wait: true,
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	},
}

// Make an enqueuer with a particular namespace
var enqueuer = work.NewEnqueuer("user_defined_scheduler", redisPool)

type Context struct{
	customerID int64
}

func main() {
	// Set delay for 10s
	secondsInTheFuture := int64(10)
	_, err := enqueuer.EnqueueIn("send_email", secondsInTheFuture, work.Q{"address": "danympradana@gmail.com", "subject": "hello world", "customer_id": 4})
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Request send email to customer...")
	}

	pool := work.NewWorkerPool(Context{}, 10, "user_defined_scheduler", redisPool)

	// Add middleware that will be executed for each job
	pool.Middleware((*Context).Log)
	pool.Middleware((*Context).FindCustomer)

	pool.Job("send_email", (*Context).SendEmail)

	pool.Start()

	// Wait for a signal to quit:
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan

	// Stop the pool
	pool.Stop()
}

func (c *Context) Log(job *work.Job, next work.NextMiddlewareFunc) error {
	log.Println("Starting job: ", job.Name)
	return next()
}

func (c *Context) FindCustomer(job *work.Job, next work.NextMiddlewareFunc) error {
	// If there's a customer_id param, set it in the context for future middleware and handlers to use.
	if _, ok := job.Args["customer_id"]; ok {
		c.customerID = job.ArgInt64("customer_id")
		if err := job.ArgError(); err != nil {
			return err
		}
	}

	return next()
}

func (c *Context) SendEmail(job *work.Job) error {
	// Extract arguments:
	addr := job.ArgString("address")
	subject := job.ArgString("subject")
	if err := job.ArgError(); err != nil {
		return err
	}

	log.Println(fmt.Sprintf("Send email to: %s, subject; %s", addr, subject))

	return nil
}