package main

import "context"

type Queue interface {
	Produce(context.Context, string) error
	Consume(context.Context, ConsumerFunc) chan error
}

type ConsumerFunc func(context.Context, string) error
