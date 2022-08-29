package utils

import (
	redis "github.com/go-redis/redis/v8"
)

// create redis client
func NewClient(addr, password string, db int) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return rdb
}
