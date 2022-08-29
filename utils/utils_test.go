package utils

import (
	"informer/redis"
	"testing"
)

func TestNewClient(t *testing.T) {
	client := NewClient("localhost:6379", "mima", 0)
	redisManager := redis.NewRedisManager(client)
	redisManager.HMset("1562588391084695552/pod/data", "namespace/podname", "{ \"firstName\": \"Brett\", \"lastName\":\"McLaughlin\", \"email\": \"brett@newInstance.com\" }")
}
