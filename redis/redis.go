package redis

import (
	"context"

	redis "github.com/go-redis/redis/v8"
)

//create redis client
func NewClient(addr, password string, db int) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return rdb
}

type RedisManager struct {
	cache *redis.Client
}

func NewRedisManager(addr, password string, db int) *RedisManager {
	var cache *redis.Client = NewClient(addr, password, db)
	return &RedisManager{
		cache: cache,
	}
}

// HASH SET
// key: hash key
// field: value's key
// value: data
func (r *RedisManager) HMSet(key string, field string, value interface{}) bool {
	result := r.cache.HMSet(context.TODO(), key, field, value)
	return result.Val()
}

// HASH GET field
func (r *RedisManager) HMGet(key string, field string) []interface{} {
	result := r.cache.HMGet(context.TODO(), key, field)
	return result.Val()
}

// HASH GET LEN
func (r *RedisManager) HLen(key string) int64 {
	result := r.cache.HLen(context.TODO(), key)
	return result.Val()
}

// HASH GET ALL
func (r *RedisManager) HGetAll(key string) map[string]string {
	result := r.cache.HGetAll(context.TODO(), key)
	return result.Val()
}

// HASH GET values
func (r *RedisManager) HVals(key string) []string {
	result := r.cache.HVals(context.TODO(), key)
	return result.Val()
}

// HASH GET fields
func (r *RedisManager) HKeys(key string) []string {
	result := r.cache.HKeys(context.TODO(), key)
	return result.Val()
}

// HASH HDel
func (r *RedisManager) HDel(key string, field string) int64 {
	result := r.cache.HDel(context.TODO(), key, field)
	return result.Val()
}

// get key
func (r *RedisManager) Keys(key string) []string {
	result := r.cache.Keys(context.TODO(), key)
	return result.Val()
}

// DEL KEY
func (r *RedisManager) Del(keys ...string) int64 {
	result := r.cache.Del(context.TODO(), keys...)
	return result.Val()
}

// SET ADD
func (r *RedisManager) SAdd(key string, members ...interface{}) bool {
	result := r.cache.SAdd(context.TODO(), key, members...)
	return result.Val() != 0
}

// SET GET
func (r *RedisManager) SMembers(key string) []string {
	result := r.cache.SMembers(context.TODO(), key)
	return result.Val()
}

// SET DELETE
func (r *RedisManager) SRem(key string, members ...interface{}) int64 {
	result := r.cache.SRem(context.TODO(), key, members...)
	return result.Val()
}

func TestHMset() {
	// client := NewClient("localhost:6379", "mima", 0)
	// redisManager := NewRedisManager(client)
	// //redisManager.HMset("1562588391084695552/pod/data", "namespace/podname", "{ \"firstName\": \"Brett\", \"lastName\":\"McLaughlin\", \"email\": \"brett@newInstance.com\" }")
	// redisManager.HMSet("1562588391084695552/pod", // k8s集群所有pod数据，相当于集合名称
	// 	"namespace1/podname1", "json string111....") //以namespace/podname为field value为json串
	// //"namespace2/podname2", "json string222....", // 相当于集合元素，元素即为key-value键值对
	// //"namespace3/podname3", "json string333....")
	// //HMGET 1562588391084695552/pod namespace1/podname1  获取数据 HMGET key field value
}

func TestSAdd() {
	// client := NewClient("localhost:6379", "mima", 0)
	// redisManager := NewRedisManager(client)
	// //redisManager.HMset("1562588391084695552/pod/data", "namespace/podname", "{ \"firstName\": \"Brett\", \"lastName\":\"McLaughlin\", \"email\": \"brett@newInstance.com\" }")
	// redisManager.SAdd("1562588391084695552/pod/index/n1", // 使用namespace做索引，n1即为命名空间名称
	// 	"pod1", "pod2", "pod3")
	// // smembers 1562588391084695552/pod/index/n1  获取数据
}
