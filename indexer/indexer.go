package indexer

import (
	"fmt"
	"informer/redis"
	"sync"

	clientcache "k8s.io/client-go/tools/cache"
)

//use redis cache data

type RedisIndexer struct {
	lock         sync.RWMutex
	redisManager *redis.RedisManager
	dataKey      string
	indexKey     string
	// namespace/name
	indexers clientcache.Indexers
}

// create redis Indexer
// redisManager: redis manage
// clusterId: K8S cluster Id user snowflake generate
// kind: K8S resource kind
func NewRedisIndexer(redisManager *redis.RedisManager, clusterId, kind string) *cache {
	indexers := make(clientcache.Indexers, 1)
	// 设置索引函数，以namespce作为索引
	indexers["namespace"] = clientcache.MetaNamespaceIndexFunc
	return &cache{
		cacheStorage: RedisIndexer{
			redisManager: redisManager,
			dataKey:      clusterId + "/" + kind,             // redis hash key
			indexKey:     clusterId + "/" + kind + "/index/", // redis set key
			indexers:     indexers,
		},
		keyFunc: clientcache.DeletionHandlingMetaNamespaceKeyFunc,
	}
}

func (c *RedisIndexer) Add(key string, obj interface{}) {
	c.Update(key, obj)
}

func (c *RedisIndexer) getKey() {
	return
}

func (c *RedisIndexer) Update(field string, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// get old value
	oldValue := c.redisManager.HMGet(c.dataKey, field)

	// update new value
	c.redisManager.HMSet(c.dataKey, field, value)

	c.updateIndices(oldValue, value, field)
}

func (c *RedisIndexer) Delete(field string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	value := c.redisManager.HMGet(c.dataKey, field)
	if value != nil {
		//del idx
		c.updateIndices(value, nil, field)
		//del data
		c.redisManager.HDel(c.dataKey, field)
	}
}

func (c *RedisIndexer) Get(field string) (item interface{}, exists bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	value := c.redisManager.HMGet(c.dataKey, field)
	if value != nil {
		return value, true
	}
	return nil, false
}

func (c *RedisIndexer) List() []interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// get data count
	count := c.redisManager.HLen(c.dataKey)
	list := make([]interface{}, 0, count)

	values := c.redisManager.HVals(c.dataKey)
	for _, item := range values {
		list = append(list, item)
	}

	return list
}

// ListKeys returns a list of all the keys of the objects currently
// in the threadSafeMap.
func (c *RedisIndexer) ListKeys() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.redisManager.HKeys(c.dataKey)
}

func (c *RedisIndexer) Replace(items map[string]interface{}, resourceVersion string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	//delete all old data
	c.redisManager.Del(c.dataKey)

	//save all
	for field, value := range items {
		c.redisManager.HMSet(c.dataKey, field, value)
	}

	// delete all old index
	// get all key
	keys := c.redisManager.Keys(c.indexKey + "*")
	c.redisManager.Del(keys...)

	// rebuild all index
	values := c.redisManager.HGetAll(c.dataKey)
	for key, item := range values {
		c.updateIndices(nil, item, key)
	}
}

// updateIndices modifies the objects location in the managed indexes:
// - for create you must provide only the newObj
// - for update you must provide both the oldObj and the newObj
// - for delete you must provide only the oldObj
// updateIndices must be called from a function that already has a lock on the cache
func (c *RedisIndexer) updateIndices(oldObj interface{}, newObj interface{}, field string) {
	var oldIndexValues, indexValues []string
	var err error
	for name, indexFunc := range c.indexers {
		// get old value index value
		if oldObj != nil { // update
			oldIndexValues, err = indexFunc(oldObj)
		} else {
			// clean old index value
			oldIndexValues = oldIndexValues[:0]
		}
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", field, name, err))
		}

		// create update
		if newObj != nil {
			indexValues, err = indexFunc(newObj)
		} else {
			// delete
			indexValues = indexValues[:0]
		}
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", field, name, err))
		}

		// if index equal, skip
		if len(indexValues) == 1 && len(oldIndexValues) == 1 && indexValues[0] == oldIndexValues[0] {
			// We optimize for the most common case where indexFunc returns a single value which has not been changed
			continue
		}

		for _, value := range oldIndexValues {
			c.deleteKeyFromIndex(field, value)
		}

		for _, value := range indexValues {
			c.addKeyToIndex(field, value)
		}
	}
}

// indexValue is namespace'name
func (c *RedisIndexer) addKeyToIndex(field, indexValue string) {
	c.redisManager.SAdd(c.indexKey+indexValue, field)
}

func (c *RedisIndexer) deleteKeyFromIndex(field, indexValue string) {
	c.redisManager.SRem(c.indexKey+indexValue, field)
}

func (c *RedisIndexer) GetIndexers() clientcache.Indexers {
	return c.indexers
}

func (c *RedisIndexer) Index(indexName string, obj interface{}) ([]interface{}, error) {
	return nil, nil
}

func (c *RedisIndexer) IndexKeys(indexName, indexedValue string) ([]string, error) {
	return nil, nil
}

func (c *RedisIndexer) ListIndexFuncValues(indexName string) []string {
	return nil
}

func (c *RedisIndexer) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	return nil, nil
}

func (c *RedisIndexer) AddIndexers(newIndexers clientcache.Indexers) error {
	return nil
}

type cache struct {
	// cacheStorage bears the burden of thread safety for the cache
	cacheStorage RedisIndexer
	// keyFunc is used to make the key for objects stored in and retrieved from items, and
	// should be deterministic.
	keyFunc clientcache.KeyFunc
}

// Add inserts an item into the cache.
func (c *cache) Add(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return clientcache.KeyError{obj, err}
	}
	c.cacheStorage.Add(key, obj)
	return nil
}

// Update sets an item in the cache to its updated state.
func (c *cache) Update(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return clientcache.KeyError{obj, err}
	}
	c.cacheStorage.Update(key, obj)
	return nil
}

// Delete removes an item from the cache.
func (c *cache) Delete(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return clientcache.KeyError{obj, err}
	}
	c.cacheStorage.Delete(key)
	return nil
}

// List returns a list of all the items.
// List is completely threadsafe as long as you treat all items as immutable.
func (c *cache) List() []interface{} {
	return c.cacheStorage.List()
}

// ListKeys returns a list of all the keys of the objects currently
// in the cache.
func (c *cache) ListKeys() []string {
	return c.cacheStorage.ListKeys()
}

// GetIndexers returns the indexers of cache
func (c *cache) GetIndexers() clientcache.Indexers {
	return c.cacheStorage.GetIndexers()
}

// Index returns a list of items that match on the index function
// Index is thread-safe so long as you treat all items as immutable
func (c *cache) Index(indexName string, obj interface{}) ([]interface{}, error) {
	return c.cacheStorage.Index(indexName, obj)
}

func (c *cache) IndexKeys(indexName, indexKey string) ([]string, error) {
	return c.cacheStorage.IndexKeys(indexName, indexKey)
}

// ListIndexFuncValues returns the list of generated values of an Index func
func (c *cache) ListIndexFuncValues(indexName string) []string {
	return c.cacheStorage.ListIndexFuncValues(indexName)
}

func (c *cache) ByIndex(indexName, indexKey string) ([]interface{}, error) {
	return c.cacheStorage.ByIndex(indexName, indexKey)
}

func (c *cache) AddIndexers(newIndexers clientcache.Indexers) error {
	return c.cacheStorage.AddIndexers(newIndexers)
}

// Get returns the requested item, or sets exists=false.
// Get is completely threadsafe as long as you treat all items as immutable.
func (c *cache) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := c.keyFunc(obj)
	if err != nil {
		return nil, false, clientcache.KeyError{obj, err}
	}
	return c.GetByKey(key)
}

// GetByKey returns the request item, or exists=false.
// GetByKey is completely threadsafe as long as you treat all items as immutable.
func (c *cache) GetByKey(key string) (item interface{}, exists bool, err error) {
	item, exists = c.cacheStorage.Get(key)
	return item, exists, nil
}

// Replace will delete the contents of 'c', using instead the given list.
// 'c' takes ownership of the list, you should not reference the list again
// after calling this function.
func (c *cache) Replace(list []interface{}, resourceVersion string) error {
	items := make(map[string]interface{}, len(list))
	for _, item := range list {
		key, err := c.keyFunc(item)
		if err != nil {
			return clientcache.KeyError{item, err}
		}
		items[key] = item
	}
	c.cacheStorage.Replace(items, resourceVersion)
	return nil
}

// Resync is meaningless for one of these
func (c *cache) Resync() error {
	return nil
}
