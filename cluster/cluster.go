package cluster

import (
	"encoding/json"
	"informer/manager"
	"informer/redis"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

var redisMgr *redis.RedisManager

func init() {
	redisMgr = redis.NewRedisManager("localhost:6379", "mima", 0)
}

// Monitor cluster
func Monitor(cluster *manager.ClusterBase, stopCh chan struct{}) {
	var clusterOperate manager.ClusterOperate
	clusterOperate.Base = cluster
	clusterOperate.KindOperates = getKindOperate()
	manager.CreateManager(&clusterOperate, stopCh)
}

// cluster all kind
func getKindOperate() []manager.KindOperate {
	operates := make([]manager.KindOperate, 1)
	operates = append(operates, getPodOperate())
	return nil
}

func getPodOperate() manager.KindOperate {
	var kindOperate manager.KindOperate
	kindOperate.Kind = &v1.Pod{}
	var operates = make([]manager.Operate, 1)
	operate := manager.Operate{
		Func:   PodFunc,
		Forget: PodForget,
		After:  3,
	}
	operates = append(operates, operate)
	kindOperate.Operates = operates
	return kindOperate
}

// pod deal func
func PodFunc(obj interface{}, clusterId string) bool {
	// 1) data save to redis
	data, ok := obj.(cache.Delta)
	if !ok {
		klog.Error("data transform failed.")
		return false
	}
	pod, ok := data.Object.(v1.Pod)
	if !ok {
		klog.Error("data transform failed.")
		return false
	}
	key := clusterId + "/" + "pod"
	field := pod.Namespace + "/" + pod.Name
	json, err := json.Marshal(pod)
	if err != nil {
		klog.Error("marshal pod failed. ")
		return false
	}

	switch data.Type {
	case cache.Added:
		redisMgr.HMSet(key, field, string(json))

	case cache.Updated:
		redisMgr.HMSet(key, field, string(json))

	case cache.Deleted:
		redisMgr.HDel(key, field)
	}

	// 2) data save to nebula, use redis stream

	return true
}

//pod skip function
func PodForget(obj interface{}, clusterId string) bool {

	return false
}


