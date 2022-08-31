package manager

import (
	"errors"
	"flag"
	"informer/tools"
	"net"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/fields"
	rt "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

// deal func
type Func func(obj interface{}, clusterId string) bool

// skip function
type Forget func(obj interface{}, clusterId string) bool

type Operate struct {
	Func   Func          // deal func
	Forget Forget        // deal failed, forget
	After  time.Duration // deal failed, delay time
}

type Controller struct {
	queue       workqueue.RateLimitingInterface
	ctrl        cache.Controller
	kindOperate KindOperate
	clusterId   string
}

type ClusterBase struct {
	Id    string
	Ip    string
	Port  string
	Token string
}

type ClusterOperate struct {
	Base         *ClusterBase
	KindOperates []KindOperate
}

type Manager struct {
	cluster *ClusterOperate
	ctrls   map[string]*Controller
	client  kubernetes.Clientset // k8s client
}

type KindOperate struct {
	Kind     rt.Object
	Operates []Operate
}

func NewK8SClient() (*kubernetes.Clientset, error) {
	var err error
	var config *rest.Config
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(可选) kubeconfig 文件的绝对路径")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "kubeconfig 文件的绝对路径")
	}
	flag.Parse()

	if config, err = rest.InClusterConfig(); err != nil {
		if config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig); err != nil {
			panic(err.Error())
		}
	}
	return kubernetes.NewForConfig(config)
}

// create client with k8s admin token
func NewK8SClientWithToken(host, port, token string) (*kubernetes.Clientset, error) {
	clientSet, err := kubernetes.NewForConfig(&rest.Config{
		//"https://" + net.JoinHostPort("10.254.177.103", "6443"),
		Host:            "https://" + net.JoinHostPort(host, port),
		TLSClientConfig: rest.TLSClientConfig{Insecure: true},
		BearerToken:     string(token),
	})
	if err != nil {
		return nil, err
	}
	return clientSet, nil
}

func NewInformer(lw cache.ListerWatcher, objType rt.Object, resyncPeriod time.Duration, h cache.ResourceEventHandler) cache.Controller {
	fifo := cache.NewDeltaFIFOWithOptions(cache.DeltaFIFOOptions{
		KnownObjects:          nil, // indexer set nil
		EmitDeltaTypeReplaced: true,
	})
	cfg := &cache.Config{
		Queue:            fifo, // deltafifo
		ListerWatcher:    lw,
		ObjectType:       objType,
		FullResyncPeriod: resyncPeriod,
		RetryOnError:     false,

		Process: func(obj interface{}) error {
			if deltas, ok := obj.(cache.Deltas); ok {
				return processDeltas(h, nil, nil, deltas)
			}
			return errors.New("object given as Process argument is not Deltas")
		},
	}
	return cache.New(cfg)
}

func processDeltas(handler cache.ResourceEventHandler, clientState cache.Store, transformer cache.TransformFunc, deltas cache.Deltas) error {
	for _, d := range deltas {
		obj := d.Object
		if transformer != nil {
			var err error
			obj, err = transformer(obj)
			if err != nil {
				return err
			}
		}

		// delete indexer
		switch d.Type {
		case cache.Sync, cache.Replaced, cache.Updated:
			handler.OnUpdate(nil, obj)
		case cache.Added:
			handler.OnAdd(obj)
		case cache.Deleted:
			handler.OnDelete(obj)
		}
	}
	return nil
}

// create controller
// runtime: k8s GVK
// namespace: namespace
// ip: k8s ip
// port: k8s port
// token: k8s access token
func NewController(clientset *kubernetes.Clientset, clusterId string, kindOperate KindOperate) *Controller {
	// create list watch
	podListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), kindOperate.Kind.GetObjectKind().GroupVersionKind().Kind, "", fields.Everything())

	// create queue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	//create informer
	informer := NewInformer(podListWatcher, kindOperate.Kind, 10, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			queue.Add(cache.Delta{Type: cache.Added, Object: obj})
		},
		UpdateFunc: func(old interface{}, new interface{}) { // indexer not exist, so old is nil
			queue.Add(cache.Delta{Type: cache.Updated, Object: new})
		},
		DeleteFunc: func(obj interface{}) {
			queue.Add(cache.Delta{Type: cache.Deleted, Object: obj})
		},
	})

	return &Controller{
		ctrl:        informer,
		queue:       queue,
		kindOperate: kindOperate,
		clusterId:   clusterId,
	}
}

func (c *Controller) Run(threadiness int, stopCh chan struct{}) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	go c.ctrl.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.ctrl.HasSynced) {
		klog.Error("wait for cache sync failed. ")
		return
	}

	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
}

// start worker deal item
func (c *Controller) runWorker() {
	for c.process() {
	}
}

func (c *Controller) process() bool {
	item, shutdown := c.queue.Get()
	if shutdown {
		return false
	}

	for _, operate := range c.kindOperate.Operates {
		success := operate.Func(item, c.clusterId)
		if success {
			c.queue.Done(item)
		} else {
			if operate.Forget(item, c.clusterId) {
				c.queue.Done(item)
			} else {
				c.queue.AddAfter(item, operate.After)
			}
		}
	}

	return true
}

func CreateManager(cluster *ClusterOperate, stopCh chan struct{}) (*Manager, error) {
	var manager Manager
	manager.cluster = cluster

	clientset, err := NewK8SClientWithToken(cluster.Base.Ip, cluster.Base.Port, cluster.Base.Token)
	if err != nil {
		klog.Fatal(err)
	}
	manager.client = *clientset

	ctrls := make(map[string]*Controller, len(cluster.KindOperates))
	cpu := tools.GetCpuCore()
	for _, kindOperate := range cluster.KindOperates {
		ctrl := NewController(clientset, cluster.Base.Id, kindOperate)
		uuid, _ := tools.GetUuid()
		ctrls[kindOperate.Kind.GetObjectKind().GroupVersionKind().Kind+"/"+uuid] = ctrl
		go ctrl.Run(cpu, stopCh)
	}
	manager.ctrls = ctrls

	return &manager, nil
}
