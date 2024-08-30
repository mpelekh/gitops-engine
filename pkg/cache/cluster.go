package cache

import (
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/semaphore"
	authorizationv1 "k8s.io/api/authorization/v1"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	authType1 "k8s.io/client-go/kubernetes/typed/authorization/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/pager"
	watchutil "k8s.io/client-go/tools/watch"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2/textlogger"
	"k8s.io/kubectl/pkg/util/openapi"

	"github.com/argoproj/gitops-engine/pkg/utils/kube"
	"github.com/argoproj/gitops-engine/pkg/utils/tracing"
)

const (
	watchResourcesRetryTimeout = 1 * time.Second
	ClusterRetryTimeout        = 10 * time.Second

	// default duration before we invalidate entire cluster cache. Can be set to 0 to never invalidate cache
	defaultClusterResyncTimeout = 24 * time.Hour

	// default duration before restarting individual resource watch
	defaultWatchResyncTimeout = 10 * time.Minute

	// Same page size as in k8s.io/client-go/tools/pager/pager.go
	defaultListPageSize = 500
	// Prefetch only a single page
	defaultListPageBufferSize = 1
	// listSemaphore is used to limit the number of concurrent memory consuming operations on the
	// k8s list queries results.
	// Limit is required to avoid memory spikes during cache initialization.
	// The default limit of 50 is chosen based on experiments.
	defaultListSemaphoreWeight = 50
)

const (
	// RespectRbacDisabled default value for respectRbac
	RespectRbacDisabled = iota
	// RespectRbacNormal checks only api response for forbidden/unauthorized errors
	RespectRbacNormal
	// RespectRbacStrict checks both api response for forbidden/unauthorized errors and SelfSubjectAccessReview
	RespectRbacStrict
)

type apiMeta struct {
	namespaced  bool
	watchCancel context.CancelFunc
}

type eventMeta struct {
	event watch.EventType
	un    *unstructured.Unstructured
}

// ClusterInfo holds cluster cache stats
type ClusterInfo struct {
	// Server holds cluster API server URL
	Server string
	// K8SVersion holds Kubernetes version
	K8SVersion string
	// ResourcesCount holds number of observed Kubernetes resources
	ResourcesCount int
	// APIsCount holds number of observed Kubernetes API count
	APIsCount int
	// LastCacheSyncTime holds time of most recent cache synchronization
	LastCacheSyncTime *time.Time
	// SyncError holds most recent cache synchronization error
	SyncError error
	// APIResources holds list of API resources supported by the cluster
	APIResources []kube.APIResourceInfo
}

// OnEventHandler is a function that handles Kubernetes event
type OnEventHandler func(event watch.EventType, un *unstructured.Unstructured)

// OnPopulateResourceInfoHandler returns additional resource metadata that should be stored in cache
type OnPopulateResourceInfoHandler func(un *unstructured.Unstructured, isRoot bool) (info interface{}, cacheManifest bool)

// OnResourceUpdatedHandler handlers resource update event
type OnResourceUpdatedHandler func(newRes *Resource, oldRes *Resource, namespaceResources map[kube.ResourceKey]*Resource)

// OnResourceLockAcquireHandler handlers resource lock acquire event
type OnResourceLockAcquireHandler func(duration time.Duration)

// OnProcessEventHandler handlers process event
type OnProcessEventHandler func(duration time.Duration)

// OnProcessEventsHandler handlers process event
type OnProcessEventsHandler func(duration time.Duration, processedEventsNumber int)

// OnIterateHierarchyHandler handlers resource hierarchy iteration event
type OnIterateHierarchyHandler func(kind, namespace string, duration time.Duration, acquireLockDuration time.Duration)

type Unsubscribe func()

type ClusterCache interface {
	// EnsureSynced checks cache state and synchronizes it if necessary
	EnsureSynced() error
	// GetServerVersion returns observed cluster version
	GetServerVersion() string
	// GetAPIResources returns information about observed API resources
	GetAPIResources() []kube.APIResourceInfo
	// GetOpenAPISchema returns open API schema of supported API resources
	GetOpenAPISchema() openapi.Resources
	// GetGVKParser returns a parser able to build a TypedValue used in
	// structured merge diffs.
	GetGVKParser() *managedfields.GvkParser
	// Invalidate cache and executes callback that optionally might update cache settings
	Invalidate(opts ...UpdateSettingsFunc)
	// FindResources returns resources that matches given list of predicates from specified namespace or everywhere if specified namespace is empty
	FindResources(namespace string, predicates ...func(r *Resource) bool) map[kube.ResourceKey]*Resource
	// IterateHierarchy iterates resource tree starting from the specified top level resource and executes callback for each resource in the tree.
	// The action callback returns true if iteration should continue and false otherwise.
	IterateHierarchy(key kube.ResourceKey, action func(resource *Resource, namespaceResources map[kube.ResourceKey]*Resource) bool)
	// IterateHierarchyV2 iterates resource tree starting from the specified top level resources and executes callback for each resource in the tree.
	// The action callback returns true if iteration should continue and false otherwise.
	IterateHierarchyV2(keys []kube.ResourceKey, action func(resource *Resource, namespaceResources map[kube.ResourceKey]*Resource) bool)
	// IsNamespaced answers if specified group/kind is a namespaced resource API or not
	IsNamespaced(gk schema.GroupKind) (bool, error)
	// GetManagedLiveObjs helps finding matching live K8S resources for a given resources list.
	// The function returns all resources from cache for those `isManaged` function returns true and resources
	// specified in targetObjs list.
	GetManagedLiveObjs(targetObjs []*unstructured.Unstructured, isManaged func(r *Resource) bool) (map[kube.ResourceKey]*unstructured.Unstructured, error)
	// GetClusterInfo returns cluster cache statistics
	GetClusterInfo() ClusterInfo
	// OnResourceUpdated register event handler that is executed every time when resource get's updated in the cache
	OnResourceUpdated(handler OnResourceUpdatedHandler) Unsubscribe
	// OnEvent register event handler that is executed every time when new K8S event received
	OnEvent(handler OnEventHandler) Unsubscribe
	// OnResourceLockAcquire register event handler that is executed every time when resource lock is acquired by goroutine
	OnResourceLockAcquire(handler OnResourceLockAcquireHandler) Unsubscribe
	// OnProcessEventHandler register event handler that is executed every time when event received
	OnProcessEventHandler(handler OnProcessEventHandler) Unsubscribe
	// OnProcessEventsHandler register event handler that is executed every time when events were processed
	OnProcessEventsHandler(handler OnProcessEventsHandler) Unsubscribe
	// OnIterateHierarchyHandler register event handler that is executed every time when hierarchy is iterated
	OnIterateHierarchyHandler(handler OnIterateHierarchyHandler) Unsubscribe
}

type WeightedSemaphore interface {
	Acquire(ctx context.Context, n int64) error
	TryAcquire(n int64) bool
	Release(n int64)
}

type ListRetryFunc func(err error) bool

// NewClusterCache creates new instance of cluster cache
func NewClusterCache(config *rest.Config, opts ...UpdateSettingsFunc) *clusterCache {
	log := textlogger.NewLogger(textlogger.NewConfig())
	cache := &clusterCache{
		eventCh:            make(chan eventMeta),
		settings:           Settings{ResourceHealthOverride: &noopSettings{}, ResourcesFilter: &noopSettings{}},
		apisMeta:           make(map[schema.GroupKind]*apiMeta),
		listPageSize:       defaultListPageSize,
		listPageBufferSize: defaultListPageBufferSize,
		listSemaphore:      semaphore.NewWeighted(defaultListSemaphoreWeight),
		resources:          make(map[kube.ResourceKey]*Resource),
		nsIndex:            make(map[string]map[kube.ResourceKey]*Resource),
		config:             config,
		kubectl: &kube.KubectlCmd{
			Log:    log,
			Tracer: tracing.NopTracer{},
		},
		syncStatus: clusterCacheSync{
			resyncTimeout: defaultClusterResyncTimeout,
			syncTime:      nil,
		},
		watchResyncTimeout:         defaultWatchResyncTimeout,
		clusterSyncRetryTimeout:    ClusterRetryTimeout,
		resourceUpdatedHandlers:    map[uint64]OnResourceUpdatedHandler{},
		eventHandlers:              map[uint64]OnEventHandler{},
		resourceLockAcquireHandler: map[uint64]OnResourceLockAcquireHandler{},
		processEventHandlers:       map[uint64]OnProcessEventHandler{},
		processEventsHandlers:      map[uint64]OnProcessEventsHandler{},
		iterateHierarchyHandlers:   map[uint64]OnIterateHierarchyHandler{},
		log:                        log,
		listRetryLimit:             1,
		listRetryUseBackoff:        false,
		listRetryFunc:              ListRetryFuncNever,
	}
	for i := range opts {
		opts[i](cache)
	}
	return cache
}

type clusterCache struct {
	eventCh    chan eventMeta
	syncStatus clusterCacheSync

	apisMeta      map[schema.GroupKind]*apiMeta
	serverVersion string
	apiResources  []kube.APIResourceInfo
	// namespacedResources is a simple map which indicates a groupKind is namespaced
	namespacedResources map[schema.GroupKind]bool

	// maximum time we allow watches to run before relisting the group/kind and restarting the watch
	watchResyncTimeout time.Duration
	// sync retry timeout for cluster when sync error happens
	clusterSyncRetryTimeout time.Duration

	// size of a page for list operations pager.
	listPageSize int64
	// number of pages to prefetch for list pager.
	listPageBufferSize int32
	listSemaphore      WeightedSemaphore

	// retry options for list operations
	listRetryLimit      int32
	listRetryUseBackoff bool
	listRetryFunc       ListRetryFunc

	// lock is a rw lock which protects the fields of clusterInfo
	lock      sync.RWMutex
	resources map[kube.ResourceKey]*Resource
	nsIndex   map[string]map[kube.ResourceKey]*Resource

	kubectl          kube.Kubectl
	log              logr.Logger
	config           *rest.Config
	namespaces       []string
	clusterResources bool
	settings         Settings

	handlersLock                sync.RWMutex
	handlerKey                  uint64
	populateResourceInfoHandler OnPopulateResourceInfoHandler
	resourceUpdatedHandlers     map[uint64]OnResourceUpdatedHandler
	eventHandlers               map[uint64]OnEventHandler
	resourceLockAcquireHandler  map[uint64]OnResourceLockAcquireHandler
	processEventHandlers        map[uint64]OnProcessEventHandler
	processEventsHandlers       map[uint64]OnProcessEventsHandler
	iterateHierarchyHandlers    map[uint64]OnIterateHierarchyHandler
	openAPISchema               openapi.Resources
	gvkParser                   *managedfields.GvkParser

	respectRBAC int
}

type clusterCacheSync struct {
	// When using this struct:
	// 1) 'lock' mutex should be acquired when reading/writing from fields of this struct.
	// 2) The parent 'clusterCache.lock' does NOT need to be owned to r/w from fields of this struct (if it is owned, that is fine, but see below)
	// 3) To prevent deadlocks, do not acquire parent 'clusterCache.lock' after acquiring this lock; if you need both locks, always acquire the parent lock first
	lock          sync.Mutex
	syncTime      *time.Time
	syncError     error
	resyncTimeout time.Duration
}

// ListRetryFuncNever never retries on errors
func ListRetryFuncNever(err error) bool {
	return false
}

// ListRetryFuncAlways always retries on errors
func ListRetryFuncAlways(err error) bool {
	return true
}

// OnResourceUpdated register event handler that is executed every time when resource get's updated in the cache
func (c *clusterCache) OnResourceUpdated(handler OnResourceUpdatedHandler) Unsubscribe {
	c.handlersLock.Lock()
	defer c.handlersLock.Unlock()
	key := c.handlerKey
	c.handlerKey++
	c.resourceUpdatedHandlers[key] = handler
	return func() {
		c.handlersLock.Lock()
		defer c.handlersLock.Unlock()
		delete(c.resourceUpdatedHandlers, key)
	}
}

func (c *clusterCache) getResourceUpdatedHandlers() []OnResourceUpdatedHandler {
	c.handlersLock.RLock()
	defer c.handlersLock.RUnlock()
	var handlers []OnResourceUpdatedHandler
	for _, h := range c.resourceUpdatedHandlers {
		handlers = append(handlers, h)
	}
	return handlers
}

// OnEvent register event handler that is executed every time when new K8S event received
func (c *clusterCache) OnEvent(handler OnEventHandler) Unsubscribe {
	c.handlersLock.Lock()
	defer c.handlersLock.Unlock()
	key := c.handlerKey
	c.handlerKey++
	c.eventHandlers[key] = handler
	return func() {
		c.handlersLock.Lock()
		defer c.handlersLock.Unlock()
		delete(c.eventHandlers, key)
	}
}

func (c *clusterCache) getEventHandlers() []OnEventHandler {
	c.handlersLock.RLock()
	defer c.handlersLock.RUnlock()
	handlers := make([]OnEventHandler, 0, len(c.eventHandlers))
	for _, h := range c.eventHandlers {
		handlers = append(handlers, h)
	}
	return handlers
}

// OnResourceLockAcquire register event handler that is executed every time when resource lock is acquired by goroutine
func (c *clusterCache) OnResourceLockAcquire(handler OnResourceLockAcquireHandler) Unsubscribe {
	c.handlersLock.Lock()
	defer c.handlersLock.Unlock()
	key := c.handlerKey
	c.handlerKey++
	c.resourceLockAcquireHandler[key] = handler
	return func() {
		c.handlersLock.Lock()
		defer c.handlersLock.Unlock()
		delete(c.resourceLockAcquireHandler, key)
	}
}

func (c *clusterCache) getResourceLockAcquireHandlers() []OnResourceLockAcquireHandler {
	c.handlersLock.RLock()
	defer c.handlersLock.RUnlock()
	handlers := make([]OnResourceLockAcquireHandler, 0, len(c.resourceLockAcquireHandler))
	for _, h := range c.resourceLockAcquireHandler {
		handlers = append(handlers, h)
	}
	return handlers
}

// OnProcessEventHandler register event handler that is executed every time when event received
func (c *clusterCache) OnProcessEventHandler(handler OnProcessEventHandler) Unsubscribe {
	c.handlersLock.Lock()
	defer c.handlersLock.Unlock()
	key := c.handlerKey
	c.handlerKey++
	c.processEventHandlers[key] = handler
	return func() {
		c.handlersLock.Lock()
		defer c.handlersLock.Unlock()
		delete(c.processEventHandlers, key)
	}
}

func (c *clusterCache) getProcessEventHandlers() []OnProcessEventHandler {
	c.handlersLock.RLock()
	defer c.handlersLock.RUnlock()
	handlers := make([]OnProcessEventHandler, 0, len(c.processEventHandlers))
	for _, h := range c.processEventHandlers {
		handlers = append(handlers, h)
	}
	return handlers
}

// OnProcessEventHandler register event handler that is executed every time when event received
func (c *clusterCache) OnProcessEventsHandler(handler OnProcessEventsHandler) Unsubscribe {
	c.handlersLock.Lock()
	defer c.handlersLock.Unlock()
	key := c.handlerKey
	c.handlerKey++
	c.processEventsHandlers[key] = handler
	return func() {
		c.handlersLock.Lock()
		defer c.handlersLock.Unlock()
		delete(c.processEventsHandlers, key)
	}
}

func (c *clusterCache) getProcessEventsHandlers() []OnProcessEventsHandler {
	c.handlersLock.RLock()
	defer c.handlersLock.RUnlock()
	handlers := make([]OnProcessEventsHandler, 0, len(c.processEventsHandlers))
	for _, h := range c.processEventsHandlers {
		handlers = append(handlers, h)
	}
	return handlers
}

// OnIterateHierarchyHandler register event handler that is executed every time when hierarchy is iterated
func (c *clusterCache) OnIterateHierarchyHandler(handler OnIterateHierarchyHandler) Unsubscribe {
	c.handlersLock.Lock()
	defer c.handlersLock.Unlock()
	key := c.handlerKey
	c.handlerKey++
	c.iterateHierarchyHandlers[key] = handler
	return func() {
		c.handlersLock.Lock()
		defer c.handlersLock.Unlock()
		delete(c.iterateHierarchyHandlers, key)
	}
}

func (c *clusterCache) getIterateHierarchyHandlers() []OnIterateHierarchyHandler {
	c.handlersLock.RLock()
	defer c.handlersLock.RUnlock()
	handlers := make([]OnIterateHierarchyHandler, 0, len(c.iterateHierarchyHandlers))
	for _, h := range c.iterateHierarchyHandlers {
		handlers = append(handlers, h)
	}
	return handlers
}

// GetServerVersion returns observed cluster version
func (c *clusterCache) GetServerVersion() string {
	return c.serverVersion
}

// GetAPIResources returns information about observed API resources
// This method is called frequently during reconciliation to pass API resource info to `helm template`
// NOTE: we do not provide any consistency guarantees about the returned list. The list might be
// updated in place (anytime new CRDs are introduced or removed). If necessary, a separate method
// would need to be introduced to return a copy of the list so it can be iterated consistently.
func (c *clusterCache) GetAPIResources() []kube.APIResourceInfo {
	return c.apiResources
}

// GetOpenAPISchema returns open API schema of supported API resources
func (c *clusterCache) GetOpenAPISchema() openapi.Resources {
	return c.openAPISchema
}

// GetGVKParser returns a parser able to build a TypedValue used in
// structured merge diffs.
func (c *clusterCache) GetGVKParser() *managedfields.GvkParser {
	return c.gvkParser
}

func (c *clusterCache) appendAPIResource(info kube.APIResourceInfo) {
	exists := false
	for i := range c.apiResources {
		if c.apiResources[i].GroupKind == info.GroupKind && c.apiResources[i].GroupVersionResource.Version == info.GroupVersionResource.Version {
			exists = true
			break
		}
	}
	if !exists {
		c.apiResources = append(c.apiResources, info)
	}
}

func (c *clusterCache) deleteAPIResource(info kube.APIResourceInfo) {
	for i := range c.apiResources {
		if c.apiResources[i].GroupKind == info.GroupKind && c.apiResources[i].GroupVersionResource.Version == info.GroupVersionResource.Version {
			c.apiResources[i] = c.apiResources[len(c.apiResources)-1]
			c.apiResources = c.apiResources[:len(c.apiResources)-1]
			break
		}
	}
}

func (c *clusterCache) replaceResourceCache(gk schema.GroupKind, resources []*Resource, ns string) {
	objByKey := make(map[kube.ResourceKey]*Resource)
	for i := range resources {
		objByKey[resources[i].ResourceKey()] = resources[i]
	}

	// update existing nodes
	for i := range resources {
		res := resources[i]
		oldRes := c.resources[res.ResourceKey()]
		if oldRes == nil || oldRes.ResourceVersion != res.ResourceVersion {
			c.onNodeUpdated(oldRes, res)
		}
	}

	for key := range c.resources {
		if key.Kind != gk.Kind || key.Group != gk.Group || ns != "" && key.Namespace != ns {
			continue
		}

		if _, ok := objByKey[key]; !ok {
			c.onNodeRemoved(key)
		}
	}
}

func (c *clusterCache) newResource(un *unstructured.Unstructured) *Resource {
	ownerRefs, isInferredParentOf := c.resolveResourceReferences(un)

	cacheManifest := false
	var info interface{}
	if c.populateResourceInfoHandler != nil {
		info, cacheManifest = c.populateResourceInfoHandler(un, len(ownerRefs) == 0)
	}
	var creationTimestamp *metav1.Time
	ct := un.GetCreationTimestamp()
	if !ct.IsZero() {
		creationTimestamp = &ct
	}
	resource := &Resource{
		ResourceVersion:    un.GetResourceVersion(),
		Ref:                kube.GetObjectRef(un),
		OwnerRefs:          ownerRefs,
		Info:               info,
		CreationTimestamp:  creationTimestamp,
		isInferredParentOf: isInferredParentOf,
	}
	if cacheManifest {
		resource.Resource = un
	}

	return resource
}

func (c *clusterCache) setNode(n *Resource) {
	key := n.ResourceKey()
	c.resources[key] = n
	ns, ok := c.nsIndex[key.Namespace]
	if !ok {
		ns = make(map[kube.ResourceKey]*Resource)
		c.nsIndex[key.Namespace] = ns
	}
	ns[key] = n

	// update inferred parent references
	if n.isInferredParentOf != nil || mightHaveInferredOwner(n) {
		for k, v := range ns {
			// update child resource owner references
			if n.isInferredParentOf != nil && mightHaveInferredOwner(v) {
				v.setOwnerRef(n.toOwnerRef(), n.isInferredParentOf(k))
			}
			if mightHaveInferredOwner(n) && v.isInferredParentOf != nil {
				n.setOwnerRef(v.toOwnerRef(), v.isInferredParentOf(n.ResourceKey()))
			}
		}
	}
}

// Invalidate cache and executes callback that optionally might update cache settings
func (c *clusterCache) Invalidate(opts ...UpdateSettingsFunc) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.syncStatus.lock.Lock()
	c.syncStatus.syncTime = nil
	c.syncStatus.lock.Unlock()

	for i := range c.apisMeta {
		c.apisMeta[i].watchCancel()
	}
	for i := range opts {
		opts[i](c)
	}

	c.log.V(1).Info("Closing event channel in Invalidate method")
	c.closeEventChannelSafely()

	c.apisMeta = nil
	c.namespacedResources = nil
	c.log.Info("Invalidated cluster")
}

// clusterCacheSync's lock should be held before calling this method
func (syncStatus *clusterCacheSync) synced(clusterRetryTimeout time.Duration) bool {
	syncTime := syncStatus.syncTime

	if syncTime == nil {
		return false
	}
	if syncStatus.syncError != nil {
		return time.Now().Before(syncTime.Add(clusterRetryTimeout))
	}
	if syncStatus.resyncTimeout == 0 {
		// cluster resync timeout has been disabled
		return true
	}
	return time.Now().Before(syncTime.Add(syncStatus.resyncTimeout))
}

func (c *clusterCache) stopWatching(gk schema.GroupKind, ns string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if info, ok := c.apisMeta[gk]; ok {
		info.watchCancel()
		delete(c.apisMeta, gk)
		c.replaceResourceCache(gk, nil, ns)
		c.log.Info(fmt.Sprintf("Stop watching: %s not found", gk))
	}

	c.log.V(1).Info("Closing event channel in stopWatching method")
	c.closeEventChannelSafely()
}

// startMissingWatches lists supported cluster resources and start watching for changes unless watch is already running
func (c *clusterCache) startMissingWatches() error {
	apis, err := c.kubectl.GetAPIResources(c.config, true, c.settings.ResourcesFilter)
	c.log.V(1).Info("Start missing watches", "apis", apis)
	if err != nil {
		return err
	}
	client, err := c.kubectl.NewDynamicClient(c.config)
	if err != nil {
		return err
	}
	clientset, err := kubernetes.NewForConfig(c.config)
	if err != nil {
		return err
	}
	namespacedResources := make(map[schema.GroupKind]bool)
	for i := range apis {
		api := apis[i]
		namespacedResources[api.GroupKind] = api.Meta.Namespaced
		if _, ok := c.apisMeta[api.GroupKind]; !ok {
			ctx, cancel := context.WithCancel(context.Background())
			c.apisMeta[api.GroupKind] = &apiMeta{namespaced: api.Meta.Namespaced, watchCancel: cancel}

			err := c.processApi(client, api, func(resClient dynamic.ResourceInterface, ns string) error {
				resourceVersion, err := c.loadInitialState(ctx, api, resClient, ns, false) // don't lock here, we are already in a lock before startMissingWatches is called inside watchEvents
				if err != nil && c.isRestrictedResource(err) {
					keep := false
					if c.respectRBAC == RespectRbacStrict {
						k, permErr := c.checkPermission(ctx, clientset.AuthorizationV1().SelfSubjectAccessReviews(), api)
						if permErr != nil {
							return fmt.Errorf("failed to check permissions for resource %s: %w, original error=%v", api.GroupKind.String(), permErr, err.Error())
						}
						keep = k
					}
					// if we are not allowed to list the resource, remove it from the watch list
					if !keep {
						delete(c.apisMeta, api.GroupKind)
						delete(namespacedResources, api.GroupKind)
						return nil
					}

				}
				go c.watchEvents(ctx, api, resClient, ns, resourceVersion)
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	c.namespacedResources = namespacedResources
	return nil
}

func runSynced(lock sync.Locker, action func() error) error {
	lock.Lock()
	defer lock.Unlock()
	return action()
}

// listResources creates list pager and enforces number of concurrent list requests
// The callback should not wait on any locks that may be held by other callers.
func (c *clusterCache) listResources(ctx context.Context, resClient dynamic.ResourceInterface, callback func(*pager.ListPager) error) (string, error) {
	if err := c.listSemaphore.Acquire(ctx, 1); err != nil {
		return "", err
	}
	defer c.listSemaphore.Release(1)

	var retryCount int64 = 0
	resourceVersion := ""
	listPager := pager.New(func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
		var res *unstructured.UnstructuredList
		var listRetry wait.Backoff

		if c.listRetryUseBackoff {
			listRetry = retry.DefaultBackoff
		} else {
			listRetry = retry.DefaultRetry
		}

		listRetry.Steps = int(c.listRetryLimit)
		err := retry.OnError(listRetry, c.listRetryFunc, func() error {
			var ierr error
			res, ierr = resClient.List(ctx, opts)
			if ierr != nil {
				// Log out a retry
				if c.listRetryLimit > 1 && c.listRetryFunc(ierr) {
					retryCount += 1
					c.log.Info(fmt.Sprintf("Error while listing resources: %v (try %d/%d)", ierr, retryCount, c.listRetryLimit))
				}
				return ierr
			}
			resourceVersion = res.GetResourceVersion()
			return nil
		})
		return res, err
	})
	listPager.PageBufferSize = c.listPageBufferSize
	listPager.PageSize = c.listPageSize

	return resourceVersion, callback(listPager)
}

func (c *clusterCache) loadInitialState(ctx context.Context, api kube.APIResourceInfo, resClient dynamic.ResourceInterface, ns string, lock bool) (string, error) {
	var items []*Resource
	resourceVersion, err := c.listResources(ctx, resClient, func(listPager *pager.ListPager) error {
		return listPager.EachListItem(ctx, metav1.ListOptions{}, func(obj runtime.Object) error {
			if un, ok := obj.(*unstructured.Unstructured); !ok {
				return fmt.Errorf("object %s/%s has an unexpected type", un.GroupVersionKind().String(), un.GetName())
			} else {
				items = append(items, c.newResource(un))
			}
			return nil
		})
	})

	if err != nil {
		return "", fmt.Errorf("failed to load initial state of resource %s: %w", api.GroupKind.String(), err)
	}

	if lock {
		return resourceVersion, runSynced(&c.lock, func() error {
			c.replaceResourceCache(api.GroupKind, items, ns)
			return nil
		})
	} else {
		c.replaceResourceCache(api.GroupKind, items, ns)
		return resourceVersion, nil
	}
}

func (c *clusterCache) watchEvents(ctx context.Context, api kube.APIResourceInfo, resClient dynamic.ResourceInterface, ns string, resourceVersion string) {
	kube.RetryUntilSucceed(ctx, watchResourcesRetryTimeout, fmt.Sprintf("watch %s on %s", api.GroupKind, c.config.Host), c.log, func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("Recovered from panic: %+v\n%s", r, debug.Stack())
			}
		}()

		// load API initial state if no resource version provided
		if resourceVersion == "" {
			resourceVersion, err = c.loadInitialState(ctx, api, resClient, ns, true)
			if err != nil {
				return err
			}
		}

		w, err := watchutil.NewRetryWatcher(resourceVersion, &cache.ListWatch{
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				res, err := resClient.Watch(ctx, options)
				if errors.IsNotFound(err) {
					c.stopWatching(api.GroupKind, ns)
				}
				return res, err
			},
		})
		if err != nil {
			return err
		}

		defer func() {
			w.Stop()
			resourceVersion = ""
		}()

		var watchResyncTimeoutCh <-chan time.Time
		if c.watchResyncTimeout > 0 {
			shouldResync := time.NewTimer(c.watchResyncTimeout)
			defer shouldResync.Stop()
			watchResyncTimeoutCh = shouldResync.C
		}

		for {
			select {
			// stop watching when parent context got cancelled
			case <-ctx.Done():
				return nil

			// re-synchronize API state and restart watch periodically
			case <-watchResyncTimeoutCh:
				return fmt.Errorf("Resyncing %s on %s due to timeout", api.GroupKind, c.config.Host)

			// re-synchronize API state and restart watch if retry watcher failed to continue watching using provided resource version
			case <-w.Done():
				return fmt.Errorf("Watch %s on %s has closed", api.GroupKind, c.config.Host)

			case event, ok := <-w.ResultChan():
				if !ok {
					return fmt.Errorf("Watch %s on %s has closed", api.GroupKind, c.config.Host)
				}

				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					return fmt.Errorf("Failed to convert to *unstructured.Unstructured: %v", event.Object)
				}

				log := c.log.WithValues(
					"event", event.Type,
					"groupKind", obj.GroupVersionKind().GroupKind().String(),
					"namespace", obj.GetNamespace(),
					"name", obj.GetName(),
				)
				start := time.Now()
				log.V(1).Info("Received event from ResultChan")

				c.processEvent(event.Type, obj)
				if kube.IsCRD(obj) {
					var resources []kube.APIResourceInfo
					crd := v1.CustomResourceDefinition{}
					err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &crd)
					if err != nil {
						c.log.Error(err, "Failed to extract CRD resources")
					}
					for _, v := range crd.Spec.Versions {
						resources = append(resources, kube.APIResourceInfo{
							GroupKind: schema.GroupKind{
								Group: crd.Spec.Group, Kind: crd.Spec.Names.Kind},
							GroupVersionResource: schema.GroupVersionResource{
								Group: crd.Spec.Group, Version: v.Name, Resource: crd.Spec.Names.Plural},
							Meta: metav1.APIResource{
								Group:        crd.Spec.Group,
								SingularName: crd.Spec.Names.Singular,
								Namespaced:   crd.Spec.Scope == v1.NamespaceScoped,
								Name:         crd.Spec.Names.Plural,
								Kind:         crd.Spec.Names.Singular,
								Version:      v.Name,
								ShortNames:   crd.Spec.Names.ShortNames,
							},
						})
					}

					if event.Type == watch.Deleted {
						for i := range resources {
							c.deleteAPIResource(resources[i])
						}
					} else {
						c.log.Info("Updating Kubernetes APIs, watches, and Open API schemas due to CRD event", "eventType", event.Type, "groupKind", crd.GroupVersionKind().GroupKind().String())
						// add new CRD's groupkind to c.apigroups
						if event.Type == watch.Added {
							for i := range resources {
								c.appendAPIResource(resources[i])
							}
						}
						err = runSynced(&c.lock, func() error {
							return c.startMissingWatches()
						})
						if err != nil {
							c.log.Error(err, "Failed to start missing watch")
						}
					}
					err = runSynced(&c.lock, func() error {
						openAPISchema, gvkParser, err := c.kubectl.LoadOpenAPISchema(c.config)
						if err != nil {
							return fmt.Errorf("failed to load open api schema while handling CRD change: %w", err)
						}
						if gvkParser != nil {
							c.gvkParser = gvkParser
						}
						c.openAPISchema = openAPISchema
						return nil
					})
					if err != nil {
						c.log.Error(err, "Failed to reload open api schema")
					}
				}

				duration := time.Since(start)
				log.V(1).Info("Handled event from ResultChan", "duration", duration.Milliseconds())
			}
		}
	})
}

func (c *clusterCache) processApi(client dynamic.Interface, api kube.APIResourceInfo, callback func(resClient dynamic.ResourceInterface, ns string) error) error {
	resClient := client.Resource(api.GroupVersionResource)
	switch {
	// if manage whole cluster or resource is cluster level and cluster resources enabled
	case len(c.namespaces) == 0 || (!api.Meta.Namespaced && c.clusterResources):
		return callback(resClient, "")
	// if manage some namespaces and resource is namespaced
	case len(c.namespaces) != 0 && api.Meta.Namespaced:
		for _, ns := range c.namespaces {
			err := callback(resClient.Namespace(ns), ns)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// isRestrictedResource checks if the kube api call is unauthorized or forbidden
func (c *clusterCache) isRestrictedResource(err error) bool {
	return c.respectRBAC != RespectRbacDisabled && (k8sErrors.IsForbidden(err) || k8sErrors.IsUnauthorized(err))
}

// checkPermission runs a self subject access review to check if the controller has permissions to list the resource
func (c *clusterCache) checkPermission(ctx context.Context, reviewInterface authType1.SelfSubjectAccessReviewInterface, api kube.APIResourceInfo) (keep bool, err error) {
	sar := &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: "*",
				Verb:      "list", // uses list verb to check for permissions
				Resource:  api.GroupVersionResource.Resource,
			},
		},
	}

	switch {
	// if manage whole cluster or resource is cluster level and cluster resources enabled
	case len(c.namespaces) == 0 || (!api.Meta.Namespaced && c.clusterResources):
		resp, err := reviewInterface.Create(ctx, sar, metav1.CreateOptions{})
		if err != nil {
			return false, err
		}
		if resp != nil && resp.Status.Allowed {
			return true, nil
		}
		// unsupported, remove from watch list
		return false, nil
	// if manage some namespaces and resource is namespaced
	case len(c.namespaces) != 0 && api.Meta.Namespaced:
		for _, ns := range c.namespaces {
			sar.Spec.ResourceAttributes.Namespace = ns
			resp, err := reviewInterface.Create(ctx, sar, metav1.CreateOptions{})
			if err != nil {
				return false, err
			}
			if resp != nil && resp.Status.Allowed {
				return true, nil
			} else {
				// unsupported, remove from watch list
				return false, nil
			}
		}
	}
	// checkPermission follows the same logic of determining namespace/cluster resource as the processApi function
	// so if neither of the cases match it means the controller will not watch for it so it is safe to return true.
	return true, nil
}

func (c *clusterCache) sync() error {
	c.log.Info("Start syncing cluster")

	c.log.V(1).Info("Closing event channel in sync method")
	c.closeEventChannelSafely()

	for i := range c.apisMeta {
		c.apisMeta[i].watchCancel()
	}
	c.apisMeta = make(map[schema.GroupKind]*apiMeta)
	c.resources = make(map[kube.ResourceKey]*Resource)
	c.namespacedResources = make(map[schema.GroupKind]bool)
	config := c.config
	version, err := c.kubectl.GetServerVersion(config)
	c.eventCh = make(chan eventMeta)

	if err != nil {
		return err
	}
	c.serverVersion = version
	apiResources, err := c.kubectl.GetAPIResources(config, false, NewNoopSettings())
	c.log.V(1).Info("sync(): API resources", "resources", apiResources)
	if err != nil {
		return err
	}
	c.apiResources = apiResources

	openAPISchema, gvkParser, err := c.kubectl.LoadOpenAPISchema(config)
	if err != nil {
		return fmt.Errorf("failed to load open api schema while syncing cluster cache: %w", err)
	}

	if gvkParser != nil {
		c.gvkParser = gvkParser
	}

	c.openAPISchema = openAPISchema

	apis, err := c.kubectl.GetAPIResources(c.config, true, c.settings.ResourcesFilter)
	c.log.V(1).Info("sync(): APIs", "apis", apis)

	if err != nil {
		return err
	}
	client, err := c.kubectl.NewDynamicClient(c.config)
	if err != nil {
		return err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	go c.processItems()

	lock := sync.Mutex{}
	err = kube.RunAllAsync(len(apis), func(i int) error {
		api := apis[i]

		lock.Lock()
		ctx, cancel := context.WithCancel(context.Background())
		info := &apiMeta{namespaced: api.Meta.Namespaced, watchCancel: cancel}
		c.apisMeta[api.GroupKind] = info
		c.namespacedResources[api.GroupKind] = api.Meta.Namespaced
		lock.Unlock()

		return c.processApi(client, api, func(resClient dynamic.ResourceInterface, ns string) error {
			resourceVersion, err := c.listResources(ctx, resClient, func(listPager *pager.ListPager) error {
				return listPager.EachListItem(context.Background(), metav1.ListOptions{}, func(obj runtime.Object) error {
					if un, ok := obj.(*unstructured.Unstructured); !ok {
						return fmt.Errorf("object %s/%s has an unexpected type", un.GroupVersionKind().String(), un.GetName())
					} else {
						lock.Lock()
						c.setNode(c.newResource(un))
						lock.Unlock()
					}
					return nil
				})
			})
			if err != nil {
				if c.isRestrictedResource(err) {
					keep := false
					if c.respectRBAC == RespectRbacStrict {
						k, permErr := c.checkPermission(ctx, clientset.AuthorizationV1().SelfSubjectAccessReviews(), api)
						if permErr != nil {
							return fmt.Errorf("failed to check permissions for resource %s: %w, original error=%v", api.GroupKind.String(), permErr, err.Error())
						}
						keep = k
					}
					// if we are not allowed to list the resource, remove it from the watch list
					if !keep {
						lock.Lock()
						delete(c.apisMeta, api.GroupKind)
						delete(c.namespacedResources, api.GroupKind)
						lock.Unlock()
						return nil
					}
				}
				return fmt.Errorf("failed to load initial state of resource %s: %w", api.GroupKind.String(), err)
			}

			go c.watchEvents(ctx, api, resClient, ns, resourceVersion)

			return nil
		})
	})

	if err != nil {
		c.log.Error(err, "Failed to sync cluster")
		return fmt.Errorf("failed to sync cluster %s: %v", c.config.Host, err)
	}

	c.log.Info("Cluster successfully synced")
	return nil
}

func (c *clusterCache) processItems() {
	log := c.log.WithValues(
		"fn", "processItems",
	)

	log.V(1).Info("Starting processing items")

	var items []eventMeta

	// Create a ticker that ticks every 1 second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		// Handle receiving items from the channel
		case item, ok := <-c.eventCh:
			if !ok {
				log.V(1).Info("Channel is closed. Exiting loop.")
				return
			}
			items = append(items, item)
			log.V(1).Info("Put item into items slice", "len", len(items))
		// Handle processing the items every 1 second
		case <-ticker.C:
			log.V(1).Info("Processing items", "len", len(items))
			if len(items) > 0 {
				func() {
					start := time.Now()
					c.lock.Lock()
					lockAcquired := time.Now()
					defer func() {
						c.lock.Unlock()
						lockReleased := time.Now()
						duration := lockReleased.Sub(lockAcquired)
						ms := duration.Milliseconds()
						log.V(1).Info(fmt.Sprintf("Lock released in %v ms", ms), "duration", ms)
						// Update the metric with the duration of the events processing
						for _, handler := range c.getProcessEventsHandlers() {
							handler(time.Since(start), len(items))
						}
					}()

					duration := lockAcquired.Sub(start)
					log.V(1).Info(fmt.Sprintf("Lock acquired in %v ms", duration.Milliseconds()), "duration", duration.Milliseconds())
					for _, h := range c.getResourceLockAcquireHandlers() {
						h(duration)
					}

					for _, item := range items {
						start := time.Now()
						key := kube.GetResourceKey(item.un)
						event := item.event

						ok, newRes, oldRes, ns := c.writeForResourceEvent(key, event, item.un)

						if ok {
							for _, h := range c.getResourceUpdatedHandlers() {
								h(newRes, oldRes, ns)
							}
						}

						duration := time.Since(start)
						// Update the metric with the duration of the event processing
						for _, handler := range c.getProcessEventHandlers() {
							handler(duration)
						}
					}
				}()

				log.V(1).Info("Processed items", "len", len(items))

				// Clear the array after processing
				items = make([]eventMeta, 0)
			} else {
				log.V(1).Info("No items to process", "len", len(items))
			}
		}
	}
}

// EnsureSynced checks cache state and synchronizes it if necessary
func (c *clusterCache) EnsureSynced() error {
	syncStatus := &c.syncStatus

	// first check if cluster is synced *without acquiring the full clusterCache lock*
	syncStatus.lock.Lock()
	if syncStatus.synced(c.clusterSyncRetryTimeout) {
		syncError := syncStatus.syncError
		syncStatus.lock.Unlock()
		return syncError
	}
	syncStatus.lock.Unlock() // release the lock, so that we can acquire the parent lock (see struct comment re: lock acquisition ordering)

	c.lock.Lock()
	defer c.lock.Unlock()
	syncStatus.lock.Lock()
	defer syncStatus.lock.Unlock()

	// before doing any work, check once again now that we have the lock, to see if it got
	// synced between the first check and now
	if syncStatus.synced(c.clusterSyncRetryTimeout) {
		return syncStatus.syncError
	}
	err := c.sync()
	syncTime := time.Now()
	syncStatus.syncTime = &syncTime
	syncStatus.syncError = err
	return syncStatus.syncError
}

func (c *clusterCache) FindResources(namespace string, predicates ...func(r *Resource) bool) map[kube.ResourceKey]*Resource {
	c.lock.RLock()
	defer c.lock.RUnlock()
	result := map[kube.ResourceKey]*Resource{}
	resources := map[kube.ResourceKey]*Resource{}
	if namespace != "" {
		if ns, ok := c.nsIndex[namespace]; ok {
			resources = ns
		}
	} else {
		resources = c.resources
	}

	for k := range resources {
		r := resources[k]
		matches := true
		for i := range predicates {
			if !predicates[i](r) {
				matches = false
				break
			}
		}

		if matches {
			result[k] = r
		}
	}
	return result
}

// IterateHierarchy iterates resource tree starting from the specified top level resource and executes callback for each resource in the tree
func (c *clusterCache) IterateHierarchy(key kube.ResourceKey, action func(resource *Resource, namespaceResources map[kube.ResourceKey]*Resource) bool) {
	log := c.log.WithValues(
		"fn", "IterateHierarchy",
		"resource", key,
	)

	log.V(1).Info("Iterating hierarchy")

	start := time.Now()
	c.lock.RLock()
	lockAcquired := time.Now()
	defer func() {
		c.lock.RUnlock()
		lockReleased := time.Now()
		ms := lockReleased.Sub(lockAcquired).Milliseconds()
		log.V(1).Info(fmt.Sprintf("Lock released in %v ms", ms), "duration", ms)

		// Commented it, since going to collect metrics from IterateHierarchyV2
		// for _, handler := range c.getIterateHierarchyHandlers() {
		// 	handler(key.Kind, key.Namespace, lockReleased.Sub(start), lockAcquired.Sub(start))
		// }
	}()

	duration := lockAcquired.Sub(start)
	log.V(1).Info(fmt.Sprintf("Lock acquired in %v ms", duration.Milliseconds()), "duration", duration.Milliseconds())

	if res, ok := c.resources[key]; ok {
		nsNodes := c.nsIndex[key.Namespace]
		if !action(res, nsNodes) {
			return
		}
		childrenByUID := make(map[types.UID][]*Resource)
		for _, child := range nsNodes {
			if res.isParentOf(child) {
				childrenByUID[child.Ref.UID] = append(childrenByUID[child.Ref.UID], child)
			}
		}
		// make sure children has no duplicates
		for _, children := range childrenByUID {
			if len(children) > 0 {
				// The object might have multiple children with the same UID (e.g. replicaset from apps and extensions group). It is ok to pick any object but we need to make sure
				// we pick the same child after every refresh.
				sort.Slice(children, func(i, j int) bool {
					key1 := children[i].ResourceKey()
					key2 := children[j].ResourceKey()
					return strings.Compare(key1.String(), key2.String()) < 0
				})
				child := children[0]
				if action(child, nsNodes) {
					child.iterateChildren(nsNodes, map[kube.ResourceKey]bool{res.ResourceKey(): true}, func(err error, child *Resource, namespaceResources map[kube.ResourceKey]*Resource) bool {
						if err != nil {
							c.log.V(2).Info(err.Error())
							return false
						}
						return action(child, namespaceResources)
					})
				}
			}
		}
	}
}

// IterateHierarchy iterates resource tree starting from the specified top level resources and executes callback for each resource in the tree
func (c *clusterCache) IterateHierarchyV2(keys []kube.ResourceKey, action func(resource *Resource, namespaceResources map[kube.ResourceKey]*Resource) bool) {
	log := c.log.WithValues("fn", "IterateHierarchyV2")

	log.V(1).Info("Iterating hierarchy")

	start := time.Now()
	c.lock.RLock()
	lockAcquired := time.Now()

	defer func() {
		c.lock.RUnlock()
		lockReleased := time.Now()
		ms := lockReleased.Sub(lockAcquired).Milliseconds()
		log.V(1).Info(fmt.Sprintf("Lock released in %v ms", ms), "duration", ms)

		for _, handler := range c.getIterateHierarchyHandlers() {
			handler("", "", lockReleased.Sub(start), lockAcquired.Sub(start))
		}

		duration := time.Since(start)
		log.V(1).Info("Iterated hierarchy", "duration", duration.Milliseconds())
	}()

	duration := lockAcquired.Sub(start)
	log.V(1).Info(fmt.Sprintf("Lock acquired in %v ms", duration.Milliseconds()), "duration", duration.Milliseconds())

	keysPerNamespace := make(map[string][]kube.ResourceKey)
	for _, key := range keys {
		_, ok := c.resources[key]
		if !ok {
			continue
		}
		keysPerNamespace[key.Namespace] = append(keysPerNamespace[key.Namespace], key)
	}
	for namespace, namespaceKeys := range keysPerNamespace {
		nsNodes := c.nsIndex[namespace]
		graph := buildGraph(nsNodes)
		visited := make(map[kube.ResourceKey]int)
		for _, key := range namespaceKeys {
			visited[key] = 0
		}
		for _, key := range namespaceKeys {
			// The check for existence of key is done above.
			res := c.resources[key]
			if visited[key] == 2 || !action(res, nsNodes) {
				continue
			}
			visited[key] = 1
			if _, ok := graph[key]; ok {
				for _, child := range graph[key] {
					if visited[child.ResourceKey()] == 0 && action(child, nsNodes) {
						child.iterateChildrenV2(graph, nsNodes, visited, func(err error, child *Resource, namespaceResources map[kube.ResourceKey]*Resource) bool {
							if err != nil {
								c.log.V(2).Info(err.Error())
								return false
							}
							return action(child, namespaceResources)
						})
					}
				}
			}
			visited[key] = 2
		}
	}
}

func buildGraph(nsNodes map[kube.ResourceKey]*Resource) map[kube.ResourceKey]map[types.UID]*Resource {
	// Prepare to construct a graph
	nodesByUID := make(map[types.UID][]*Resource, len(nsNodes))
	for _, node := range nsNodes {
		nodesByUID[node.Ref.UID] = append(nodesByUID[node.Ref.UID], node)
	}

	// In graph, they key is the parent and the value is a list of children.
	graph := make(map[kube.ResourceKey]map[types.UID]*Resource)

	// Loop through all nodes, calling each one "childNode," because we're only bothering with it if it has a parent.
	for _, childNode := range nsNodes {
		for i, ownerRef := range childNode.OwnerRefs {
			// First, backfill UID of inferred owner child references.
			if ownerRef.UID == "" {
				group, err := schema.ParseGroupVersion(ownerRef.APIVersion)
				if err != nil {
					// APIVersion is invalid, so we couldn't find the parent.
					continue
				}
				graphKeyNode, ok := nsNodes[kube.ResourceKey{Group: group.Group, Kind: ownerRef.Kind, Namespace: childNode.Ref.Namespace, Name: ownerRef.Name}]
				if ok {
					ownerRef.UID = graphKeyNode.Ref.UID
					childNode.OwnerRefs[i] = ownerRef
				} else {
					// No resource found with the given graph key, so move on.
					continue
				}
			}

			// Now that we have the UID of the parent, update the graph.
			uidNodes, ok := nodesByUID[ownerRef.UID]
			if ok {
				for _, uidNode := range uidNodes {
					// Update the graph for this owner to include the child.
					if _, ok := graph[uidNode.ResourceKey()]; !ok {
						graph[uidNode.ResourceKey()] = make(map[types.UID]*Resource)
					}
					r, ok := graph[uidNode.ResourceKey()][childNode.Ref.UID]
					if !ok {
						graph[uidNode.ResourceKey()][childNode.Ref.UID] = childNode
					} else if r != nil {
						// The object might have multiple children with the same UID (e.g. replicaset from apps and extensions group).
						// It is ok to pick any object, but we need to make sure we pick the same child after every refresh.
						key1 := r.ResourceKey()
						key2 := childNode.ResourceKey()
						if strings.Compare(key1.String(), key2.String()) > 0 {
							graph[uidNode.ResourceKey()][childNode.Ref.UID] = childNode
						}
					}
				}
			}
		}
	}
	return graph
}

// IsNamespaced answers if specified group/kind is a namespaced resource API or not
func (c *clusterCache) IsNamespaced(gk schema.GroupKind) (bool, error) {
	if isNamespaced, ok := c.namespacedResources[gk]; ok {
		return isNamespaced, nil
	}
	return false, errors.NewNotFound(schema.GroupResource{Group: gk.Group}, "")
}

func (c *clusterCache) managesNamespace(namespace string) bool {
	for _, ns := range c.namespaces {
		if ns == namespace {
			return true
		}
	}
	return false
}

// GetManagedLiveObjs helps finding matching live K8S resources for a given resources list.
// The function returns all resources from cache for those `isManaged` function returns true and resources
// specified in targetObjs list.
func (c *clusterCache) GetManagedLiveObjs(targetObjs []*unstructured.Unstructured, isManaged func(r *Resource) bool) (map[kube.ResourceKey]*unstructured.Unstructured, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for _, o := range targetObjs {
		if len(c.namespaces) > 0 {
			if o.GetNamespace() == "" && !c.clusterResources {
				return nil, fmt.Errorf("Cluster level %s %q can not be managed when in namespaced mode", o.GetKind(), o.GetName())
			} else if o.GetNamespace() != "" && !c.managesNamespace(o.GetNamespace()) {
				return nil, fmt.Errorf("Namespace %q for %s %q is not managed", o.GetNamespace(), o.GetKind(), o.GetName())
			}
		}
	}

	managedObjs := make(map[kube.ResourceKey]*unstructured.Unstructured)
	// iterate all objects in live state cache to find ones associated with app
	for key, o := range c.resources {
		if isManaged(o) && o.Resource != nil && len(o.OwnerRefs) == 0 {
			managedObjs[key] = o.Resource
		}
	}
	// but are simply missing our label
	lock := &sync.Mutex{}
	err := kube.RunAllAsync(len(targetObjs), func(i int) error {
		targetObj := targetObjs[i]
		key := kube.GetResourceKey(targetObj)
		lock.Lock()
		managedObj := managedObjs[key]
		lock.Unlock()

		if managedObj == nil {
			if existingObj, exists := c.resources[key]; exists {
				if existingObj.Resource != nil {
					managedObj = existingObj.Resource
				} else {
					var err error
					managedObj, err = c.kubectl.GetResource(context.TODO(), c.config, targetObj.GroupVersionKind(), existingObj.Ref.Name, existingObj.Ref.Namespace)
					if err != nil {
						if errors.IsNotFound(err) {
							return nil
						}
						return err
					}
				}
			} else if _, watched := c.apisMeta[key.GroupKind()]; !watched {
				var err error
				managedObj, err = c.kubectl.GetResource(context.TODO(), c.config, targetObj.GroupVersionKind(), targetObj.GetName(), targetObj.GetNamespace())
				if err != nil {
					if errors.IsNotFound(err) {
						return nil
					}
					return err
				}
			}
		}

		if managedObj != nil {
			converted, err := c.kubectl.ConvertToVersion(managedObj, targetObj.GroupVersionKind().Group, targetObj.GroupVersionKind().Version)
			if err != nil {
				// fallback to loading resource from kubernetes if conversion fails
				c.log.V(1).Info(fmt.Sprintf("Failed to convert resource: %v", err))
				managedObj, err = c.kubectl.GetResource(context.TODO(), c.config, targetObj.GroupVersionKind(), managedObj.GetName(), managedObj.GetNamespace())
				if err != nil {
					if errors.IsNotFound(err) {
						return nil
					}
					return err
				}
			} else {
				managedObj = converted
			}
			lock.Lock()
			managedObjs[key] = managedObj
			lock.Unlock()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return managedObjs, nil
}

func (c *clusterCache) processEvent(event watch.EventType, un *unstructured.Unstructured) {
	log := c.log.WithValues(
		"fn", "processEvent",
		"event", event,
		"kind", un.GetKind(),
		"namespace", un.GetNamespace(),
		"name", un.GetName(),
	)

	log.V(1).Info("Processing event")

	for _, h := range c.getEventHandlers() {
		h(event, un)
	}
	key := kube.GetResourceKey(un)
	if event == watch.Modified && skipAppRequeuing(key) {
		log.V(1).Info("Skipping requeue for resource")
		return
	}

	c.eventCh <- eventMeta{event, un}
	log.V(1).Info("Sent event to channel")
}

func (c *clusterCache) writeForResourceEvent(key kube.ResourceKey, event watch.EventType, un *unstructured.Unstructured) (bool, *Resource, *Resource, map[kube.ResourceKey]*Resource) {
	existingNode, exists := c.resources[key]
	if event == watch.Deleted {
		if exists {
			ok, existing, ns := c.removeNode(key)
			return ok, nil, existing, ns
		} else {
			return false, nil, nil, nil
		}
	} else {
		newRes, ns := c.updateNode(c.newResource(un))
		return true, newRes, existingNode, ns
	}
}

func (c *clusterCache) updateNode(newRes *Resource) (*Resource, map[kube.ResourceKey]*Resource) {
	c.setNode(newRes)
	return newRes, c.nsIndex[newRes.Ref.Namespace]
}

func (c *clusterCache) onNodeUpdated(oldRes *Resource, newRes *Resource) {
	_, ns := c.updateNode(newRes)
	for _, h := range c.getResourceUpdatedHandlers() {
		h(newRes, oldRes, ns)
	}
}

func (c *clusterCache) removeNode(key kube.ResourceKey) (bool, *Resource, map[kube.ResourceKey]*Resource) {
	existing, ok := c.resources[key]
	if ok {
		delete(c.resources, key)
		ns, ok := c.nsIndex[key.Namespace]
		if ok {
			delete(ns, key)
			if len(ns) == 0 {
				delete(c.nsIndex, key.Namespace)
			}
			// remove ownership references from children with inferred references
			if existing.isInferredParentOf != nil {
				for k, v := range ns {
					if mightHaveInferredOwner(v) && existing.isInferredParentOf(k) {
						v.setOwnerRef(existing.toOwnerRef(), false)
					}
				}
			}
		}
		return true, existing, ns
	}
	return false, nil, nil
}

func (c *clusterCache) onNodeRemoved(key kube.ResourceKey) {
	ok, existing, ns := c.removeNode(key)
	if ok {
		for _, h := range c.getResourceUpdatedHandlers() {
			h(nil, existing, ns)
		}
	}
}

var (
	ignoredRefreshResources = map[string]bool{
		"/" + kube.EndpointsKind: true,
	}
)

// GetClusterInfo returns cluster cache statistics
func (c *clusterCache) GetClusterInfo() ClusterInfo {
	c.lock.RLock()
	defer c.lock.RUnlock()
	c.syncStatus.lock.Lock()
	defer c.syncStatus.lock.Unlock()

	return ClusterInfo{
		APIsCount:         len(c.apisMeta),
		K8SVersion:        c.serverVersion,
		ResourcesCount:    len(c.resources),
		Server:            c.config.Host,
		LastCacheSyncTime: c.syncStatus.syncTime,
		SyncError:         c.syncStatus.syncError,
		APIResources:      c.apiResources,
	}
}

// skipAppRequeuing checks if the object is an API type which we want to skip requeuing against.
// We ignore API types which have a high churn rate, and/or whose updates are irrelevant to the app
func skipAppRequeuing(key kube.ResourceKey) bool {
	return ignoredRefreshResources[key.Group+"/"+key.Kind]
}

func (c *clusterCache) closeEventChannelSafely() {
	defer func() {
		if r := recover(); r != nil {
			c.log.V(1).Info("Recovered from panic.", "error", r)
		}
	}()

	close(c.eventCh)
	c.log.V(1).Info("Closed event channel")
}
