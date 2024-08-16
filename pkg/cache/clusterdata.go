package cache

import (
	"sync"

	"github.com/argoproj/gitops-engine/pkg/utils/kube"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func NewApisMetaMap() ApisMetaMap {
	return ApisMetaMap{data: NewStringer[schema.GroupKind, *apiMeta]()}
}

// ApisMetaMap is thread-safe map of apiMeta
type ApisMetaMap struct {
	data ConcurrentMap[schema.GroupKind, *apiMeta]
}

func (m *ApisMetaMap) Load(gk schema.GroupKind) (*apiMeta, bool) {
	val, ok := m.data.Get(gk)
	if !ok {
		return nil, false
	}
	return val, true
}

func (m *ApisMetaMap) LoadOrStore(key schema.GroupKind, val *apiMeta) (*apiMeta, bool) {
	actual, ok := m.data.Get(key)
	if !ok {
		m.data.Set(key, val)
		return val, false
	}
	return actual, true
}

func (m *ApisMetaMap) Store(gk schema.GroupKind, meta *apiMeta) {
	m.data.Set(gk, meta)
}

func (m *ApisMetaMap) Delete(gk schema.GroupKind) {
	m.data.Remove(gk)
}

func (m *ApisMetaMap) Range(fn func(key schema.GroupKind, value *apiMeta)) {
	m.data.IterCb(fn)
}

func (m *ApisMetaMap) Len() int {
	return m.data.Count()
}

func NewResourceMap() ResourceMap {
	return ResourceMap{data: NewStringer[kube.ResourceKey, *Resource]()}
}

type ResourceMap struct {
	data ConcurrentMap[kube.ResourceKey, *Resource]
}

func (m *ResourceMap) Load(key kube.ResourceKey) (*Resource, bool) {
	val, ok := m.data.Get(key)
	if !ok {
		return nil, false
	}
	return val, true
}

func (m *ResourceMap) LoadOrStore(key kube.ResourceKey, val *Resource) (*Resource, bool) {
	actual, ok := m.data.Get(key)
	if !ok {
		m.data.Set(key, val)
		return val, false
	}
	return actual, true
}

func (m *ResourceMap) Store(key kube.ResourceKey, resource *Resource) {
	m.data.Set(key, resource)
}

func (m *ResourceMap) Delete(key kube.ResourceKey) {
	m.data.Remove(key)
}

func (m *ResourceMap) Range(fn func(key kube.ResourceKey, value *Resource)) {
	m.data.IterCb(fn)
}

func (m *ResourceMap) Len() int {
	return m.data.Count()
}

func (m *ResourceMap) All() map[kube.ResourceKey]*Resource {
	return m.data.Items()
}

func NewAPIResourcesInfoList() APIResourcesInfoList {
	return APIResourcesInfoList{}
}

type APIResourcesInfoList struct {
	lock sync.RWMutex
	list []kube.APIResourceInfo
}

func (l *APIResourcesInfoList) Add(info kube.APIResourceInfo) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.list = append(l.list, info)
}

func (l *APIResourcesInfoList) Get() []kube.APIResourceInfo {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.list
}

func (l *APIResourcesInfoList) Len() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return len(l.list)
}

// Remove return true if item exited
func (l *APIResourcesInfoList) Remove(info kube.APIResourceInfo) bool {
	l.lock.Lock()
	defer l.lock.Unlock()
	for i, v := range l.list {
		if v.GroupKind == info.GroupKind && v.GroupVersionResource.Version == info.GroupVersionResource.Version {
			l.list = append(l.list[:i], l.list[i+1:]...)
			return true
		}
	}
	return false
}

func (l *APIResourcesInfoList) Clear() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.list = []kube.APIResourceInfo{}
}

func (l *APIResourcesInfoList) GetReferrerList() []kube.APIResourceInfo {
	return l.list
}

// All return all of the original resources in the map, this maybe cause pointer leaks, depreacated.
// TODO remove
func (l *APIResourcesInfoList) All() []kube.APIResourceInfo {
	snapshot := make([]kube.APIResourceInfo, len(l.list))
	copy(snapshot, l.list)
	return snapshot
}

// AddIfAbsent return true if added, deprecated O(N)
func (l *APIResourcesInfoList) AddIfAbsent(info kube.APIResourceInfo) bool {
	l.lock.Lock()
	defer l.lock.Unlock()
	for _, v := range l.list {
		if v.GroupKind == info.GroupKind && v.GroupVersionResource.Version == info.GroupVersionResource.Version {
			return false
		}
	}
	l.list = append(l.list, info)
	return true
}

func NewNamespaceResourcesMap() NamespaceResourcesMap {
	return NamespaceResourcesMap{data: New[*ResourceMap]()}
}

type NamespaceResourcesMap struct {
	data ConcurrentMap[string, *ResourceMap]
}

func (m *NamespaceResourcesMap) Load(key string) (*ResourceMap, bool) {
	val, ok := m.data.Get(key)
	if !ok {
		return nil, false
	}
	return val, true
}

func (m *NamespaceResourcesMap) LoadOrStore(key string, val *ResourceMap) (*ResourceMap, bool) {
	actual, ok := m.data.Get(key)
	if !ok {
		m.data.Set(key, val)
		return val, false
	}
	return actual, true
}

func (m *NamespaceResourcesMap) Store(key string, resource *ResourceMap) {
	m.data.Set(key, resource)
}

func (m *NamespaceResourcesMap) Delete(key string) {
	m.data.Remove(key)
}

func (m *NamespaceResourcesMap) Range(fn func(key string, value *ResourceMap)) {
	m.data.IterCb(fn)
}

func (m *NamespaceResourcesMap) Len() int {
	return m.data.Count()
}

func NewGroupKindBoolMap() GroupKindBoolMap {
	return GroupKindBoolMap{data: NewStringer[schema.GroupKind, bool]()}
}

type GroupKindBoolMap struct {
	data ConcurrentMap[schema.GroupKind, bool]
}

func (m *GroupKindBoolMap) Load(key schema.GroupKind) (bool, bool) {
	val, ok := m.data.Get(key)
	if !ok {
		return false, false
	}
	return val, true
}

func (m *GroupKindBoolMap) LoadOrStore(key schema.GroupKind, val bool) (bool, bool) {
	actual, ok := m.data.Get(key)
	if !ok {
		m.data.Set(key, val)
		return val, false
	}
	return actual, true
}

func (m *GroupKindBoolMap) Store(key schema.GroupKind, resource bool) {
	m.data.Set(key, resource)
}

func (m *GroupKindBoolMap) Delete(key schema.GroupKind) {
	m.data.Remove(key)
}

func (m *GroupKindBoolMap) Range(fn func(key schema.GroupKind, value bool)) {
	m.data.IterCb(fn)
}

func (m *GroupKindBoolMap) Len() int {
	return m.data.Count()
}

func (m *GroupKindBoolMap) Reload(resources map[schema.GroupKind]bool) {
	m.data.Clear()
	m.data.MSet(resources)
}

type StringList struct {
	lock sync.RWMutex
	list []string
}

func (l *StringList) Add(s string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.list = append(l.list, s)
}

func (l *StringList) Remove(s string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	for i, v := range l.list {
		if v == s {
			l.list = append(l.list[:i], l.list[i+1:]...)
			return
		}
	}
}

func (l *StringList) Range(fn func(string) bool) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	for _, v := range l.list {
		if !fn(v) {
			return
		}
	}
}

func (l *StringList) Contains(s string) bool {
	l.lock.RLock()
	defer l.lock.RUnlock()
	for _, v := range l.list {
		if v == s {
			return true
		}
	}
	return false
}

func (l *StringList) Len() int {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return len(l.list)
}

func (l *StringList) List() []string {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.list
}

func (l *StringList) Clear() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.list = []string{}
}
