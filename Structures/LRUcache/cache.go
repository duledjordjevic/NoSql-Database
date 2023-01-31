// Least recently used algorithm
package lru

import (
	"container/list"
	"fmt"
)

// Need OneElement so we can convert from Element to []byte
type OneElement struct {
	Key   string
	Value []byte
}

type LRUCache struct {
	ListOfElements *list.List
	MapOfElements  map[string]*list.Element
	CacheCapacity  uint
}

// Constructor - given capacity of cache
func NewLRUCache(capacity uint) *LRUCache {

	if capacity == 0 {
		panic("Kapacitet ne moze biti 0!")
	}

	mapCache := make(map[string]*list.Element, capacity)
	listOfElements := list.New()

	return &LRUCache{
		ListOfElements: listOfElements,
		MapOfElements:  mapCache,
		CacheCapacity:  capacity}

}

// Just for testing
func (lru *LRUCache) PrintList() {

	fmt.Println("List of elements")
	for e := lru.ListOfElements.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
}

// Add element to cache
func (lru *LRUCache) AddElement(key string, value []byte) {

	result, present := lru.MapOfElements[key]
	if present {
		// the new newest element
		newElement := OneElement{Key: key, Value: value}
		result.Value = newElement
		lru.ListOfElements.MoveToFront(result)
	}

	// if not present add el
	newElement := OneElement{Key: key, Value: value}
	// returns him self but type *Element
	newElementTypeElement := lru.ListOfElements.PushFront(newElement)
	lru.MapOfElements[key] = newElementTypeElement

	// check if full cache
	if lru.ListOfElements.Len() > int(lru.CacheCapacity) {

		// last element in list is one to be deleted
		elementToDelete := lru.ListOfElements.Back()
		lru.ListOfElements.Remove(elementToDelete)
		delete(lru.MapOfElements, elementToDelete.Value.(OneElement).Key)
	}
}

// Remove element from cache - if we need to DELETE, to keep data accurate
func (lru *LRUCache) RemoveElement(key string) {

	result, present := lru.MapOfElements[key]
	if present {
		// the new newest element
		lru.ListOfElements.Remove(result)
		delete(lru.MapOfElements, key)
	}

	// else don't have to do anything because element is not in cache
}

// Get element from cache, nil if
func (lru *LRUCache) GetElement(key string) (bool, []byte) {

	result, present := lru.MapOfElements[key]
	if present {
		// the new newest element
		lru.ListOfElements.MoveToFront(result)
		return true, result.Value.(OneElement).Value
	}

	return false, nil
}
