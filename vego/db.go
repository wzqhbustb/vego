package vego

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DB is the unified database interface for vector search
type DB struct {
	config      *Config
	path        string                 // Database directory path
	collections map[string]*Collection // Collection name -> Collection

	mu     sync.RWMutex
	closed bool
}

// Open opens or creates a database at the given path
func Open(path string, opts ...Option) (*DB, error) {
	config := DefaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	// Ensure directory exists
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	db := &DB{
		config:      config,
		path:        path,
		collections: make(map[string]*Collection),
	}

	// Load existing collections
	if err := db.loadCollections(); err != nil {
		return nil, fmt.Errorf("failed to load collections: %w", err)
	}

	return db, nil
}

// Close closes the database and all collections
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	var errs []error
	for name, coll := range db.collections {
		if err := coll.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close collection %s: %w", name, err))
		}
	}

	db.closed = true
	if len(errs) > 0 {
		return fmt.Errorf("errors closing collections: %v", errs)
	}
	return nil
}

// Collection returns a collection by name, creates if not exists
func (db *DB) Collection(name string) (*Collection, error) {
	db.mu.RLock()
	coll, exists := db.collections[name]
	db.mu.RUnlock()

	if exists {
		return coll, nil
	}

	// Create new collection
	db.mu.Lock()
	defer db.mu.Unlock()

	// Double check
	if coll, exists := db.collections[name]; exists {
		return coll, nil
	}

	coll, err := db.createCollection(name)
	if err != nil {
		return nil, err
	}

	db.collections[name] = coll
	return coll, nil
}

// DropCollection removes a collection and all its data
func (db *DB) DropCollection(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	coll, exists := db.collections[name]
	if !exists {
		return fmt.Errorf("collection %s not found", name)
	}

	if err := coll.Drop(); err != nil {
		return fmt.Errorf("failed to drop collection: %w", err)
	}

	delete(db.collections, name)
	return nil
}

// Collections returns list of collection names
func (db *DB) Collections() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.collections))
	for name := range db.collections {
		names = append(names, name)
	}
	return names
}

func (db *DB) createCollection(name string) (*Collection, error) {
	collPath := filepath.Join(db.path, name)
	return NewCollection(name, collPath, db.config)
}

func (db *DB) loadCollections() error {
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		coll, err := db.createCollection(entry.Name())
		if err != nil {
			return fmt.Errorf("load collection %s: %w", entry.Name(), err)
		}

		db.collections[entry.Name()] = coll
	}

	return nil
}
