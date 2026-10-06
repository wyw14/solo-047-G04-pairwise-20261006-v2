package telemetry

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type Kind string

const (
	KindOpticalPower Kind = "optical_power"
	KindErrorRate    Kind = "error_rate"
	KindCurrent      Kind = "current"
	KindVoltage      Kind = "voltage"
	KindTemperature  Kind = "temperature"
	KindHeartbeat    Kind = "heartbeat"
)

type Sample struct {
	ID         string    `json:"id"`
	AssetID    string    `json:"asset_id"`
	Kind       Kind      `json:"kind"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	Sequence   uint64    `json:"sequence"`
	Generation uint64    `json:"generation"`
	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
}

func (s Sample) Validate() error {
	if s.ID == "" || s.AssetID == "" || s.Kind == "" {
		return errors.New("sample identity, asset and kind are required")
	}
	if s.Sequence == 0 || s.ObservedAt.IsZero() {
		return errors.New("sample sequence and observed time are required")
	}
	return nil
}

type Repository struct {
	mu      sync.RWMutex
	samples map[string][]Sample
}

func NewRepository() *Repository {
	return &Repository{samples: make(map[string][]Sample)}
}

func (r *Repository) Add(sample Sample) error {
	if err := sample.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	items := append(r.samples[sample.AssetID], sample)
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Sequence == items[j].Sequence {
			return items[i].ObservedAt.Before(items[j].ObservedAt)
		}
		return items[i].Sequence < items[j].Sequence
	})
	r.samples[sample.AssetID] = items
	return nil
}

func (r *Repository) History(assetID string) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := r.samples[assetID]
	result := make([]Sample, len(items))
	copy(result, items)
	return result
}

func (r *Repository) Latest(assetID string, kind Kind) (Sample, bool) {
	items := r.History(assetID)
	for index := len(items) - 1; index >= 0; index-- {
		if items[index].Kind == kind {
			return items[index], true
		}
	}
	return Sample{}, false
}
