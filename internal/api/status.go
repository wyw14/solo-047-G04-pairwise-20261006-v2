package api

import (
	"sort"
	"time"

	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/topology"
)

type SystemStatus struct {
	GeneratedAt time.Time                `json:"generated_at"`
	Spans       []span.Snapshot          `json:"spans"`
	Routes      []route.Snapshot         `json:"routes"`
	Repeaters   []repeater.Snapshot      `json:"repeaters"`
	Thermal     []repeater.ThermalStatus `json:"thermal"`
	Topologies  []topology.Snapshot      `json:"topologies"`
	EventCount  int                      `json:"event_count"`
	Integrity   []IntegrityFinding       `json:"integrity"`
	Warnings    []string                 `json:"warnings"`
}

type EventPage struct {
	Total  int            `json:"total"`
	Offset int            `json:"offset"`
	Limit  int            `json:"limit"`
	Items  []store.Record `json:"items"`
}

func (s *Services) Status() (SystemStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := SystemStatus{GeneratedAt: s.Clock.Now().UTC()}
	spanIDs := make([]string, 0, len(s.Spans))
	for id := range s.Spans {
		spanIDs = append(spanIDs, id)
	}
	sort.Strings(spanIDs)
	for _, id := range spanIDs {
		result.Spans = append(result.Spans, s.Spans[id].Snapshot())
		topologySnapshot, err := s.Topology.Snapshot(id)
		if err != nil {
			result.Warnings = append(result.Warnings, err.Error())
			continue
		}
		result.Topologies = append(result.Topologies, topologySnapshot)
	}

	routeIDs := make([]string, 0, len(s.Routes))
	for id := range s.Routes {
		routeIDs = append(routeIDs, id)
	}
	sort.Strings(routeIDs)
	for _, id := range routeIDs {
		result.Routes = append(result.Routes, s.Routes[id].Snapshot())
	}

	repeaterIDs := make([]string, 0, len(s.Repeaters))
	for id := range s.Repeaters {
		repeaterIDs = append(repeaterIDs, id)
	}
	sort.Strings(repeaterIDs)
	for _, id := range repeaterIDs {
		result.Repeaters = append(result.Repeaters, s.Repeaters[id].Snapshot())
		if s.ThermalSafety != nil {
			result.Thermal = append(result.Thermal, s.ThermalSafety.Status(id))
		}
	}
	records, err := s.Journal.Records()
	if err != nil {
		return SystemStatus{}, err
	}
	result.EventCount = len(records)
	result.Integrity = auditIntegrity(result.Spans, result.Routes, result.Topologies)
	for _, finding := range result.Integrity {
		if finding.Severity == "critical" {
			result.Warnings = append(result.Warnings, finding.Message)
		}
	}
	return result, nil
}

func (s *Services) Events(kind, entityID string, offset, limit int) (EventPage, error) {
	records, err := s.Journal.Records()
	if err != nil {
		return EventPage{}, err
	}
	filtered := make([]store.Record, 0, len(records))
	for _, record := range records {
		if kind != "" && record.Kind != kind {
			continue
		}
		if entityID != "" && record.EntityID != entityID {
			continue
		}
		filtered = append(filtered, record)
	}
	if offset < 0 {
		offset = 0
	}
	if limit < 1 || limit > 200 {
		limit = 50
	}
	end := offset + limit
	if offset > len(filtered) {
		offset = len(filtered)
	}
	if end > len(filtered) {
		end = len(filtered)
	}
	return EventPage{
		Total: len(filtered), Offset: offset, Limit: limit,
		Items: append([]store.Record(nil), filtered[offset:end]...),
	}, nil
}
