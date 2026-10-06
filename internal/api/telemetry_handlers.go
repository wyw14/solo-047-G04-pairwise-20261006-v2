package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/telemetry"
)

func (s *Server) ingestTelemetry(writer http.ResponseWriter, request *http.Request) {
	repeaterID := chi.URLParam(request, "id")
	if _, err := s.services.Repeater(repeaterID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input TelemetryRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	sample := telemetry.Sample{
		ID: s.services.IDs.New("sample"), AssetID: repeaterID, Kind: telemetry.Kind(input.Kind),
		Value: input.Value, Unit: input.Unit, Sequence: input.Sequence, Generation: input.Generation,
		ObservedAt: s.services.Clock.Now().UTC(),
	}
	if err := s.services.Telemetry.Apply(request.Context(), sample); err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("telemetry", sample.ID, sample); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, sample)
}

func (s *Server) getRepeater(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}

func (s *Server) getRepeaterThermal(writer http.ResponseWriter, request *http.Request) {
	repeaterID := chi.URLParam(request, "id")
	if _, err := s.services.Repeater(repeaterID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	if s.services.Thermal == nil {
		writeError(writer, http.StatusServiceUnavailable, errors.New("thermal safety is not configured"))
		return
	}
	assessment, err := s.services.Thermal.Assess(repeaterID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, assessment)
}
