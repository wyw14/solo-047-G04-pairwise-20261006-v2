package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// getRepeaterThermal 返回中继站热安全状态：降额系数、停用/恢复判定与不可判定原因。
func (s *Server) getRepeaterThermal(writer http.ResponseWriter, request *http.Request) {
	repeaterID := chi.URLParam(request, "id")
	if _, err := s.services.Repeater(repeaterID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	if s.services.ThermalSafety == nil {
		writeJSON(writer, http.StatusServiceUnavailable, ErrorResponse{Error: "thermal safety module unavailable"})
		return
	}
	writeJSON(writer, http.StatusOK, s.services.ThermalSafety.Status(repeaterID))
}

// refreshRepeaterThermal 用当前时钟重新执行一次过期/阈值判定（轮询入口）。
func (s *Server) refreshRepeaterThermal(writer http.ResponseWriter, request *http.Request) {
	repeaterID := chi.URLParam(request, "id")
	if _, err := s.services.Repeater(repeaterID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	if s.services.ThermalSafety == nil {
		writeJSON(writer, http.StatusServiceUnavailable, ErrorResponse{Error: "thermal safety module unavailable"})
		return
	}
	if err := s.services.ThermalSafety.Refresh(request.Context(), repeaterID); err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusOK, s.services.ThermalSafety.Status(repeaterID))
}
