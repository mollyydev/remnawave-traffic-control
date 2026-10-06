package api

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mollyydev/remnawave-traffic-control/internal/store"
)

type Server struct {
	store            *store.Store
	db               *pgxpool.Pool
	remnaDB          *pgxpool.Pool
	apiKey           string
	events           store.EventOptions
	enforceWhitelist bool
	log              *slog.Logger
}

type createRequest struct {
	TotalGB        float64     `json:"total"`
	LimitResetDays NullableInt `json:"limit_reset"`
	RemnawaveID    int64       `json:"remnawave_id"`
}

type updateRequest struct {
	TotalGB        *float64    `json:"total"`
	LimitResetDays NullableInt `json:"limit_reset"`
}

// NullableInt distinguishes an omitted JSON field from an explicit null.
// This is required because PUT /v1/users/{id} uses null to switch reset
// management back to Remnawave, while an omitted field means "leave unchanged".
type NullableInt struct {
	Set   bool
	Value *int
}

func (n *NullableInt) UnmarshalJSON(data []byte) error {
	n.Set = true
	if string(data) == "null" {
		n.Value = nil
		return nil
	}
	var value int
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("limit_reset must be an integer or null: %w", err)
	}
	n.Value = &value
	return nil
}

type trafficResponse struct {
	Total int64 `json:"total"`
	Used  int64 `json:"used"`
}

func New(st *store.Store, db, remnaDB *pgxpool.Pool, apiKey string, events store.EventOptions, enforceWhitelist bool, logger *slog.Logger) *Server {
	apiKey = strings.Trim(strings.TrimSpace(apiKey), `"'`)
	return &Server{
		store:            st,
		db:               db,
		remnaDB:          remnaDB,
		apiKey:           apiKey,
		events:           events,
		enforceWhitelist: enforceWhitelist,
		log:              logger,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/v1/users/", s.users)
	mux.HandleFunc("/traffic/healthz", s.health)
	mux.HandleFunc("/traffic/v1/users/", s.users)
	return s.requestID(s.recoverPanic(s.securityHeaders(s.normalizePath(s.auth(mux)))))
}

func (s *Server) normalizePath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasPrefix(path, "/traffic/") {
			r.URL.Path = strings.TrimPrefix(path, "/traffic")
			if r.URL.RawPath != "" && strings.HasPrefix(r.URL.RawPath, "/traffic/") {
				r.URL.RawPath = strings.TrimPrefix(r.URL.RawPath, "/traffic")
			}
		} else if path == "/traffic" {
			r.URL.Path = "/"
			if r.URL.RawPath == "/traffic" {
				r.URL.RawPath = "/"
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if requestID == "" || len(requestID) > 100 {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.log.Error("panic in HTTP handler", "request_id", w.Header().Get("X-Request-ID"), "panic", v)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		provided := r.Header.Get("X-API-Key")
		if provided == "" {
			provided = r.Header.Get("X-Api-Key")
		}
		if provided == "" {
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				provided = strings.TrimPrefix(authHeader, "Bearer ")
			} else if strings.HasPrefix(authHeader, "ApiKey ") {
				provided = strings.TrimPrefix(authHeader, "ApiKey ")
			} else if strings.HasPrefix(authHeader, "Token ") {
				provided = strings.TrimPrefix(authHeader, "Token ")
			} else if authHeader != "" && !strings.Contains(authHeader, " ") {
				provided = authHeader
			}
		}
		if provided == "" {
			provided = r.URL.Query().Get("api_key")
		}
		provided = strings.Trim(strings.TrimSpace(provided), `"'`)
		serverKey := strings.Trim(strings.TrimSpace(s.apiKey), `"'`)

		if provided == "" || serverKey == "" || !hmac.Equal([]byte(provided), []byte(serverKey)) {
			writeError(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	if s.remnaDB != nil {
		if err := s.remnaDB.Ping(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "Remnawave database unavailable")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) users(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.HasPrefix(path, "/traffic") {
		path = strings.TrimPrefix(path, "/traffic")
	}
	path = strings.TrimPrefix(path, "/v1/users/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return
	}

	if len(parts) == 2 && parts[1] == "trafic" {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		u, err := s.store.SyncAndReconcileUser(r.Context(), id, s.events)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, trafficResponse{Total: u.TotalBytes, Used: u.UsedBytes})
		return
	}

	if len(parts) == 3 && parts[1] == "trafic" && parts[2] == "reset" {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		u, err := s.store.ResetUsage(r.Context(), id, s.events)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id":        u.ID,
			"total":     u.TotalBytes,
			"used":      u.UsedBytes,
			"remaining": remainingBytes(u.TotalBytes, u.UsedBytes),
		})
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodPost:
			s.create(w, r, id)
		case http.MethodPut:
			s.update(w, r, id)
		case http.MethodDelete:
			s.delete(w, r, id)
		default:
			methodNotAllowed(w, http.MethodPost, http.MethodPut, http.MethodDelete)
		}
		return
	}

	writeError(w, http.StatusNotFound, "not found")
}

func (s *Server) create(w http.ResponseWriter, r *http.Request, id int64) {
	var req createRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.RemnawaveID <= 0 {
		writeError(w, http.StatusBadRequest, "remnawave_id must be > 0")
		return
	}
	bytes, err := gbToBytes(req.TotalGB)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	panelState, err := s.store.GetPanelState(r.Context(), req.RemnawaveID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusBadRequest, "remnawave user not found")
			return
		}
		s.writeStoreError(w, err)
		return
	}

	now := time.Now().UTC()
	u := store.User{
		ID:               id,
		RemnawaveID:      req.RemnawaveID,
		TotalBytes:       bytes,
		ResetMode:        store.ResetModeRemnawave,
		ResetStrategy:    panelState.Strategy,
		PanelLastResetAt: panelState.LastResetAt,
		// A whitelist quota starts when the whitelist service registration is
		// created. The next Remnawave reset then re-aligns the period to the panel.
		PeriodStartedAt: &now,
		UsageBaselineAt: &now,
		LimitReached:    false,
	}
	if req.LimitResetDays.Set && req.LimitResetDays.Value != nil {
		if *req.LimitResetDays.Value <= 0 {
			writeError(w, http.StatusBadRequest, "limit_reset must be > 0 when provided")
			return
		}
		days := *req.LimitResetDays.Value
		next := now.AddDate(0, 0, days)
		u.ResetMode = store.ResetModeDays
		u.ResetDays = &days
		u.NextResetAt = &next
		u.PeriodStartedAt = &now
		u.UsageBaselineAt = &now
	}

	if err := s.store.CreateUser(r.Context(), u, store.EventOptions{}); err != nil {
		if isUniqueViolation(err) {
			writeError(w, http.StatusConflict, "user already exists")
			return
		}
		s.writeStoreError(w, err)
		return
	}

	used, err := s.store.ReconcileUserUsage(r.Context(), id, s.events)
	if err != nil {
		// Registration without a baseline would be unsafe: the user could start
		// counting from an unknown value. Remove the partially-created user.
		_ = s.store.DeleteUser(context.Background(), id)
		writeError(w, http.StatusInternalServerError, "initial traffic reconciliation failed")
		return
	}

	if s.enforceWhitelist {
		current, err := s.store.GetUser(r.Context(), id)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if !current.LimitReached {
			if err := s.store.EnqueueAccessSync(r.Context(), id); err != nil {
				s.writeStoreError(w, err)
				return
			}
		}
	}

	result, err := s.store.GetUser(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":             result.ID,
		"remnawave_id":   result.RemnawaveID,
		"total":          result.TotalBytes,
		"used":           used,
		"reset_mode":     result.ResetMode,
		"reset_days":     result.ResetDays,
		"reset_strategy": result.ResetStrategy,
		"next_reset_at":  result.NextResetAt,
		"limit_reached":  result.LimitReached,
	})
}

func (s *Server) update(w http.ResponseWriter, r *http.Request, id int64) {
	var req updateRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var totalBytes *int64
	if req.TotalGB != nil {
		b, err := gbToBytes(*req.TotalGB)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		totalBytes = &b
	}

	var resetAction *store.ResetAction
	if req.LimitResetDays.Set {
		if req.LimitResetDays.Value == nil {
			resetAction = &store.ResetAction{Mode: store.ResetModeRemnawave}
		} else {
			days := *req.LimitResetDays.Value
			if days <= 0 {
				writeError(w, http.StatusBadRequest, "limit_reset must be > 0")
				return
			}
			resetAction = &store.ResetAction{Mode: store.ResetModeDays, Days: &days}
		}
	}

	if totalBytes == nil && resetAction == nil {
		writeError(w, http.StatusBadRequest, "at least one of total or limit_reset must be provided")
		return
	}

	var panelState *store.PanelState
	if resetAction != nil && resetAction.Mode == store.ResetModeRemnawave {
		current, err := s.store.GetUser(r.Context(), id)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		state, err := s.store.GetPanelState(r.Context(), current.RemnawaveID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, http.StatusBadGateway, "Remnawave user not found")
				return
			}
			writeError(w, http.StatusBadGateway, "failed to read Remnawave reset state")
			return
		}
		panelState = &state
	}

	// When switching back to Remnawave-managed reset, defer side effects until
	// after the panel boundary has been applied and usage has been reconciled.
	// This avoids restoring/removing the squad from stale pre-switch usage.
	updateOpts := s.events
	if panelState != nil {
		updateOpts = store.EventOptions{}
	}

	u, err := s.store.UpdateUser(r.Context(), id, totalBytes, resetAction, updateOpts)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	if panelState != nil {
		if _, err := s.store.SwitchToRemnawave(r.Context(), id, *panelState); err != nil {
			s.writeStoreError(w, err)
			return
		}
		if _, err := s.store.ReconcileUserUsage(r.Context(), id, s.events); err != nil {
			writeError(w, http.StatusInternalServerError, "traffic reconciliation failed")
			return
		}
		if s.enforceWhitelist {
			if err := s.store.EnqueueAccessSync(r.Context(), id); err != nil {
				s.writeStoreError(w, err)
				return
			}
		}
		u, err = s.store.GetUser(r.Context(), id)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":             u.ID,
		"remnawave_id":   u.RemnawaveID,
		"total":          u.TotalBytes,
		"used":           u.UsedBytes,
		"remaining":      remainingBytes(u.TotalBytes, u.UsedBytes),
		"reset_mode":     u.ResetMode,
		"reset_days":     u.ResetDays,
		"reset_strategy": u.ResetStrategy,
		"next_reset_at":  u.NextResetAt,
		"limit_reached":  u.LimitReached,
	})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request, id int64) {
	if err := s.store.DeleteUser(r.Context(), id); err != nil {
		s.writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func gbToBytes(gb float64) (int64, error) {
	if math.IsNaN(gb) || math.IsInf(gb, 0) || gb < 0 {
		return 0, fmt.Errorf("total must be a finite number >= 0")
	}
	const bytesPerGB = 1024 * 1024 * 1024
	maxGB := float64(^uint64(0)>>1) / bytesPerGB
	if gb > maxGB {
		return 0, fmt.Errorf("total is too large")
	}
	return int64(gb*bytesPerGB + 0.5), nil
}

func remainingBytes(total, used int64) int64 {
	if total == 0 {
		return 0
	}
	remaining := total - used
	if remaining < 0 {
		return 0
	}
	return remaining
}

func decodeJSON(r *http.Request, dst any) error {
	if r.Body == nil {
		return fmt.Errorf("empty request body")
	}
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("empty request body")
		}
		return fmt.Errorf("invalid JSON: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("request body must contain a single JSON object")
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeError(w, http.StatusGatewayTimeout, "database timeout")
		return
	}
	s.log.Error("request failed", "error", err, "request_id", w.Header().Get("X-Request-ID"))
	writeError(w, http.StatusInternalServerError, "internal server error")
}

func methodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}
