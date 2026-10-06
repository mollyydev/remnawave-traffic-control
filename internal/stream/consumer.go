package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/mollyydev/remnawave-traffic-control/internal/store"
)

var ErrPermanentMessage = errors.New("permanent stream message error")

type Consumer struct {
	redis    *redis.Client
	store    *store.Store
	stream   string
	group    string
	consumer string
	startID  string
	logger   *slog.Logger
	opts     store.EventOptions
}

func New(rdb *redis.Client, st *store.Store, streamName, group, consumer, startID string, logger *slog.Logger, opts store.EventOptions) *Consumer {
	return &Consumer{
		redis:    rdb,
		store:    st,
		stream:   streamName,
		group:    group,
		consumer: consumer,
		startID:  startID,
		logger:   logger,
		opts:     opts,
	}
}

func (c *Consumer) EnsureGroup(ctx context.Context) error {
	err := c.redis.XGroupCreateMkStream(ctx, c.stream, c.group, c.startID).Err()
	if err != nil && !strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
		return err
	}
	return nil
}

func (c *Consumer) Run(ctx context.Context) error {
	if err := c.EnsureGroup(ctx); err != nil {
		return err
	}

	claimTicker := time.NewTicker(30 * time.Second)
	defer claimTicker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		streams, err := c.readNew(ctx)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Error("redis XREADGROUP failed", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		c.processMessages(ctx, streams)

		select {
		case <-claimTicker.C:
			c.claimPending(ctx)
		default:
		}
	}
}

func (c *Consumer) readNew(ctx context.Context) ([]redis.XStream, error) {
	return c.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.consumer,
		Streams:  []string{c.stream, ">"},
		Count:    100,
		Block:    5 * time.Second,
	}).Result()
}

func (c *Consumer) processMessages(ctx context.Context, streams []redis.XStream) {
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			if err := c.processMessage(ctx, msg); err != nil {
				if errors.Is(err, ErrPermanentMessage) {
					// A malformed/incompatible exporter message cannot become valid on retry.
					// ACK it to prevent a poison message from staying pending forever.
					c.logger.Error("discarding malformed stream message", "stream_id", msg.ID, "error", err)
					if ackErr := c.redis.XAck(ctx, c.stream, c.group, msg.ID).Err(); ackErr != nil {
						c.logger.Error("redis XACK failed", "stream_id", msg.ID, "error", ackErr)
					}
					continue
				}
				c.logger.Error("failed to process stream message", "stream_id", msg.ID, "error", err)
				continue
			}
			if err := c.redis.XAck(ctx, c.stream, c.group, msg.ID).Err(); err != nil {
				c.logger.Error("redis XACK failed", "stream_id", msg.ID, "error", err)
			}
		}
	}
}

func (c *Consumer) claimPending(ctx context.Context) {
	start := "0-0"
	for {
		claimed, next, err := c.redis.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   c.stream,
			Group:    c.group,
			Consumer: c.consumer,
			MinIdle:  30 * time.Second,
			Start:    start,
			Count:    100,
		}).Result()
		if err != nil {
			if ctx.Err() == nil {
				c.logger.Warn("redis XAUTOCLAIM failed", "error", err)
			}
			return
		}

		for _, msg := range claimed {
			if err := c.processMessage(ctx, msg); err == nil || errors.Is(err, ErrPermanentMessage) {
				if errors.Is(err, ErrPermanentMessage) {
					c.logger.Error("discarding malformed claimed stream message", "stream_id", msg.ID, "error", err)
				}
				if ackErr := c.redis.XAck(ctx, c.stream, c.group, msg.ID).Err(); ackErr != nil {
					c.logger.Error("redis XACK failed", "stream_id", msg.ID, "error", ackErr)
				}
			} else {
				c.logger.Error("failed to process claimed stream message", "stream_id", msg.ID, "error", err)
			}
		}

		if len(claimed) == 0 || next == "0-0" || next == start {
			return
		}
		start = next
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg redis.XMessage) error {
	eventAt, nodeID, records, err := parseMessage(msg.ID, msg.Values)
	if err != nil {
		return err
	}
	return c.store.ApplyUsage(ctx, msg.ID, eventAt, nodeID, records, c.opts)
}

func parseMessage(streamID string, values map[string]any) (time.Time, int64, map[int64]int64, error) {
	var nodeID int64
	if v, ok := values["nodeId"]; ok {
		nodeID = parseInt64(v)
	} else if v, ok := values["node_id"]; ok {
		nodeID = parseInt64(v)
	}
	if nodeID <= 0 {
		return time.Time{}, 0, nil, fmt.Errorf("%w: missing or invalid nodeId in stream message", ErrPermanentMessage)
	}

	eventAt := parseTimestamp(values["ts"])
	if eventAt.IsZero() {
		eventAt = timestampFromStreamID(streamID)
	}
	if eventAt.IsZero() {
		return time.Time{}, 0, nil, fmt.Errorf("%w: missing valid timestamp in stream message", ErrPermanentMessage)
	}

	var raw string
	if v, ok := values["records"]; ok {
		raw = fmt.Sprint(v)
	} else if v, ok := values["data"]; ok {
		raw = fmt.Sprint(v)
	} else {
		return time.Time{}, 0, nil, fmt.Errorf("%w: missing records in stream message", ErrPermanentMessage)
	}

	result := make(map[int64]int64)
	for _, item := range strings.Split(raw, ";") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, ":", 2)
		if len(parts) != 2 {
			continue
		}
		uid, err1 := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		bytesUsed, err2 := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err1 != nil || err2 != nil || uid <= 0 || bytesUsed < 0 {
			continue
		}
		if bytesUsed == 0 {
			continue
		}
		result[uid] += bytesUsed
	}
	return eventAt, nodeID, result, nil
}

func parseTimestamp(v any) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x.UTC()
	case int64:
		return unixAuto(x)
	case int:
		return unixAuto(int64(x))
	case int32:
		return unixAuto(int64(x))
	case float64:
		if x <= 0 {
			return time.Time{}
		}
		return unixAuto(int64(x))
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return time.Time{}
		}
		return unixAuto(n)
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return time.Time{}
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return unixAuto(n)
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UTC()
			}
		}
	}
	return time.Time{}
}

func unixAuto(v int64) time.Time {
	// Remnawave normally uses epoch milliseconds for stream timestamps. Accept
	// seconds too, which keeps the consumer tolerant of older/custom exporters.
	if v >= 1_000_000_000_000 {
		return time.UnixMilli(v).UTC()
	}
	if v >= 1_000_000_000 {
		return time.Unix(v, 0).UTC()
	}
	return time.Time{}
}

func timestampFromStreamID(id string) time.Time {
	parts := strings.SplitN(id, "-", 2)
	if len(parts) != 2 {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func parseInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n
	default:
		return 0
	}
}
