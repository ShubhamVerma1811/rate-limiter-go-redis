package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx = context.Background()
)

type APIResponse struct {
	Message string `json:"message"`
}

const (
	HRLIMIT = "x-rateLimit-limit"
	HRREM   = "x-ratelimit-remaining"
	HRRESET = "x-ratelimit-reset"
	LIMIT   = 10
	WINDOW  = 5
)

type RateLimiter struct{}

func (rl *RateLimiter) FixedBucket(next http.HandlerFunc) http.HandlerFunc {
	rdb.Set(ctx, "bucket", LIMIT, redis.KeepTTL)

	go func() {
		for {
			time.Sleep(time.Second)
			if v, _ := strconv.Atoi(rdb.Get(ctx, "bucket").Val()); v < LIMIT {
				rdb.Incr(ctx, "bucket")
			}
		}
	}()

	return func(w http.ResponseWriter, r *http.Request) {
		val, err := strconv.Atoi(rdb.Get(ctx, "bucket").Val())

		if err != nil {
			log.Panic(err)
			os.Exit(1)
		}

		w.Header().Add(HRLIMIT, fmt.Sprint(LIMIT))
		w.Header().Add(HRREM, fmt.Sprint(val))

		if val <= 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			res := APIResponse{
				Message: "Rate Limit reached",
			}
			json.NewEncoder(w).Encode(res)
		} else {
			rdb.Decr(ctx, "bucket")
			next.ServeHTTP(w, r)
		}
	}
}

func (rl *RateLimiter) WindowCounter(next http.HandlerFunc) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		ok, err := rl.isAllowed(rdb)

		if err != nil {
			log.Panic(err)
			os.Exit(1)
		}

		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			res := APIResponse{
				Message: "Rate Limit reached",
			}
			json.NewEncoder(w).Encode(res)
		} else {
			next.ServeHTTP(w, r)
		}
	}
}

func (rl *RateLimiter) isAllowed(r *redis.Client) (bool, error) {
	now := time.Now()
	timeStamp := strconv.FormatInt(now.Truncate(time.Minute).Unix(), 10)

	v, err := r.HIncrBy(ctx, "window", timeStamp, 1).Result()

	if err != nil {
		return false, err
	}

	if v > LIMIT {
		return false, nil
	}

	vals, err := r.HGetAll(ctx, "window").Result()

	if err != nil {
		return false, err
	}

	total := 0
	threshold := now.Add(-time.Minute * time.Duration(WINDOW)).Truncate(time.Minute).Unix()

	for k, v := range vals {
		intKey, err := strconv.Atoi(k)

		if err != nil {
			return false, err
		}

		if int64(intKey) > threshold {
			val, err := strconv.Atoi(v)

			if err != nil {
				return false, err
			}

			total += val
		} else {
			r.HDel(ctx, "window", k)
		}
	}

	if total > LIMIT {
		return false, nil
	}

	return true, nil
}

func main() {
	mux := http.NewServeMux()
	rl := &RateLimiter{}

	mux.HandleFunc("GET /fixed", rl.FixedBucket(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		res := APIResponse{
			Message: "Fixed API endpoint",
		}

		json.NewEncoder(w).Encode(res)
	}))

	mux.HandleFunc("GET /window", rl.WindowCounter(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		res := APIResponse{
			Message: "Window API endpoint",
		}

		json.NewEncoder(w).Encode(res)
	}))

	log.Fatal(http.ListenAndServe(":8080", mux))
}
