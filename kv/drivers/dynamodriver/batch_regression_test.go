package dynamodriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type wireKey map[string]map[string]string

type wireRead struct {
	Keys                     []wireKey         `json:"Keys"`
	ProjectionExpression     string            `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
}

func fakeDynamo(t *testing.T, fn http.HandlerFunc) *DynamoDB {
	t.Helper()
	server := httptest.NewServer(fn)
	t.Cleanup(server.Close)
	client := dynamodb.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, HTTPClient: server.Client()}, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.RetryMaxAttempts = 1
	})
	d := New(client)
	if err := d.Open(t.Context(), "items"); err != nil {
		t.Fatal(err)
	}
	return d
}

func respond(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Error(err)
	}
}

func TestMGetRetriesOnlyUnprocessedKeys(t *testing.T) {
	var calls atomic.Int64
	d := fakeDynamo(t, func(w http.ResponseWriter, r *http.Request) {
		var request struct{ RequestItems map[string]wireRead }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		keys := request.RequestItems["items"].Keys
		call := calls.Add(1)
		if call == 1 {
			if len(keys) != 2 {
				t.Errorf("first request has %d keys, want 2", len(keys))
			}
			respond(t, w, map[string]any{
				"Responses":       map[string]any{"items": []wireKey{{"pk": {"S": "a"}, "val": {"B": "QQ=="}}}},
				"UnprocessedKeys": map[string]any{"items": wireRead{Keys: []wireKey{{"pk": {"S": "b"}}}}},
			})
		} else {
			if len(keys) != 1 || keys[0]["pk"]["S"] != "b" {
				t.Errorf("retry keys = %v", keys)
			}
			respond(t, w, map[string]any{"Responses": map[string]any{"items": []wireKey{{"pk": {"S": "b"}, "val": {"B": "Qg=="}}}}})
		}
	})
	values, err := d.MGet(t.Context(), []string{"a", "b", "a"})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(values) != "[[65] [66] [65]]" {
		t.Errorf("values = %v", values)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
	if len(values[0]) > 0 && len(values[2]) > 0 {
		values[0][0] = 'X'
		if values[2][0] != 'A' {
			t.Error("duplicate results share mutable bytes")
		}
	}
}

func TestBatchGetBoundsPersistentThrottling(t *testing.T) {
	var calls atomic.Int64
	d := fakeDynamo(t, func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		calls.Add(1)
		respond(t, w, map[string]any{"UnprocessedKeys": request["RequestItems"]})
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := d.BatchGet(ctx, []string{"a"})
	if err == nil {
		t.Fatal("persistent unprocessed keys reported success")
	}
	if calls.Load() > 6 {
		t.Errorf("persistent throttling made %d calls, want at most 6", calls.Load())
	}
}

func TestBatchWritesRetryOnlyUnprocessedItems(t *testing.T) {
	for _, operation := range []string{"mset", "put", "delete"} {
		t.Run(operation, func(t *testing.T) {
			var calls atomic.Int64
			var pending json.RawMessage
			d := fakeDynamo(t, func(w http.ResponseWriter, r *http.Request) {
				var request struct{ RequestItems map[string][]json.RawMessage }
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				items := request.RequestItems["items"]
				call := calls.Add(1)
				if call == 1 {
					if len(items) != 2 {
						t.Errorf("initial batch has %d items, want 2", len(items))
					}
					pending = items[len(items)-1]
				} else if len(items) != 1 || !sameJSON(items[0], pending) {
					t.Errorf("retry sent processed items: %s", items)
				}
				if call < 3 {
					respond(t, w, map[string]any{"UnprocessedItems": map[string]any{"items": []json.RawMessage{pending}}})
				} else {
					respond(t, w, map[string]any{})
				}
			})
			var err error
			switch operation {
			case "mset":
				err = d.MSet(t.Context(), map[string][]byte{"a": []byte("A"), "b": []byte("B")}, time.Minute)
			case "put":
				err = d.BatchPut(t.Context(), []map[string]types.AttributeValue{d.primaryKey("a"), d.primaryKey("b")})
			case "delete":
				err = d.BatchDelete(t.Context(), []string{"a", "b", "a"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 3 {
				t.Errorf("calls = %d, want 3", calls.Load())
			}
		})
	}
}

func TestBatchRetriesReportIncompleteWork(t *testing.T) {
	for _, write := range []bool{false, true} {
		t.Run(fmt.Sprint(write), func(t *testing.T) {
			var calls atomic.Int64
			d := fakeDynamo(t, func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				calls.Add(1)
				field := "UnprocessedKeys"
				if write {
					field = "UnprocessedItems"
				}
				respond(t, w, map[string]any{field: request["RequestItems"]})
			})
			var err error
			if write {
				err = d.MSet(t.Context(), map[string][]byte{"a": []byte("A")}, 0)
			} else {
				_, err = d.MGet(t.Context(), []string{"a"})
			}
			if !errors.Is(err, ErrBatchIncomplete) {
				t.Errorf("error = %v, want incomplete batch", err)
			}
			if calls.Load() != maxBatchAttempts {
				t.Errorf("calls = %d, want %d", calls.Load(), maxBatchAttempts)
			}
		})
	}
}

func TestBatchRetryCancellationStopsRequests(t *testing.T) {
	for _, write := range []bool{false, true} {
		t.Run(fmt.Sprint(write), func(t *testing.T) {
			var calls atomic.Int64
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			d := fakeDynamo(t, func(w http.ResponseWriter, r *http.Request) {
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				calls.Add(1)
				field := "UnprocessedKeys"
				if write {
					field = "UnprocessedItems"
				}
				respond(t, w, map[string]any{field: request["RequestItems"]})
				cancel()
			})
			var err error
			if write {
				err = d.BatchDelete(ctx, []string{"a"})
			} else {
				_, err = d.BatchGet(ctx, []string{"a"})
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("error = %v, want cancellation", err)
			}
			if calls.Load() != 1 {
				t.Errorf("calls = %d after cancellation, want 1", calls.Load())
			}
		})
	}
}

func TestExistsBatchesUniqueKeysAndChecksExpiry(t *testing.T) {
	var calls atomic.Int64
	d := fakeDynamo(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Target") != "DynamoDB_20120810.BatchGetItem" {
			t.Errorf("operation = %s", r.Header.Get("X-Amz-Target"))
		}
		var request struct{ RequestItems map[string]wireRead }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		input := request.RequestItems["items"]
		if len(input.Keys) > 100 {
			t.Errorf("batch too large: %d", len(input.Keys))
		}
		if input.ProjectionExpression != "#pk, #ttl" || input.ExpressionAttributeNames["#pk"] != "key.name" || input.ExpressionAttributeNames["#ttl"] != "expires.at" {
			t.Errorf("projection = %+v", input)
		}
		calls.Add(1)
		var items []wireKey
		for _, key := range input.Keys {
			if key["key.name"]["S"] == "key-0" {
				key["expires.at"] = map[string]string{"N": "1"}
			}
			items = append(items, key)
		}
		respond(t, w, map[string]any{"Responses": map[string]any{"items": items}})
	})
	WithPKAttribute("key.name")(d)
	WithTTLAttribute("expires.at")(d)
	keys := make([]string, 205, 207)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	keys = append(keys, "key-1", "key-0")
	count, err := d.Exists(t.Context(), keys...)
	if err != nil {
		t.Fatal(err)
	}
	if count != 205 {
		t.Errorf("exists = %d, want 205", count)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
}

func TestBatchWriteChunksAndEmptyOperations(t *testing.T) {
	var calls atomic.Int64
	d := fakeDynamo(t, func(w http.ResponseWriter, r *http.Request) {
		var request struct{ RequestItems map[string][]json.RawMessage }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		size := len(request.RequestItems["items"])
		if size > 25 || size == 0 {
			t.Errorf("batch size = %d", size)
		}
		calls.Add(1)
		respond(t, w, map[string]any{})
	})
	pairs := make(map[string][]byte)
	for i := range 51 {
		pairs[fmt.Sprint(i)] = []byte("value")
	}
	if err := d.MSet(t.Context(), pairs, 0); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
	if err := d.BatchDelete(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if err := d.BatchPut(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if err := d.MSet(t.Context(), nil, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := d.MGet(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := d.BatchGet(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Exists(t.Context()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Errorf("empty operations made requests: %d", calls.Load())
	}
}

func sameJSON(a, b []byte) bool {
	var left, right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func TestBatchRetryBackoffCanBeCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		if err := waitBatchRetry(t.Context(), 1); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed < 12*time.Millisecond {
			t.Fatalf("retry did not back off: %s", elapsed)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		time.AfterFunc(time.Millisecond, cancel)
		start = time.Now()
		if err := waitBatchRetry(ctx, 5); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
		if elapsed := time.Since(start); elapsed != time.Millisecond {
			t.Fatalf("canceled backoff took %s", elapsed)
		}
	})
}
