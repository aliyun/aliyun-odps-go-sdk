// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tunnel

import (
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

func newTestSlots(t *testing.T, servers ...string) []slot {
	t.Helper()

	slots := make([]slot, len(servers))
	for i, server := range servers {
		s, err := newSlot(strconv.Itoa(i), server)
		if err != nil {
			t.Fatalf("unexpected error building slot %s: %v", server, err)
		}
		slots[i] = s
	}

	return slots
}

func TestSlotSelectorRotatesOverAllSlots(t *testing.T) {
	selector := newSlotSelect(newTestSlots(t, "10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80"))

	seen := make(map[string]int)
	for i := 0; i < 9; i++ {
		s, err := selector.NextSlot()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		seen[s.Server()]++
	}

	if len(seen) != 3 {
		t.Fatalf("expected all 3 slots to be used, got %v", seen)
	}
	for server, count := range seen {
		if count != 3 {
			t.Fatalf("expected %s to be used 3 times, got %d", server, count)
		}
	}
}

// NextSlot must hand out a copy: the caller keeps using it after the lock is
// released, while another flush may reschedule that same slot.
func TestSlotSelectorNextSlotReturnsCopy(t *testing.T) {
	selector := newSlotSelect(newTestSlots(t, "10.0.0.1:80"))

	taken, err := selector.NextSlot()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := selector.UpdateServer(taken.id, "10.0.0.9:81"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if taken.Server() != "10.0.0.1:80" {
		t.Fatalf("the slot taken earlier was mutated in place: %s", taken.Server())
	}

	next, err := selector.NextSlot()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next.Server() != "10.0.0.9:81" {
		t.Fatalf("expected the stored slot to be updated, got %s", next.Server())
	}
}

func TestSlotSelectorNextSlotWithoutSlots(t *testing.T) {
	selector := newSlotSelect(nil)

	if _, err := selector.NextSlot(); err == nil {
		t.Fatal("expected an error instead of a panic when no slot is available")
	}
	if selector.SlotNum() != 0 {
		t.Fatalf("expected 0 slots, got %d", selector.SlotNum())
	}
}

// A reload that comes back with no slot must not wipe the routing information the
// session is still able to use.
func TestSlotSelectorResetIgnoresEmptyList(t *testing.T) {
	selector := newSlotSelect(newTestSlots(t, "10.0.0.1:80", "10.0.0.2:80"))

	selector.Reset(nil)

	if selector.SlotNum() != 2 {
		t.Fatalf("expected the previous 2 slots to be kept, got %d", selector.SlotNum())
	}
}

func TestSlotSelectorResetReplacesSlots(t *testing.T) {
	selector := newSlotSelect(newTestSlots(t, "10.0.0.1:80", "10.0.0.2:80"))

	selector.Reset(newTestSlots(t, "10.0.1.1:80"))

	if selector.SlotNum() != 1 {
		t.Fatalf("expected 1 slot after reset, got %d", selector.SlotNum())
	}

	s, err := selector.NextSlot()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Server() != "10.0.1.1:80" {
		t.Fatalf("expected the new slot, got %s", s.Server())
	}
}

func TestSlotSelectorUpdateServer(t *testing.T) {
	selector := newSlotSelect(newTestSlots(t, "10.0.0.1:80"))

	if err := selector.UpdateServer("0", "10.0.0.2:90"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s, _ := selector.NextSlot()
	if s.Server() != "10.0.0.2:90" {
		t.Fatalf("expected 10.0.0.2:90, got %s", s.Server())
	}

	if err := selector.UpdateServer("does-not-exist", "10.0.0.3:90"); err == nil {
		t.Fatal("expected an error for a slot the session no longer owns")
	}

	if err := selector.UpdateServer("0", ":90"); err == nil {
		t.Fatal("expected an error for an empty server ip")
	}
}

// Run with -race. A stream upload session is shared by several goroutines, each
// with its own pack writer but all rotating over and rewriting the same slots.
func TestSlotSelectorConcurrentAccess(t *testing.T) {
	selector := newSlotSelect(newTestSlots(t, "10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80"))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < 200; j++ {
				s, err := selector.NextSlot()
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				_ = s.Server()
				_ = selector.UpdateServer(s.id, "10.0.1.1:81")
				_ = selector.SlotNum()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for j := 0; j < 50; j++ {
			selector.Reset(newTestSlots(t, "10.0.2.1:80", "10.0.2.2:80"))
		}
	}()

	wg.Wait()
}

func TestSlotRoutingIsStale(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"no error", nil, false},
		{"308 slot reassignment", restclient.HttpError{StatusCode: http.StatusPermanentRedirect}, true},
		{"502 bad gateway", restclient.HttpError{StatusCode: http.StatusBadGateway}, true},
		{"504 gateway timeout", restclient.HttpError{StatusCode: http.StatusGatewayTimeout}, true},
		{"429 flow exceeded", restclient.HttpError{StatusCode: http.StatusTooManyRequests}, false},
		{"400 bad request", restclient.HttpError{StatusCode: http.StatusBadRequest}, false},
		{"403 forbidden", restclient.HttpError{StatusCode: http.StatusForbidden}, false},
		{"412 schema modified", restclient.HttpError{StatusCode: http.StatusPreconditionFailed}, false},
		{"500 internal error", restclient.HttpError{StatusCode: http.StatusInternalServerError}, false},
		{"503 unavailable", restclient.HttpError{StatusCode: http.StatusServiceUnavailable}, false},
		{"transport failure", errors.New("dial tcp 10.0.0.1:80: connect: connection refused"), true},
		{
			"wrapped 502",
			errors.WithMessage(
				errors.WithStack(restclient.HttpError{StatusCode: http.StatusBadGateway}),
				"flush failed",
			),
			true,
		},
		{
			"wrapped 400",
			errors.WithStack(restclient.HttpError{StatusCode: http.StatusBadRequest}),
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := slotRoutingIsStale(tt.err); got != tt.want {
				t.Fatalf("slotRoutingIsStale(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestFlushFailedMarksSlotsStaleOnlyWhenRoutingIsStale(t *testing.T) {
	su := &StreamUploadSession{slotSelector: newSlotSelect(nil)}

	_ = su.flushFailed(restclient.HttpError{StatusCode: http.StatusBadRequest})
	if su.takeSlotsStale() {
		t.Fatal("a 400 must not mark the slot routing stale")
	}

	_ = su.flushFailed(restclient.HttpError{StatusCode: http.StatusBadGateway})
	if !su.takeSlotsStale() {
		t.Fatal("a 502 must mark the slot routing stale")
	}

	// takeSlotsStale clears the mark, so that concurrent flushes send one reload
	if su.takeSlotsStale() {
		t.Fatal("the stale mark must be cleared once taken")
	}
}

func TestFlushFailedKeepsTheOriginalError(t *testing.T) {
	su := &StreamUploadSession{slotSelector: newSlotSelect(nil)}

	err := su.flushFailed(restclient.HttpError{StatusCode: http.StatusBadGateway})

	var httpErr restclient.HttpError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected the original http error to survive, got %v", err)
	}
}

func TestRefreshSlotRoutingUpdatesRescheduledSlot(t *testing.T) {
	su := &StreamUploadSession{slotSelector: newSlotSelect(newTestSlots(t, "10.0.0.1:80"))}
	currentSlot, _ := su.slotSelector.NextSlot()

	su.refreshSlotRouting(currentSlot, "1", "10.0.0.2:81")

	updated, _ := su.slotSelector.NextSlot()
	if updated.Server() != "10.0.0.2:81" {
		t.Fatalf("expected the slot to follow the routed server header, got %s", updated.Server())
	}
	if su.takeSlotsStale() {
		t.Fatal("a successful routing update must not mark the slots stale")
	}
}

// The tunnel not sending odps-tunnel-routed-server means the slot was not
// rescheduled. It must neither be treated as an error nor trigger a reload,
// otherwise every flush would reload.
func TestRefreshSlotRoutingKeepsServerWhenHeaderIsAbsent(t *testing.T) {
	su := &StreamUploadSession{slotSelector: newSlotSelect(newTestSlots(t, "10.0.0.1:80"))}
	currentSlot, _ := su.slotSelector.NextSlot()

	su.refreshSlotRouting(currentSlot, "1", "")

	unchanged, _ := su.slotSelector.NextSlot()
	if unchanged.Server() != "10.0.0.1:80" {
		t.Fatalf("expected the slot to keep its server, got %s", unchanged.Server())
	}
	if su.takeSlotsStale() {
		t.Fatal("an absent routed server header must not mark the slots stale")
	}
}

func TestRefreshSlotRoutingMarksStaleOnUnusableSlotNumHeader(t *testing.T) {
	su := &StreamUploadSession{slotSelector: newSlotSelect(newTestSlots(t, "10.0.0.1:80"))}
	currentSlot, _ := su.slotSelector.NextSlot()

	su.refreshSlotRouting(currentSlot, "not-a-number", "10.0.0.2:81")

	if !su.takeSlotsStale() {
		t.Fatal("an unusable slot num header must mark the slots stale")
	}
}

func TestRefreshSlotRoutingMarksStaleWhenSlotIsGone(t *testing.T) {
	su := &StreamUploadSession{slotSelector: newSlotSelect(newTestSlots(t, "10.0.0.1:80"))}
	goneSlot, err := newSlot("999", "10.0.0.7:80")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	su.refreshSlotRouting(goneSlot, "1", "10.0.0.8:81")

	if !su.takeSlotsStale() {
		t.Fatal("a slot the session no longer owns must mark the slots stale")
	}
}
