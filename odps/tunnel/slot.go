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
	"net"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/exp/rand"
)

type slot struct {
	id   string
	ip   string
	port int
}

func newSlot(id string, server string) (slot, error) {
	ip, portText, err := net.SplitHostPort(server)
	if err != nil {
		return slot{}, errors.Wrap(err, "invalid slot format")
	}

	if ip == "" {
		return slot{}, errors.Errorf("empty server ip: %s", server)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return slot{}, errors.WithStack(err)
	}

	s := slot{
		id:   id,
		ip:   ip,
		port: port,
	}

	return s, nil
}

func (s *slot) SetServer(server string) error {
	next, err := newSlot(s.id, server)
	if err != nil {
		return err
	}
	*s = next
	return nil
}

func (s *slot) Server() string {
	return net.JoinHostPort(s.ip, strconv.Itoa(s.port))
}

// slotSelector rotates over the slots of a stream upload session. One session can
// be shared by several goroutines, and the server a slot points at is rewritten in
// place whenever the tunnel reschedules it, so every access is guarded by mu.
type slotSelector struct {
	mu    sync.Mutex
	index int
	arr   []slot
}

func newSlotSelect(arr []slot) *slotSelector {
	s := &slotSelector{}
	s.Reset(arr)
	return s
}

// Reset replaces the slot list with the one just loaded from the tunnel. An empty
// list is ignored, so that a reload returning no slot leaves the current routing
// information in place instead of making the session unusable. This is what the
// java sdk does too.
func (s *slotSelector) Reset(arr []slot) {
	if len(arr) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.arr = arr
	s.index = rand.Intn(len(arr))
}

// NextSlot returns a copy of the next slot, not a pointer into arr: the caller
// uses it outside the lock, while a concurrent flush may be rewriting the server
// of that very slot.
func (s *slotSelector) NextSlot() (slot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.arr) == 0 {
		return slot{}, errors.New("no slot is available in the stream upload session")
	}

	if s.index >= len(s.arr) {
		s.index = 0
	}

	e := s.arr[s.index]
	s.index += 1

	return e, nil
}

// UpdateServer records the server a slot has been rescheduled to, as reported by
// the odps-tunnel-routed-server header of a flush response.
func (s *slotSelector) UpdateServer(slotId string, server string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.arr {
		if s.arr[i].id == slotId {
			return errors.WithStack(s.arr[i].SetServer(server))
		}
	}

	return errors.Errorf("slot %s is no longer owned by the session", slotId)
}

func (s *slotSelector) SlotNum() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.arr)
}
