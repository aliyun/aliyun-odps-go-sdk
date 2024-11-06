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
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/exp/rand"
)

type slot struct {
	id   string
	ip   string
	port int
}

func newSlot(id string, server string) (slot, error) {
	parts := strings.Split(server, ":")

	if len(parts) != 2 {
		return slot{}, errors.Errorf("invalid slot format: %s", server)
	}

	ip := parts[0]
	port, err := strconv.Atoi(parts[1])
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
	parts := strings.Split(server, ":")

	if len(parts) != 2 {
		return errors.Errorf("invalid slot format: %s", server)
	}

	s.ip = parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return errors.WithStack(err)
	}

	s.port = port
	return nil
}

func (s *slot) Server() string {
	return fmt.Sprintf("%s:%d", s.ip, s.port)
}

type slotSelector struct {
	index int
	arr   []slot
}

func newSlotSelect(arr []slot) slotSelector {
	return slotSelector{
		rand.Intn(len(arr)),
		arr,
	}
}

func (s *slotSelector) NextSlot() *slot {
	if s.index >= len(s.arr) {
		s.index = 0
	}

	e := &s.arr[s.index]
	s.index += 1

	return e
}

func (s *slotSelector) SlotNum() int {
	return len(s.arr)
}
