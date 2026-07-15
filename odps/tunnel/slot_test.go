package tunnel

import "testing"

func TestNewSlotRejectsEmptyServerIP(t *testing.T) {
	_, err := newSlot("1", ":1234")
	if err == nil {
		t.Fatal("expected error for empty server ip")
	}
}

func TestSlotSetServerRejectsEmptyServerIP(t *testing.T) {
	s, err := newSlot("1", "127.0.0.1:1234")
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	err = s.SetServer(":5678")
	if err == nil {
		t.Fatal("expected error for empty server ip")
	}
}
