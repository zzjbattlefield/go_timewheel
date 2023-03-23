package timewheel

import (
	"testing"
	"time"
)

func TestNewTimeWheel(t *testing.T) {
	interval := time.Millisecond * 500
	slotsNum := 10
	wheel := NewTimeWheel(interval, slotsNum)
	if wheel.interval != interval {
		t.Errorf("interval should be %v but got %v", interval, wheel.interval)
	}
	if len(wheel.slots) != slotsNum {
		t.Errorf("number of slots should be %d but got %d", slotsNum, len(wheel.slots))
	}
	if wheel.slotNum != slotsNum {
		t.Errorf("slotNum should be %d but got %d", slotsNum, wheel.slotNum)
	}
}

func TestTimeWheel_StartAndStop(t *testing.T) {
	interval := time.Millisecond * 500
	slotsNum := 10
	wheel := NewTimeWheel(interval, slotsNum)
	go wheel.Start()
	time.Sleep(time.Second)
	wheel.Stop()
}

func TestTimeWheel_RemoveJob(t *testing.T) {
	interval := time.Second
	slotsNum := 10
	wheel := NewTimeWheel(interval, slotsNum)
	go wheel.Start()
	jobExecuted := false
	key := "testRemoveJob"
	wheel.AddJob(time.Second, key, func() {
		jobExecuted = true
	})
	time.Sleep(time.Second * 2)
	wheel.RemoveJob(key)
	time.Sleep(time.Second)
	if jobExecuted {
		t.Errorf("job should not have been executed but it was")
	}
	wheel.Stop()
}

func TestTimeWheel_AddJob(t *testing.T) {
	interval := time.Second
	wheel := NewTimeWheel(interval, 2)
	ch := make(chan time.Time)
	wheel.Start()
	beginTime := time.Now()
	wheel.AddJob(time.Second, "", func() {
		ch <- time.Now()
	})
	execAt := <-ch

	if execAt.Sub(beginTime) < time.Second || execAt.Sub(beginTime) > time.Second*3 {
		t.Errorf("wrong execute time, got %+v", execAt.Sub(beginTime).Seconds())
	}
	beginTime = time.Now()
	wheel.AddJob(time.Second*3, "testKey", func() {
		ch <- time.Now()
	})
	execAt = <-ch
	if execAt.Sub(beginTime) < time.Second || execAt.Sub(beginTime) > time.Second*4 {
		t.Errorf("wrong execute time, got %+v", execAt.Sub(beginTime).Seconds())
	}
	wheel.Stop()
}

func TestTimeWheel_getPositionAndCircle(t *testing.T) {
	interval := time.Second
	slotsNum := 60
	wheel := NewTimeWheel(interval, slotsNum)
	position, circle := wheel.getPositionAndCircle(time.Second * 30)
	if position != 30 || circle != 0 {
		t.Errorf("getPositionAndCircle should return position=30 and circle=0 for 30s delay, but got position=%d and circle=%d", position, circle)
	}
	position, circle = wheel.getPositionAndCircle(time.Second * 12)
	if position != 12 || circle != 0 {
		t.Errorf("getPositionAndCircle should return position=8 and circle=1 for 12s delay, but got position=%d and circle=%d", position, circle)
	}
	position, circle = wheel.getPositionAndCircle(time.Second * 200)
	if position != 20 || circle != 3 {
		t.Errorf("getPositionAndCircle should return position=0 and circle=0 for 200ms delay, but got position=%d and circle=%d", position, circle)
	}
}
