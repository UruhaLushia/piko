package piko

import (
	"errors"
	"time"
)

const (
	slowConnectionCheckInterval = 2 * time.Second
	slowConnectionMinAge        = 5 * time.Second
	slowConnectionMinBytes      = 1024 * 1024
	slowConnectionRatio         = 0.45
	slowConnectionStrikes       = 2
	slowConnectionMinPeers      = 4
	slowTailWindow              = 2 * 1024 * 1024
	slowTailMinAge              = 4 * time.Second
	slowTailMinBytes            = 256 * 1024
)

var errSlowConnection = errors.New("slow connection")

func (d *downloader) registerRangeSpeed() int64 {
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	d.nextSpeedID++
	id := d.nextSpeedID
	if d.activeSpeeds == nil {
		d.activeSpeeds = make(map[int64]float64)
	}
	d.activeSpeeds[id] = 0
	return id
}

func (d *downloader) unregisterRangeSpeed(id int64) {
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	delete(d.activeSpeeds, id)
}

func (d *downloader) updateRangeSpeed(id int64, speed float64) (float64, int) {
	if speed <= 0 {
		return 0, 0
	}

	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	if d.activeSpeeds == nil {
		return 0, 0
	}
	d.activeSpeeds[id] = speed

	var total float64
	peers := 0
	for _, activeSpeed := range d.activeSpeeds {
		if activeSpeed <= 0 {
			continue
		}
		total += activeSpeed
		peers++
	}
	if peers == 0 {
		return 0, 0
	}
	return total / float64(peers), peers
}
