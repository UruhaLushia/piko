package piko

import "time"

const (
	slowConnectionCheckInterval = 2 * time.Second
	slowConnectionMinAge        = 5 * time.Second
	slowConnectionMinBytes      = 1024 * 1024
	slowConnectionRatio         = 0.45
	slowConnectionSourceRatio   = 0.6
	slowConnectionStrikes       = 2
	slowConnectionMinPeers      = 2
	slowTailWindow              = 2 * 1024 * 1024
	slowTailCheckInterval       = time.Second
	slowTailMinAge              = 2 * time.Second
	slowTailIdleTimeout         = time.Second
)

func (d *downloader) registerRangeSpeed(source int) int64 {
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	d.nextSpeedID++
	id := d.nextSpeedID
	if d.activeSpeeds == nil {
		d.activeSpeeds = make(map[int64]rangeSpeed)
	}
	d.activeSpeeds[id] = rangeSpeed{source: source}
	return id
}

func (d *downloader) unregisterRangeSpeed(id int64) {
	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	delete(d.activeSpeeds, id)
}

func (d *downloader) updateRangeSpeed(id int64, speed float64) (float64, int, float64) {
	if speed <= 0 {
		return 0, 0, 0
	}

	d.speedMu.Lock()
	defer d.speedMu.Unlock()
	if d.activeSpeeds == nil {
		return 0, 0, 0
	}
	current := d.activeSpeeds[id]
	current.rate = speed
	d.activeSpeeds[id] = current

	var total float64
	peers := 0
	var otherTotal float64
	otherPeers := 0
	for _, active := range d.activeSpeeds {
		if active.rate <= 0 {
			continue
		}
		total += active.rate
		peers++
		if active.source != current.source {
			otherTotal += active.rate
			otherPeers++
		}
	}
	if peers == 0 {
		return 0, 0, 0
	}
	otherAvg := 0.0
	if otherPeers > 0 {
		otherAvg = otherTotal / float64(otherPeers)
	}
	return total / float64(peers), peers, otherAvg
}
