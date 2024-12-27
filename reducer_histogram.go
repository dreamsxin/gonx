package gonx

import (
	"math"
)

// 定义了一个桶，包含下限、上限和计数。
type Bucket struct {
	LowerBound float64
	UpperBound float64
	Count      int
}

// 包含一个桶数组和一个总数。
type Bin struct {
	Buckets []Bucket
	Total   int
}

func NewBin(bucketSizes ...float64) *Bin {
	histogram := &Bin{
		Buckets: make([]Bucket, len(bucketSizes)),
	}

	for i, size := range bucketSizes {
		histogram.Buckets[i] = Bucket{
			LowerBound: size,
			UpperBound: math.Inf(1),
			Count:      0,
		}
		if i > 0 {
			histogram.Buckets[i-1].UpperBound = size
		}
	}

	return histogram
}

func (h *Bin) Add(value float64) {
	for i, bucket := range h.Buckets {
		if value >= bucket.LowerBound && value < bucket.UpperBound {
			h.Buckets[i].Count++
			h.Total++
			break
		}
	}
}

// 计算给定百分位数的值。
func (h *Bin) Percentile(p float64) float64 {
	if h.Total == 0 {
		return 0
	}

	count := int(float64(h.Total) * p / 100)

	sum := 0
	for _, bucket := range h.Buckets {
		sum += bucket.Count
		if sum >= count {
			return bucket.LowerBound
		}
	}

	return h.Buckets[len(h.Buckets)-1].UpperBound
}

// 计算平均值。
func (h *Bin) Mean() float64 {
	if h.Total == 0 {
		return 0
	}

	sum := 0.0
	for _, bucket := range h.Buckets {
		sum += bucket.LowerBound * float64(bucket.Count)
	}

	return sum / float64(h.Total)
}

// 计算标准差。
func (h *Bin) StdDev() float64 {
	if h.Total == 0 {
		return 0
	}

	mean := h.Mean()
	sum := 0.0
	for _, bucket := range h.Buckets {
		diff := bucket.LowerBound - mean
		sum += diff * diff * float64(bucket.Count)
	}

	variance := sum / float64(h.Total)
	return math.Sqrt(variance)
}

// Histogram implements the Reducer interface for histogram values calculation
type ReducerHistogram struct {
	Fields map[string]string
	Bins   map[string]*Bin
}

// Reduce calculates the min values for input channel Entries, using configured Fields
// of the struct. Write result to the output channel as map[string]float64
func (r *ReducerHistogram) Reduce(input chan *Entry, output chan *Entry) {
	if r.Bins == nil {
		close(output)
		return
	}

	for entry := range input {
		for label, name := range r.Fields {
			val, err := entry.FloatField(name)
			if err == nil {
				if _, ok := r.Bins[label]; !ok {
					continue
				}
				r.Bins[label].Add(val)
			}
		}
	}
	entry := NewEmptyEntry()
	for name, bin := range r.Bins {
		histogram := NewEmptyEntry()
		histogram.SetFloatField("p5", bin.Percentile(5))
		histogram.SetFloatField("p10", bin.Percentile(10))
		histogram.SetFloatField("p50", bin.Percentile(50))
		histogram.SetFloatField("p90", bin.Percentile(90))
		histogram.SetFloatField("p95", bin.Percentile(95))
		histogram.SetFloatField("p99", bin.Percentile(99))
		histogram.SetFloatField("stddev", bin.StdDev())
		histogram.SetFloatField("mean", bin.Mean())
		histogram.SetFloatField("total", float64(bin.Total))
		entry.SetEntryField(name, histogram)
	}
	output <- entry
	close(output)
}
