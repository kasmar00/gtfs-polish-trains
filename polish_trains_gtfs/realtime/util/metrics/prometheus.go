package metrics

import (
	"errors"
	"log"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func InitializePrometheusServer() {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	Metrics = *newMetrics(reg)

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	go func() {
		slog.Info("Starting metrics server", "addr", ":2112")
		if err := http.ListenAndServe(":2112", nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Metrics server failed: %s", err)
		}
	}()
}

type metrics struct {
	LookupAlternativeTripSize prometheus.Gauge
	LookupStopsSize           prometheus.Gauge
	LookupTripsSize           prometheus.Gauge
	LookupTripsByNumberSize   prometheus.Gauge
	MatchStats                *prometheus.GaugeVec
}

var Metrics metrics

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		LookupAlternativeTripSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "polish_trains_gtfs_realtime_lookup_alternative_trip_size",
			Help: "The current size of static.AlternativeTripLookup",
		}),
		LookupStopsSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "polish_trains_gtfs_realtime_lookup_stops_size",
			Help: "The current size of static.Stops",
		}),
		LookupTripsSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "polish_trains_gtfs_realtime_lookup_trips_size",
			Help: "The current size of static.Trips",
		}),
		LookupTripsByNumberSize: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "polish_trains_gtfs_realtime_lookup_trips_by_number_size",
			Help: "The current size of static.TripsByNumber",
		}),
		MatchStats: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "polish_trains_gtfs_realtime_match_stats",
			Help: "Stats of the latest match with RT data",
		}, []string{"result"}),
	}
	return m
}

func RecordMatchStats(matched, unmatched, outsideFeedDates uint) {
	Metrics.MatchStats.With(prometheus.Labels{"result": "matched"}).Set(float64(matched))
	Metrics.MatchStats.With(prometheus.Labels{"result": "unmatched"}).Set(float64(unmatched))
	Metrics.MatchStats.With(prometheus.Labels{"result": "outsideFeedDates"}).Set(float64(outsideFeedDates))
}
