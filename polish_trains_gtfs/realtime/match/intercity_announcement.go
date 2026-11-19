// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package match

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/fact"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/schedules"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/source"
)

const intercityAgencyID = "IC"

type intercityAnnouncementContent struct {
	title  string
	effect fact.AlertEffect

	// fullMessage uses the complete announcement text as the alert message, instead of only its extra details.
	fullMessage bool
}

var intercityAnnouncementContents = map[source.IntercityAnnouncementType]intercityAnnouncementContent{
	source.NoDiningCar: {
		title:  "Brak wagonu gastronomicznego",
		effect: fact.EffectOther,
	},
	source.NoDiningService: {
		title:  "Brak usługi gastronomicznej",
		effect: fact.EffectOther,
	},
	source.NoWheelchairAccessibleCar: {
		title:  "Brak wagonu przystosowanego do przewozu osób z niepełnosprawnościami poruszających się na wózku",
		effect: fact.EffectAccessibilityIssue,
	},
	source.ReplacementTrainset: {
		title:  "Zastępczy skład wagonowy",
		effect: fact.EffectOther,
	},
	source.LateTrain: {
		title:       "Opóźnienie pociągu",
		effect:      fact.EffectSignificantDelays,
		fullMessage: true,
	},
	source.CancelledTrain: {
		title:       "Odwołanie pociągu",
		effect:      fact.EffectNoService,
		fullMessage: true,
	},
}

// IntercityAnnouncements converts PKP Intercity announcements into alerts.
// Identical announcements for multiple trains are merged into a single alert.
func IntercityAnnouncements(real []*source.IntercityAnnouncement, static *schedules.Package, stats *Stats) []*fact.Alert {
	alerts := make([]*fact.Alert, 0, len(real))
	alertsByID := make(map[string]*fact.Alert)

	for _, announcement := range real {
		a := IntercityAnnouncement(announcement, static, stats)
		if a == nil {
			continue
		}

		if existing := alertsByID[a.ID]; existing != nil {
			for _, trip := range a.Trips {
				if !slices.Contains(existing.Trips, trip) {
					existing.Trips = append(existing.Trips, trip)
				}
			}
		} else {
			alertsByID[a.ID] = a
			alerts = append(alerts, a)
		}
	}

	return alerts
}

func IntercityAnnouncement(real *source.IntercityAnnouncement, static *schedules.Package, stats *Stats) *fact.Alert {
	content, ok := intercityAnnouncementContents[real.Type]
	if !ok {
		slog.Debug("Ignoring PKP Intercity announcement of unknown type", "type", real.Type)
		return nil
	}

	trip := intercityTrip(real, static)
	if stats != nil {
		if trip != nil {
			stats.Matched++
		} else if !static.Dates.Contains(real.Date) {
			stats.OutsideFeedDates++
		} else {
			stats.Unmatched++
		}
	}
	if trip == nil {
		return nil
	}

	message := real.Extra
	if content.fullMessage {
		message = real.Full
	}

	return &fact.Alert{
		ID:      intercityAnnouncementID(real),
		Title:   content.title,
		Message: message,
		Effect:  content.effect,
		URL:     real.URL,
		Trips:   tripSelectors(trip),
	}
}

func intercityTrip(real *source.IntercityAnnouncement, static *schedules.Package) *schedules.Trip {
	for _, number := range intercityCandidateNumbers(real) {
		id := schedules.NumberID{AgencyID: intercityAgencyID, Number: number, PLKStartDate: real.Date}
		if t := static.TripsByNumber[id]; t != nil {
			return t
		}
	}
	return nil
}

// intercityCandidateNumbers returns the train numbers under which the announced train could
// be present in the static schedules. Announcements use numbers like "1320/1", which may
// correspond to any of "1320" and "1321" in the schedules.
func intercityCandidateNumbers(real *source.IntercityAnnouncement) (numbers []string) {
	for _, n := range []string{real.InternalNumber, real.Number} {
		base, _, _ := strings.Cut(n, "/")
		if base == "" {
			continue
		}
		numbers = append(numbers, base)

		if i, err := strconv.Atoi(base); err == nil {
			numbers = append(numbers, strconv.Itoa(i^1))
		}
	}
	return
}

func intercityAnnouncementID(real *source.IntercityAnnouncement) string {
	h := fnv.New64a()
	for _, s := range []string{string(real.Type), real.URL, real.Full, real.Extra} {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return fmt.Sprintf("A_IC_%016x", h.Sum64())
}
