// SPDX-FileCopyrightText: 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: MIT

package source

import (
	"context"
	"net/http"

	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/http2"
	"github.com/MKuranowski/PolishTrainsGTFS/polish_trains_gtfs/realtime/util/time2"
)

const intercityAnnouncementsURL = "https://gtfs.kasznia.net/announcements.json"

type IntercityAnnouncementType string

const (
	NoDiningCar               IntercityAnnouncementType = "NO_DINING_CAR"
	NoDiningService           IntercityAnnouncementType = "NO_DINING_SERVICE"
	NoWheelchairAccessibleCar IntercityAnnouncementType = "NO_WHEELCHAIR_ACCESSIBLE_CAR"
	ReplacementTrainset       IntercityAnnouncementType = "REPLACEMENT_TRAINSET"
	LateTrain                 IntercityAnnouncementType = "LATE_TRAIN"
	CancelledTrain            IntercityAnnouncementType = "CANCELLED_TRAIN"
)

// IntercityAnnouncement is a single train-related announcement published by PKP Intercity.
type IntercityAnnouncement struct {
	Type IntercityAnnouncementType `json:"type"`

	// Number is the public train number, e.g. "1320/1" or "67" for international trains.
	Number string `json:"num"`

	// InternalNumber is the domestic train number of international trains, e.g. "21010/1".
	InternalNumber string `json:"num2"`

	Date time2.Date `json:"date"`

	// Extra contains announcement details specific to the train, e.g. the replacement trainset composition.
	Extra string `json:"extra"`

	// Full is the complete announcement text for this train.
	Full string `json:"full"`
	URL  string `json:"url"`
}

func FetchIntercityAnnouncements(ctx context.Context, client http2.Doer) ([]*IntercityAnnouncement, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", intercityAnnouncementsURL, nil)
	if err != nil {
		return nil, err
	}

	announcements, err := http2.GetJSON[[]*IntercityAnnouncement](client, req)
	if err != nil || announcements == nil {
		return nil, err
	}
	return *announcements, nil
}
