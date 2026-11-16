# Builds railways.osm.pbf - railway ways of Poland and neighbouring regions served by
# international trains - used as the routing graph for shape generation.
#
# Usage (from this directory): make -j4
# Extracts are downloaded from Geofabrik only if their filtered version is missing, and deleted
# after filtering. `make update` applies Geofabrik replication diffs to the filtered extracts
# instead, and is meant to be run periodically. The *.railways.pbf files hold the update state
# (replication sequence in their header) - deleting them forces a full download.
#
# Cleaning targets, from least to most destructive:
#   make clean            removes railways.osm.pbf, rebuilt cheaply from the filtered extracts
#   make clean-downloads  removes full (and partially downloaded) Geofabrik extracts
#   make clean-all        removes everything, including the filtered extracts and their update
#                         state - the next build downloads full extracts again
# Requires curl, osmium-tool and Python with requirements.txt installed (set PYTHON if needed).

REGIONS := \
	europe/poland

NAMES := $(notdir $(REGIONS))
DOWNLOADS := $(addsuffix .osm.pbf,$(NAMES))
FILTERED := $(addsuffix .railways.pbf,$(NAMES))
TAGS_FILTER := w/railway=rail,construction
PYTHON ?= python3

.PHONY: all update clean clean-downloads clean-all
.DELETE_ON_ERROR:
.SECONDARY: $(DOWNLOADS) $(FILTERED)

all: railways.osm.pbf

railways.osm.pbf: $(FILTERED)
	osmium merge --overwrite -o $@ $^

%.railways.pbf: %.osm.pbf
	osmium tags-filter --overwrite -o $@ $< $(TAGS_FILTER)
	rm $<

update: $(FILTERED)
	PYTHONPATH=.. $(PYTHON) -m polish_trains_gtfs.update_railways -f $(TAGS_FILTER) -- $^
	$(MAKE) railways.osm.pbf

# All missing extracts are fetched by a single parallel curl, as separate concurrent
# curl processes garble each other's progress bars.
MISSING = $(filter-out $(wildcard $(DOWNLOADS)),$(DOWNLOADS))

$(DOWNLOADS) &:
	@echo "Downloading $(MISSING:.osm.pbf=)"
	@curl -fL --parallel --parallel-max 8 \
		$(foreach f,$(MISSING),-o $(f).part https://download.geofabrik.de/$(filter %/$(f:.osm.pbf=),$(REGIONS))-latest.osm.pbf)
	@$(foreach f,$(MISSING),mv $(f).part $(f);)

clean:
	rm -f railways.osm.pbf

clean-downloads:
	rm -f $(DOWNLOADS) $(addsuffix .part,$(DOWNLOADS))

clean-all: clean clean-downloads
	rm -f $(FILTERED)
