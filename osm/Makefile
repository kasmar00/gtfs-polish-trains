# Builds railways.osm.pbf - railway ways of Poland and neighbouring regions served by
# international trains - used as the routing graph for shape generation.
#
# Usage (from this directory): make -j4
# Extracts are downloaded from Geofabrik only if missing; run `make clean-downloads` to refresh.
# Requires curl and osmium-tool.

REGIONS := \
	europe/poland

NAMES := $(notdir $(REGIONS))
DOWNLOADS := $(addsuffix .osm.pbf,$(NAMES))
FILTERED := $(addsuffix .railways.pbf,$(NAMES))

.PHONY: all clean clean-downloads
.DELETE_ON_ERROR:
.SECONDARY: $(DOWNLOADS) $(FILTERED)

all: railways.osm.pbf

railways.osm.pbf: $(FILTERED)
	osmium merge --overwrite -o $@ $^

%.railways.pbf: %.osm.pbf
	osmium tags-filter --overwrite -o $@ $< w/railway=rail,construction

# All missing extracts are fetched by a single parallel curl, as separate concurrent
# curl processes garble each other's progress bars.
MISSING = $(filter-out $(wildcard $(DOWNLOADS)),$(DOWNLOADS))

$(DOWNLOADS) &:
	@echo "Downloading $(MISSING:.osm.pbf=)"
	@curl -fL --parallel --parallel-max 8 \
		$(foreach f,$(MISSING),-o $(f).part https://download.geofabrik.de/$(filter %/$(f:.osm.pbf=),$(REGIONS))-latest.osm.pbf)
	@$(foreach f,$(MISSING),mv $(f).part $(f);)

clean:
	rm -f railways.osm.pbf $(FILTERED)

clean-downloads:
	rm -f $(DOWNLOADS) $(addsuffix .part,$(DOWNLOADS))
