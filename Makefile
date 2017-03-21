PREFIX ?= /usr/local
MANDIR ?= $(PREFIX)/share/man/man1/
BUILDTYPE ?= Release
SHELL = /bin/bash

# inherit from env if set
CC := $(CC)
CXX := $(CXX)
CFLAGS := $(CFLAGS)
CXXFLAGS := $(CXXFLAGS) -std=c++11
LDFLAGS := $(LDFLAGS)
WARNING_FLAGS := -Wall -Wshadow -Wsign-compare
RELEASE_FLAGS := -O3 -DNDEBUG
DEBUG_FLAGS := -O0 -DDEBUG -fno-inline-functions -fno-omit-frame-pointer

ifeq ($(BUILDTYPE),Release)
	FINAL_FLAGS := -g $(WARNING_FLAGS) $(RELEASE_FLAGS)
else
	FINAL_FLAGS := -g $(WARNING_FLAGS) $(DEBUG_FLAGS)
endif

PGMS := tile-count-create tile-count-decode tile-count-tile tile-count-merge

all: $(PGMS)

install: $(PGMS)
	cp $(PGMS) $(PREFIX)/bin

PG=

H = $(wildcard *.h) $(wildcard *.hpp)
C = $(wildcard *.c) $(wildcard *.cpp)

INCLUDES = -I/usr/local/include -I.
LIBS = -L/usr/local/lib

tile-count-create: tippecanoe/projection.o create.o header.o serial.o merge.o jsonpull/jsonpull.o
	$(CXX) $(PG) $(LIBS) $(FINAL_FLAGS) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) -lm -lz -lsqlite3 -lpthread

tile-count-decode: tippecanoe/projection.o decode.o header.o serial.o
	$(CXX) $(PG) $(LIBS) $(FINAL_FLAGS) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) -lm -lz -lsqlite3 -lpthread

tile-count-tile: tippecanoe/projection.o tile.o header.o serial.o tippecanoe/mbtiles.o tippecanoe/mvt.o
	$(CXX) $(PG) $(LIBS) $(FINAL_FLAGS) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) -lm -lz -lsqlite3 -lpthread -lpng

tile-count-merge: mergetool.o header.o serial.o merge.o
	$(CXX) $(PG) $(LIBS) $(FINAL_FLAGS) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) -lm -lz -lsqlite3 -lpthread

-include $(wildcard *.d)

%.o: %.c
	$(CC) -MMD $(PG) $(INCLUDES) $(FINAL_FLAGS) $(CFLAGS) -c -o $@ $<

%.o: %.cpp
	$(CXX) -MMD $(PG) $(INCLUDES) $(FINAL_FLAGS) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f ./tile-count-* *.o *.d */*.o */*.d

indent:
	clang-format -i -style="{BasedOnStyle: Google, IndentWidth: 8, UseTab: Always, AllowShortIfStatementsOnASingleLine: false, ColumnLimit: 0, ContinuationIndentWidth: 8, SpaceAfterCStyleCast: true, IndentCaseLabels: false, AllowShortBlocksOnASingleLine: false, AllowShortFunctionsOnASingleLine: false, SortIncludes: false}" $(C) $(H)

test: all
	rm -rf tests/tmp
	mkdir -p tests/tmp
	./tile-count-create -s20 -o tests/tmp/1.count tests/1.json
	./tile-count-create -o tests/tmp/2.count tests/2.json
	cat tests/1.json tests/2.json | ./tile-count-create -s16 -o tests/tmp/both.count
	# Verify merging of .count files
	./tile-count-merge -s16 -o tests/tmp/merged.count tests/tmp/1.count tests/tmp/2.count
	cmp tests/tmp/merged.count tests/tmp/both.count
	# Verify merging of vector mbtiles with separate features per bin
	./tile-count-tile -f -m0 -1 -y count -s16 -o tests/tmp/1.mbtiles tests/tmp/1.count
	./tile-count-tile -f -m0 -1 -y count -s16 -o tests/tmp/2.mbtiles tests/tmp/2.count
	./tile-count-tile -f -m0 -1 -y count -s16 -o tests/tmp/both.mbtiles tests/tmp/both.count
	./tile-count-tile -f -m0 -1 -y count -s16 -o tests/tmp/merged.mbtiles tests/tmp/1.mbtiles tests/tmp/2.mbtiles
	tippecanoe-decode tests/tmp/both.mbtiles | grep -v -e '"bounds"' -e '"center"' -e '"description"' -e '"max_density"' -e '"name"' > tests/tmp/both.geojson
	tippecanoe-decode tests/tmp/merged.mbtiles | grep -v -e '"bounds"' -e '"center"' -e '"description"' -e '"max_density"' -e '"name"' > tests/tmp/merged.geojson
	cmp tests/tmp/both.geojson tests/tmp/merged.geojson
	rm -rf tests/tmp
