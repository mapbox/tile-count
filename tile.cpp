#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sqlite3.h>
#include <sys/stat.h>
#include <string>
#include <vector>
#include <set>
#include <map>
#include "tippecanoe/projection.hpp"
#include "header.hpp"
#include "serial.hpp"
#include "tippecanoe/mvt.hpp"
#include "tippecanoe/mbtiles.hpp"

void usage(char **argv) {
	fprintf(stderr, "Usage: %s [-fs] -z zoom -o out.mbtiles file.count\n", argv[0]);
}

struct tile {
	long long x;
	long long y;
	std::vector<long long> count;
	bool active;

	tile(size_t dim) {
		x = -1;
		y = -1;
		count.resize((1 << dim) * (1 << dim), 0);
		active = false;
	}
};

void make_tile(sqlite3 *outdb, tile const &tile, int z, int detail, bool square) {
	mvt_layer layer;
	layer.name = "count";
	layer.version = 2;
	layer.extent = 4096;

	for (size_t y = 0; y < (1 << detail); y++) {
		for (size_t x = 0; x < (1 << detail); x++) {
			long long count = tile.count[y * (1 << detail) + x];

			if (count != 0) {
				mvt_feature feature;
				feature.type = mvt_point;
				feature.geometry.push_back(mvt_geometry(mvt_moveto, x << (12 - detail), y << (12 - detail)));

				if (square) {
					feature.type = mvt_polygon;
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1) << (12 - detail), (y + 0) << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1) << (12 - detail), (y + 1) << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 0) << (12 - detail), (y + 1) << (12 - detail)));
					feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 0) << (12 - detail), (y + 0) << (12 - detail)));
				}

				mvt_value val;
				val.type = mvt_uint;
				val.numeric_value.uint_value = count;
				layer.tag(feature, "count", val);
				layer.features.push_back(feature);
			}
		}
	}

	mvt_tile mvt;
	mvt.layers.push_back(layer);

	std::string compressed = mvt.encode();
	if (compressed.size() > 500000) {
		fprintf(stderr, "Tile is too big: %zu\n", compressed.size());
		exit(EXIT_FAILURE);
	}

	mbtiles_write_tile(outdb, z, tile.x, tile.y, compressed.data(), compressed.size());
}

int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;

	char *outfile = NULL;
	int zoom = -1;
	bool force = false;
	bool square = false;

	int i;
	while ((i = getopt(argc, argv, "fz:o:s")) != -1) {
		switch (i) {
		case 'f':
			force = true;
			break;

		case 's':
			square = true;
			break;

		case 'z':
			zoom = atoi(optarg);
			break;

		case 'o':
			outfile = optarg;
			break;

		default:
			usage(argv);
			exit(EXIT_FAILURE);
		}
	}

	if (optind + 1 != argc || zoom < 0 || outfile == NULL) {
		usage(argv);
		exit(EXIT_FAILURE);
	}

	if (force) {
		unlink(outfile);
	}
	sqlite3 *outdb = mbtiles_open(outfile, argv, false);

	size_t detail = 9;
	size_t zooms = zoom - detail + 1;
	if (zooms < 1) {
		zooms = 1;
	}

	std::vector<tile> tiles;
	for (size_t z = 0; z < zooms; z++) {
		tiles.push_back(tile(detail));
	}

	long long file_bbox[4] = {UINT_MAX, UINT_MAX, 0, 0};
	long long midx = 0, midy = 0;

	for (; optind < argc; optind++) {
		struct stat st;
		if (stat(argv[optind], &st) != 0) {
			perror(optind[argv]);
			exit(EXIT_FAILURE);
		}
		long long records = (st.st_size - HEADER_LEN) / RECORD_BYTES;

		FILE *f = fopen(argv[optind], "rb");
		if (f == NULL) {
			perror(optind[argv]);
			exit(EXIT_FAILURE);
		}

		char header[HEADER_LEN];
		if (fread(header, HEADER_LEN, 1, f) != 1) {
			perror("read header");
			exit(EXIT_FAILURE);
		}

		if (memcmp(header, header_text, HEADER_LEN) != 0) {
			fprintf(stderr, "%s: not a tile-count file\n", argv[optind]);
			exit(EXIT_FAILURE);
		}

		unsigned char buf[RECORD_BYTES];
		long long seq = 0;
		long long percent = -1;
		long long max = 0;

		while (fread(buf, RECORD_BYTES, 1, f) == 1) {
			unsigned long long index = read64(buf);
			unsigned long long count = read32(buf + INDEX_BYTES);
			seq++;

			long long npercent = 100 * seq / records;
			if (npercent != percent) {
				percent = npercent;
				fprintf(stderr, "  %lld%%\r", percent);
			}

			unsigned wx, wy;
			decode(index, &wx, &wy);

			if (wx < file_bbox[0]) {
				file_bbox[0] = wx;
			}
			if (wy < file_bbox[1]) {
				file_bbox[1] = wy;
			}
			if (wx > file_bbox[2]) {
				file_bbox[2] = wx;
			}
			if (wy > file_bbox[3]) {
				file_bbox[3] = wy;
			}

			for (size_t z = 0; z < zooms; z++) {
				unsigned tx = wx, ty = wy;
				if (z + detail != 32) {
					tx >>= (32 - (z + detail));
					ty >>= (32 - (z + detail));
				}

				unsigned px = tx, py = ty;
				if (detail != 32) {
					px &= ((1 << detail) - 1);
					py &= ((1 << detail) - 1);

					tx >>= detail;
					ty >>= detail;
				} else {
					tx = 0;
					ty = 0;
				}

				if (tiles[z].x != tx || tiles[z].y != ty) {
					if (tiles[z].active) {
						make_tile(outdb, tiles[z], z, detail, square);
					}

					tiles[z].active = true;
					tiles[z].x = tx;
					tiles[z].y = ty;
					tiles[z].count.resize(0);
					tiles[z].count.resize((1 << detail) * (1 << detail), 0);
				}

				tiles[z].count[py * (1 << detail) + px] += count;

				if (tiles[z].count[py * (1 << detail) + px] > max) {
					max = tiles[z].count[py * (1 << detail) + px];
					midx = wx;
					midy = wy;
				}
			}
		}

		fclose(f);

		for (size_t z = 0; z < zooms; z++) {
			if (tiles[z].active) {
				make_tile(outdb, tiles[z], z, detail, square);
			}
		}
	}

	double minlat = 0, minlon = 0, maxlat = 0, maxlon = 0, midlat = 0, midlon = 0;

	tile2lonlat(midx, midy, 32, &midlon, &midlat);
	tile2lonlat(file_bbox[0], file_bbox[1], 32, &minlon, &maxlat);
	tile2lonlat(file_bbox[2], file_bbox[3], 32, &maxlon, &minlat);

	type_and_string tas;
	tas.type = VT_NUMBER;
	tas.string = "count";

	layermap_entry lme(0);
	lme.file_keys.insert(tas);
	lme.minzoom = 0;
	lme.maxzoom = zooms - 1;

	std::map<std::string, layermap_entry> lm;
	lm.insert(std::pair<std::string, layermap_entry>("count", lme));

	mbtiles_write_metadata(outdb, outfile, 0, zooms - 1, minlat, minlon, maxlat, maxlon, midlat, midlon, false, "", lm);
	mbtiles_close(outdb, argv);
}
