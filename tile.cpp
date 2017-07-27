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
#include <queue>
#include <algorithm>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <limits.h>
#include <math.h>
#include <time.h>
#include <png.h>
#include "tippecanoe/projection.hpp"
#include "protozero/varint.hpp"
#include "protozero/pbf_reader.hpp"
#include "protozero/pbf_writer.hpp"
#include "header.hpp"
#include "serial.hpp"
#include "tippecanoe/mvt.hpp"
#include "tippecanoe/mbtiles.hpp"

int levels = 50;
int first_level = 0;
int first_count = 0;
double count_gamma = 2.5;

bool bitmap = false;
int color = 0x888888;
int white = 0;

bool single_polygons = false;
bool limit_tile_sizes = true;
bool increment_threshold = false;
bool points = false;

bool quiet = false;
bool include_density = false;
bool include_count = false;

#define MAX_TILE_SIZE 500000

void usage(char **argv) {
	fprintf(stderr, "Usage: %s [options] -o out.mbtiles file.count\n", argv[0]);
}

struct tile {
	long long x;
	long long y;
	int z;
	std::vector<long long> count;
	bool active;

	tile(size_t dim, int zoom) {
		x = -1;
		y = -1;
		z = zoom;
		count.resize((1 << dim) * (1 << dim), 0);
		active = false;
	}
};

struct tiler {
	std::vector<tile> tiles;
	std::vector<tile> partial_tiles;
	std::vector<long long> max;       // for this thread
	std::vector<long long> zoom_max;  // global on 2nd pass
	size_t pass;
	size_t start;
	size_t end;
	long long bbox[4];
	long long midx, midy;
	long long atmid;

	FILE *fp;
	size_t minzoom;
	size_t zooms;
	size_t detail;
	sqlite3 *outdb;
	int maxzoom;

	volatile int *progress;
	size_t shard;
	size_t cpus;
	std::string layername;
	std::map<std::string, layermap_entry> *layermap;
};

void gather_quantile(tile const &tile, int detail, long long &max) {
	for (size_t y = 0; y < (1U << detail); y++) {
		for (size_t x = 0; x < (1U << detail); x++) {
			long long count = tile.count[y * (1 << detail) + x];

			if (count > max) {
				max = count;
			}
		}
	}
}

void string_append(png_structp png_ptr, png_bytep data, png_size_t length) {
	std::string *s = (std::string *) png_get_io_ptr(png_ptr);
	s->append(std::string(data, data + length));
}

static void fail(png_structp png_ptr, png_const_charp error_msg) {
	fprintf(stderr, "PNG error %s\n", error_msg);
	exit(EXIT_FAILURE);
}

void make_tile(sqlite3 *outdb, tile &otile, int z, int detail, long long zoom_max, std::string const &layername, std::map<std::string, layermap_entry> *layermap) {
	long long thresh = first_count;
	bool again = true;
	tile tile = otile;

	std::string compressed;

	while (again) {
		again = false;

		compressed = "";
		tile = otile;

		std::vector<long long> normalized;
		normalized.resize(tile.count.size());

		for (size_t y = 0; y < (1U << detail); y++) {
			for (size_t x = 0; x < (1U << detail); x++) {
				long long count = tile.count[y * (1 << detail) + x];
				long long density = 0;

				if (count > 0 && (count < first_count || count < thresh)) {
					count = 0;
					tile.count[y * (1 << detail) + x] = 0;
				}

				if (count > 0) {
					density = exp(log(exp(log(levels) * count_gamma) * count / zoom_max) / count_gamma);

					if (density < first_level) {
						density = 0;
						count = 0;
						tile.count[y * (1 << detail) + x] = 0;
					}
				}
				if (density > levels - 1) {
					density = levels - 1;
				}

				normalized[y * (1 << detail) + x] = density;
			}
		}

		if (bitmap) {
			bool anything = false;
			for (size_t y = 0; y < 1U << detail; y++) {
				for (size_t x = 0; x < (1U << detail); x++) {
					long long density = normalized[y * (1 << detail) + x];
					if (density > 0) {
						anything = true;
					}
				}
			}
			if (!anything) {
				return;
			}

			unsigned char *rows[1U << detail];
			for (size_t y = 0; y < 1U << detail; y++) {
				rows[y] = new unsigned char[1U << detail];

				for (size_t x = 0; x < (1U << detail); x++) {
					long long density = normalized[y * (1 << detail) + x];
					rows[y][x] = density;
				}
			}

			png_structp png_ptr;
			png_infop info_ptr;

			png_ptr = png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, fail, fail);
			if (png_ptr == NULL) {
				fprintf(stderr, "PNG failure (write struct)\n");
				exit(EXIT_FAILURE);
			}
			info_ptr = png_create_info_struct(png_ptr);
			if (info_ptr == NULL) {
				png_destroy_write_struct(&png_ptr, NULL);
				fprintf(stderr, "PNG failure (info struct)\n");
				exit(EXIT_FAILURE);
			} else {
				png_set_IHDR(png_ptr, info_ptr, 1U << detail, 1U << detail, 8, PNG_COLOR_TYPE_PALETTE, PNG_INTERLACE_NONE, PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT);

				png_byte transparency[levels];
				png_color colors[levels];
				for (int i = 0; i < levels / 2; i++) {
					colors[i].red = (color >> 16) & 0xFF;
					colors[i].green = (color >> 8) & 0xFF;
					colors[i].blue = (color >> 0) & 0xFF;
					transparency[i] = 255 * i / (levels / 2);
				}
				for (int i = levels / 2; i < levels; i++) {
					double along = 255 * (i - levels / 2) / (levels - levels / 2 - 1) / 255.0;
					int fg = white ? 0x00 : 0xFF;

					colors[i].red = ((color >> 16) & 0xFF) * (1 - along) + fg * (along);
					colors[i].green = ((color >> 8) & 0xFF) * (1 - along) + fg * (along);
					colors[i].blue = ((color >> 0) & 0xFF) * (1 - along) + fg * (along);
					transparency[i] = 255;
				}
				png_set_tRNS(png_ptr, info_ptr, transparency, levels, NULL);
				png_set_PLTE(png_ptr, info_ptr, colors, levels);

				png_set_rows(png_ptr, info_ptr, rows);
				png_set_write_fn(png_ptr, &compressed, string_append, NULL);
				png_write_png(png_ptr, info_ptr, 0, NULL);
				png_write_end(png_ptr, info_ptr);
				png_destroy_info_struct(png_ptr, &info_ptr);
				png_destroy_write_struct(&png_ptr, &info_ptr);
			}

			for (size_t i = 0; i < 1U << detail; i++) {
				delete[] rows[i];
			}
		} else {
			mvt_layer layer;
			layer.name = layername;
			layer.version = 2;
			layer.extent = 1U << detail;

			std::vector<mvt_feature> features;
			features.resize(levels);

			if (single_polygons) {
				for (size_t y = 0; y < (1U << detail); y++) {
					for (size_t x = 0; x < (1U << detail); x++) {
						if (tile.count[y * (1 << detail) + x] != 0) {
							mvt_feature feature;
							if (points) {
								feature.type = mvt_point;
							} else {
								feature.type = mvt_polygon;
							}

							feature.geometry.push_back(mvt_geometry(mvt_moveto, x, y));

							if (!points) {
								feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1), (y + 0)));
								feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1), (y + 1)));
								feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 0), (y + 1)));
								feature.geometry.push_back(mvt_geometry(mvt_closepath, 0, 0));
							}

							if (include_density) {
								mvt_value val;
								val.type = mvt_uint;
								val.numeric_value.uint_value = normalized[y * (1 << detail) + x];
								layer.tag(feature, "density", val);

								type_and_string attrib;
								attrib.type = mvt_double;
								attrib.string = std::to_string(val.numeric_value.uint_value);

								auto fk = layermap->find(layername);
								add_to_file_keys(fk->second.file_keys, "density", attrib);
							}

							if (include_count) {
								mvt_value val;
								val.type = mvt_uint;
								val.numeric_value.uint_value = tile.count[y * (1 << detail) + x];
								layer.tag(feature, "count", val);

								type_and_string attrib;
								attrib.type = mvt_double;
								attrib.string = std::to_string(val.numeric_value.uint_value);

								auto fk = layermap->find(layername);
								add_to_file_keys(fk->second.file_keys, "count", attrib);
							}

							layer.features.push_back(feature);
						}
					}
				}
			} else {
				for (size_t y = 0; y < (1U << detail); y++) {
					for (size_t x = 0; x < (1U << detail); x++) {
						long long density = normalized[y * (1 << detail) + x];
						if (density != 0) {
							mvt_feature &feature = features[density];
							if (points) {
								feature.type = mvt_point;
							} else {
								feature.type = mvt_polygon;
							}

							feature.geometry.push_back(mvt_geometry(mvt_moveto, x, y));

							if (!points) {
								feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1), (y + 0)));
								feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 1), (y + 1)));
								feature.geometry.push_back(mvt_geometry(mvt_lineto, (x + 0), (y + 1)));
								feature.geometry.push_back(mvt_geometry(mvt_closepath, 0, 0));
							}
						}
					}
				}

				for (size_t i = first_level; i < features.size(); i++) {
					if (features[i].geometry.size() != 0) {
						// features[i].geometry = merge_rings(features[i].geometry);

						long long count = ceil(exp(log(i) * count_gamma) * zoom_max / exp(log(levels) * count_gamma));
						if (count < first_count) {
							continue;
						}

						if (include_density) {
							mvt_value val;
							val.type = mvt_uint;
							val.numeric_value.uint_value = i;
							layer.tag(features[i], "density", val);

							type_and_string attrib;
							attrib.type = mvt_double;
							attrib.string = std::to_string(val.numeric_value.uint_value);

							auto fk = layermap->find(layername);
							add_to_file_keys(fk->second.file_keys, "density", attrib);
						}

						if (include_count) {
							mvt_value val;
							val.type = mvt_uint;
							val.numeric_value.uint_value = count;
							layer.tag(features[i], "count", val);

							type_and_string attrib;
							attrib.type = mvt_double;
							attrib.string = std::to_string(val.numeric_value.uint_value);

							auto fk = layermap->find(layername);
							add_to_file_keys(fk->second.file_keys, "count", attrib);
						}

						layer.features.push_back(features[i]);
					}
				}
			}

			if (layer.features.size() > 0) {
				mvt_tile mvt;
				mvt.layers.push_back(layer);

				compressed = mvt.encode();
			}
		}

		if (compressed.size() == 0) {
			return;
		}

		if (compressed.size() > MAX_TILE_SIZE && increment_threshold) {
			std::vector<long long> vals;
			for (size_t i = 0; i < tile.count.size(); i++) {
				if (tile.count[i] > 0) {
					vals.push_back(tile.count[i]);
				}
			}
			std::sort(vals.begin(), vals.end());
			ssize_t n = ceil(vals.size() - vals.size() * MAX_TILE_SIZE / compressed.size() * 0.95);
			if (n >= (ssize_t) vals.size()) {
				n = vals.size() - 1;
			}
			if (n < 0) {
				n = 0;
			}
			thresh = vals[n] + 1;

			fprintf(stderr, "Raising threshold to %lld for %zu bytes in tile %d/%lld/%lld\n", thresh, compressed.size(), z, tile.x, tile.y);
			again = true;
			continue;
		}

		if (limit_tile_sizes && compressed.size() > MAX_TILE_SIZE) {
			fprintf(stderr, "Tile is too big: %zu\n", compressed.size());
			exit(EXIT_FAILURE);
		}
	}

	static pthread_mutex_t db_lock = PTHREAD_MUTEX_INITIALIZER;

	if (pthread_mutex_lock(&db_lock) != 0) {
		perror("pthread_mutex_lock");
		exit(EXIT_FAILURE);
	}

	mbtiles_write_tile(outdb, z, tile.x, tile.y, compressed.data(), compressed.size());

	if (pthread_mutex_unlock(&db_lock) != 0) {
		perror("pthread_mutex_unlock");
		exit(EXIT_FAILURE);
	}
}

void calc_tile_edges(size_t z, long long x, long long y, unsigned long long &start, unsigned long long &end) {
	start = encode(x << (32 - z), y << (32 - z));
	end = start;

	for (size_t i = 0; i < 32 - z; i++) {
		end |= 3ULL << (2 * i);
	}
}

void *run_tile(void *p) {
	tiler *t = (tiler *) p;

	if (t->start >= t->end) {
		return NULL;
	}

	unsigned char firstbuf[RECORD_BYTES];
	unsigned char lastbuf[RECORD_BYTES];

	if (fseeko(t->fp, t->start * RECORD_BYTES + HEADER_LEN, SEEK_SET) != 0) {
		perror("fseeko");
		exit(EXIT_FAILURE);
	}
	if (fread(firstbuf, RECORD_BYTES, 1, t->fp) != 1) {
		perror("fread");
		exit(EXIT_FAILURE);
	}

	if (fseeko(t->fp, (t->end - 1) * RECORD_BYTES + HEADER_LEN, SEEK_SET) != 0) {
		perror("fseeko");
		exit(EXIT_FAILURE);
	}
	if (fread(lastbuf, RECORD_BYTES, 1, t->fp) != 1) {
		perror("fread");
		exit(EXIT_FAILURE);
	}

	unsigned long long first = read64(firstbuf);
	unsigned long long last = read64(lastbuf);

	long long seq = 0;
	long long percent = -1;
	long long max = 0;

	if (fseeko(t->fp, t->start * RECORD_BYTES + HEADER_LEN, SEEK_SET) != 0) {
		perror("fseeko");
		exit(EXIT_FAILURE);
	}

	unsigned long long oindex = 0;
	for (size_t i = t->start; i < t->end; i++) {
		unsigned char buf[RECORD_BYTES];
		if (fread(buf, RECORD_BYTES, 1, t->fp) != 1) {
			perror("fread");
			exit(EXIT_FAILURE);
		}
		unsigned long long index = read64(buf);
		unsigned long long count = read32(buf + INDEX_BYTES);
		seq++;

		if (oindex > index) {
			fprintf(stderr, "out of order: %llx vs %llx\n", oindex, index);
		}
		oindex = index;

		long long npercent = 100 * seq / (t->end - t->start);
		if (npercent != percent) {
			percent = npercent;
			t->progress[t->shard] = percent;

			int sum = 0;
			for (size_t j = 0; j < t->cpus; j++) {
				sum += t->progress[j];
			}
			sum /= t->cpus;

			if (!quiet) {
				fprintf(stderr, "  %lu%%\r", sum / 2 + 50 * t->pass);
			}
		}

		unsigned wx, wy;
		decode(index, &wx, &wy);

		if (wx < t->bbox[0]) {
			t->bbox[0] = wx;
		}
		if (wy < t->bbox[1]) {
			t->bbox[1] = wy;
		}
		if (wx > t->bbox[2]) {
			t->bbox[2] = wx;
		}
		if (wy > t->bbox[3]) {
			t->bbox[3] = wy;
		}

		for (size_t z = t->minzoom; z < t->zooms; z++) {
			unsigned tx = wx, ty = wy;
			if (z + t->detail != 32) {
				tx >>= (32 - (z + t->detail));
				ty >>= (32 - (z + t->detail));
			}

			unsigned px = tx, py = ty;
			if (t->detail != 32) {
				px &= ((1 << t->detail) - 1);
				py &= ((1 << t->detail) - 1);

				tx >>= t->detail;
				ty >>= t->detail;
			} else {
				tx = 0;
				ty = 0;
			}

			if (t->tiles[z].x != tx || t->tiles[z].y != ty) {
				if (t->tiles[z].active) {
					unsigned long long first_for_tile, last_for_tile;
					calc_tile_edges(z, t->tiles[z].x, t->tiles[z].y, first_for_tile, last_for_tile);

					// printf("%zu/%lld/%lld: %llx (%llx %llx) %llx\n", z, t->tiles[z].x, t->tiles[z].y, first, first_for_tile, last_for_tile, last);

					if (first_for_tile >= first && last_for_tile <= last) {
						if (t->pass == 0) {
							gather_quantile(t->tiles[z], t->detail, t->max[z]);
						} else {
							make_tile(t->outdb, t->tiles[z], z, t->detail, t->zoom_max[z], t->layername, t->layermap);
						}
					} else {
						t->partial_tiles.push_back(t->tiles[z]);
					}
				}

				t->tiles[z].active = true;
				t->tiles[z].x = tx;
				t->tiles[z].y = ty;

				for (size_t x = 0; x < (1U << t->detail) * (1U << t->detail); x++) {
					t->tiles[z].count[x] = 0;
				}
			}

			t->tiles[z].count[py * (1 << t->detail) + px] += count;

			if (z == t->zooms - 1 && t->tiles[z].count[py * (1 << t->detail) + px] > max) {
				max = t->tiles[z].count[py * (1 << t->detail) + px];
				t->midx = wx;
				t->midy = wy;
				t->atmid = max;
			}
		}
	}

	for (size_t z = t->minzoom; z < t->zooms; z++) {
		if (t->tiles[z].active) {
			unsigned long long first_for_tile, last_for_tile;
			calc_tile_edges(z, t->tiles[z].x, t->tiles[z].y, first_for_tile, last_for_tile);

			// printf("%zu/%lld/%lld: %llx (%llx %llx) %llx\n", z, t->tiles[z].x, t->tiles[z].y, first, first_for_tile, last_for_tile, last);

			if (first_for_tile >= first && last_for_tile <= last) {
				if (t->pass == 0) {
					gather_quantile(t->tiles[z], t->detail, t->max[z]);
				} else {
					make_tile(t->outdb, t->tiles[z], z, t->detail, t->zoom_max[z], t->layername, t->layermap);
				}
			} else {
				t->partial_tiles.push_back(t->tiles[z]);
			}
		}
	}

	return NULL;
}

void regress(std::vector<long long> &max, size_t minzoom) {
	for (size_t i = minzoom; i < max.size(); i++) {
		if (max[i] == 0) {
			max[i] = 1;
		}
	}

	double sum_x = 0;
	double sum_y = 0;
	double sum_x2 = 0;
	double sum_y2 = 0;
	double sum_xy = 0;
	size_t n = 0;

	for (size_t i = minzoom; i < max.size(); i++) {
		double x = i;
		double y = log(max[i]);

		sum_x += x;
		sum_y += y;
		sum_xy += x * y;
		sum_x2 += x * x;
		sum_y2 += y * y;

		n++;
	}

	double m = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - (sum_x * sum_x));
	double b = (sum_y * sum_x2 - sum_x * sum_xy) / (n * sum_x2 - (sum_x * sum_x));

	for (size_t i = minzoom; i < max.size(); i++) {
		max[i] = exp(m * i + b);
		if (max[i] < 1) {
			max[i] = 1;
		}
	}
}

void write_meta(std::vector<long long> const &zoom_max, sqlite3 *outdb) {
	char *sql, *err;

	std::string maxes;
	for (size_t i = 0; i < zoom_max.size(); i++) {
		maxes.append(std::to_string(zoom_max[i]));
		maxes.append(",");
	}

	sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('max_density', %Q);", maxes.c_str());
	if (sqlite3_exec(outdb, sql, NULL, NULL, &err) != SQLITE_OK) {
		fprintf(stderr, "set name in metadata: %s\n", err);
		exit(EXIT_FAILURE);
	}
	sqlite3_free(sql);

	sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('density_levels', %d);", levels);
	if (sqlite3_exec(outdb, sql, NULL, NULL, &err) != SQLITE_OK) {
		fprintf(stderr, "set name in metadata: %s\n", err);
		exit(EXIT_FAILURE);
	}
	sqlite3_free(sql);

	sql = sqlite3_mprintf("INSERT INTO metadata (name, value) VALUES ('density_gamma', %f);", count_gamma);
	if (sqlite3_exec(outdb, sql, NULL, NULL, &err) != SQLITE_OK) {
		fprintf(stderr, "set name in metadata: %s\n", err);
		exit(EXIT_FAILURE);
	}
	sqlite3_free(sql);
}

struct tile_reader {
	sqlite3 *db = NULL;
	sqlite3_stmt *stmt = NULL;
	sqlite3 *outdb = NULL;
	std::string name;
	std::string layername;
	std::string format;

	int zoom = 0;
	int x = 0;
	int sorty = 0;
	std::string data;

	int density_levels;
	double density_gamma;
	std::vector<long long> max_density;
	std::vector<long long> global_density;
	std::map<std::string, layermap_entry> *layermap;

	int y() {
		return (1LL << zoom) - 1 - sorty;
	}

	// Comparisons are backwards because priority_queue puts highest first
	bool operator<(const tile_reader r) const {
		if (zoom > r.zoom) {
			return true;
		}
		if (zoom < r.zoom) {
			return false;
		}

		if (x > r.x) {
			return true;
		}
		if (x < r.x) {
			return false;
		}

		if (sorty > r.sorty) {
			return true;
		}
		if (sorty < r.sorty) {
			return false;
		}

		if (data > r.data) {
			return true;
		}

		return false;
	}
};

struct read_state {
	const char *base;
	size_t off;
	size_t len;
};

void user_read_data(png_structp png_ptr, png_bytep data, png_size_t length) {
	struct read_state *state = (struct read_state *) png_get_io_ptr(png_ptr);

	if (state->off + length > state->len) {
		length = state->len - state->off;
	}

	memcpy(data, state->base + state->off, length);
	state->off += length;
}

void *retile(void *v) {
	std::vector<tile_reader *> *queue = (std::vector<tile_reader *> *) v;

	tile t(0, 0);

	for (size_t i = 0; i < queue->size(); i++) {
		if ((*queue)[i]->format == std::string("png")) {
			png_structp png_ptr;
			png_infop info_ptr;

			struct read_state state;
			state.base = (*queue)[i]->data.c_str();
			state.off = 0;
			state.len = (*queue)[i]->data.length();

			png_ptr = png_create_read_struct(PNG_LIBPNG_VER_STRING, NULL, fail, fail);
			if (png_ptr == NULL) {
				fprintf(stderr, "PNG init failed\n");
				exit(EXIT_FAILURE);
			}

			info_ptr = png_create_info_struct(png_ptr);
			if (info_ptr == NULL) {
				fprintf(stderr, "PNG init failed\n");
				exit(EXIT_FAILURE);
			}

			png_set_read_fn(png_ptr, &state, user_read_data);
			png_set_sig_bytes(png_ptr, 0);

			png_read_png(png_ptr, info_ptr, 0, NULL);

			png_uint_32 width, height;
			int bit_depth;
			int color_type, interlace_type;

			png_get_IHDR(png_ptr, info_ptr, &width, &height, &bit_depth, &color_type, &interlace_type, NULL, NULL);
			if (bit_depth != 8 || color_type != PNG_COLOR_TYPE_PALETTE || width != height) {
				fprintf(stderr, "Misencoded PNG\n");
				exit(EXIT_FAILURE);
			}

			if (!t.active || t.z != (*queue)[i]->zoom || t.x != (*queue)[i]->x || t.y != (*queue)[i]->y() || t.count.size() != width * height) {
				if (t.active) {
					make_tile((*queue)[i]->outdb, t, t.z, log(sqrt(t.count.size())) / log(2), (*queue)[i]->global_density[t.z], (*queue)[i]->layername, (*queue)[i]->layermap);
				}

				t.active = true;
				t.z = (*queue)[i]->zoom;
				t.x = (*queue)[i]->x;
				t.y = (*queue)[i]->y();
				t.count.resize(width * height);

				for (size_t j = 0; j < width * height; j++) {
					t.count[j] = 0;
				}
			}

			double gamma = (*queue)[i]->density_gamma;
			long long zoom_max = (*queue)[i]->max_density[(*queue)[i]->zoom];
			size_t density_levels = (*queue)[i]->density_levels;

			png_bytepp row_pointers = png_get_rows(png_ptr, info_ptr);
			for (size_t y = 0; y < height; y++) {
				unsigned char *bytes = row_pointers[y];

				for (size_t x = 0; x < width; x++) {
					if (bytes[x] > 0) {
						double bright = bytes[x];
						long long count = ceil(exp(log(bright) * gamma) * zoom_max / exp(log(density_levels) * gamma));
#if 0
						int back = exp(log(exp(log(density_levels) * gamma) * count / zoom_max) / gamma);
						if (back != bytes[x]) {
							fprintf(stderr, "put in %d, got back %d (bitmap)\n", bytes[x], back);
							exit(EXIT_FAILURE);
						}
#endif
						t.count[width * y + x] += count;
					}
				}
			}

			png_destroy_read_struct(&png_ptr, &info_ptr, NULL);
		} else {
			mvt_tile tile;

			try {
				bool was_compressed;

				if (!tile.decode((*queue)[i]->data, was_compressed)) {
					fprintf(stderr, "Couldn't parse tile\n");
					exit(EXIT_FAILURE);
				}
			} catch (protozero::unknown_pbf_wire_type_exception e) {
				fprintf(stderr, "PBF decoding error in tile\n");
				exit(EXIT_FAILURE);
			}

			for (size_t l = 0; l < tile.layers.size(); l++) {
				mvt_layer &layer = tile.layers[l];
				size_t extent = layer.extent;

				double gamma = (*queue)[i]->density_gamma;
				long long zoom_max = (*queue)[i]->max_density[(*queue)[i]->zoom];
				size_t density_levels = (*queue)[i]->density_levels;

				if (!t.active || t.z != (*queue)[i]->zoom || t.x != (*queue)[i]->x || t.y != (*queue)[i]->y() || t.count.size() != extent * extent) {
					if (t.active) {
						make_tile((*queue)[i]->outdb, t, t.z, log(sqrt(t.count.size())) / log(2), (*queue)[i]->global_density[t.z], (*queue)[i]->layername, (*queue)[i]->layermap);
					}

					t.active = true;
					t.z = (*queue)[i]->zoom;
					t.x = (*queue)[i]->x;
					t.y = (*queue)[i]->y();
					t.count.resize(extent * extent);

					for (size_t j = 0; j < extent * extent; j++) {
						t.count[j] = 0;
					}
				}

				for (size_t f = 0; f < layer.features.size(); f++) {
					mvt_feature &feat = layer.features[f];
					double density = -1;
					long long count = -1;

					for (size_t tag = 0; tag + 1 < feat.tags.size(); tag += 2) {
						if (feat.tags[tag] >= layer.keys.size()) {
							fprintf(stderr, "Error: out of bounds feature key\n");
							exit(EXIT_FAILURE);
						}
						if (feat.tags[tag + 1] >= layer.values.size()) {
							fprintf(stderr, "Error: out of bounds feature value\n");
							exit(EXIT_FAILURE);
						}

						std::string key = layer.keys[feat.tags[tag]];
						mvt_value const &val = layer.values[feat.tags[tag + 1]];

						if (key == std::string("density")) {
							if (val.type == mvt_uint) {
								density = val.numeric_value.uint_value;
								count = ceil(exp(log(density) * gamma) * zoom_max / exp(log(density_levels) * gamma));

#if 0
								int back = exp(log(exp(log(density_levels) * gamma) * count / zoom_max) / gamma);
								if (back != val.numeric_value.uint_value) {
									fprintf(stderr, "put in %llu, got back %d (vector)\n", val.numeric_value.uint_value, back);
									exit(EXIT_FAILURE);
								}
#endif
							}
						}
						if (key == std::string("count")) {
							if (val.type == mvt_uint) {
								count = val.numeric_value.uint_value;
							}
						}
					}

					if (density < 0 && count < 0) {
						fprintf(stderr, "Can't find density or count attribute in feature being merged\n");
						exit(EXIT_FAILURE);
					}

					for (size_t g = 0; g < feat.geometry.size(); g++) {
						// XXX This thinks it knows that the moveto is always the top left of the pixel

						if (feat.geometry[g].op == mvt_moveto) {
							if (feat.geometry[g].x >= 0 &&
							    feat.geometry[g].y >= 0 &&
							    feat.geometry[g].x < (ssize_t) extent &&
							    feat.geometry[g].y < (ssize_t) extent) {
								t.count[extent * feat.geometry[g].y + feat.geometry[g].x] += count;
							}
						}
					}
				}
			}
		}
	}

	if (t.active) {
		make_tile((*queue)[0]->outdb, t, t.z, log(sqrt(t.count.size())) / log(2), (*queue)[0]->global_density[t.z], (*queue)[0]->layername, (*queue)[0]->layermap);
	}

	return NULL;
}

void merge(std::vector<tile_reader> &r, size_t cpus, std::vector<std::map<std::string, layermap_entry>> &layermaps) {
	std::vector<std::vector<tile_reader *>> queues;
	queues.resize(cpus);

	size_t o = 0;
	for (size_t i = 0; i < r.size(); i++) {
		r[i].layermap = &layermaps[o];
		queues[o].push_back(&r[i]);

		if (i + 1 < r.size() && (r[i].x != r[i + 1].x || r[i].y() != r[i + 1].y() || r[i].zoom != r[i + 1].zoom)) {
			o = (o + 1) % cpus;
		}
	}

	pthread_t pthreads[cpus];

	for (size_t i = 0; i < cpus; i++) {
		if (pthread_create(&pthreads[i], NULL, retile, &queues[i]) != 0) {
			perror("pthread_create");
			exit(EXIT_FAILURE);
		}
	}

	for (size_t i = 0; i < cpus; i++) {
		void *ret;

		if (pthread_join(pthreads[i], &ret) != 0) {
			perror("pthread_join");
			exit(EXIT_FAILURE);
		}
	}
}

std::vector<long long> parse_max_density(const unsigned char *v) {
	std::vector<long long> out;

	while (*v != '\0') {
		out.push_back(atoll((const char *) v));

		while (*v != '\0' && *v != ',') {
			v++;
		}
		if (*v == ',') {
			v++;
		}
	}

	return out;
}

void merge_tiles(char **fnames, size_t n, size_t cpus, sqlite3 *outdb, int zooms, std::vector<long long> &zoom_max, double &midlat, double &midlon, double &minlat, double &minlon, double &maxlat, double &maxlon, std::string const &layername, std::vector<std::map<std::string, layermap_entry>> &layermaps) {
	std::vector<tile_reader> readers;
	size_t total_rows = 0;
	size_t seq = 0;
	size_t oprogress = 999;
	size_t biggest = 0;

	for (size_t i = 0; i < n; i++) {
		tile_reader r;
		r.name = fnames[i];
		r.outdb = outdb;

		if (sqlite3_open(fnames[i], &r.db) != SQLITE_OK) {
			fprintf(stderr, "%s: %s\n", fnames[i], sqlite3_errmsg(r.db));
			exit(EXIT_FAILURE);
		}

		sqlite3_stmt *stmt;
		if (sqlite3_prepare_v2(r.db, "SELECT value from metadata where name = 'max_density';", -1, &stmt, NULL) == SQLITE_OK) {
			if (sqlite3_step(stmt) == SQLITE_ROW) {
				r.max_density = parse_max_density(sqlite3_column_text(stmt, 0));
			} else {
				fprintf(stderr, "%s: No max_density value in metadata\n", fnames[i]);
				exit(EXIT_FAILURE);
			}
			sqlite3_finalize(stmt);
		}

		if (sqlite3_prepare_v2(r.db, "SELECT value from metadata where name = 'density_levels';", -1, &stmt, NULL) == SQLITE_OK) {
			if (sqlite3_step(stmt) == SQLITE_ROW) {
				r.density_levels = sqlite3_column_int(stmt, 0);
			} else {
				fprintf(stderr, "%s: No density_levels value in metadata\n", fnames[i]);
				exit(EXIT_FAILURE);
			}
			sqlite3_finalize(stmt);
		}

		if (sqlite3_prepare_v2(r.db, "SELECT value from metadata where name = 'density_gamma';", -1, &stmt, NULL) == SQLITE_OK) {
			if (sqlite3_step(stmt) == SQLITE_ROW) {
				r.density_gamma = sqlite3_column_double(stmt, 0);
			} else {
				fprintf(stderr, "%s: No density_gamma value in metadata\n", fnames[i]);
				exit(EXIT_FAILURE);
			}
			sqlite3_finalize(stmt);
		}

		if (sqlite3_prepare_v2(r.db, "SELECT value from metadata where name = 'maxzoom';", -1, &stmt, NULL) == SQLITE_OK) {
			if (sqlite3_step(stmt) == SQLITE_ROW) {
				if (sqlite3_column_int(stmt, 0) + 1 != zooms) {
					fprintf(stderr, "%s: Mismatched number of zoom levels (%d)\n", fnames[i], sqlite3_column_int(stmt, 0) + 1);
					exit(EXIT_FAILURE);
				}
			}
			sqlite3_finalize(stmt);
		}

		if (sqlite3_prepare_v2(r.db, "SELECT value from metadata where name = 'format';", -1, &stmt, NULL) == SQLITE_OK) {
			if (sqlite3_step(stmt) == SQLITE_ROW) {
				r.format = (const char *) sqlite3_column_text(stmt, 0);
			} else {
				fprintf(stderr, "%s: No format value in metadata\n", fnames[i]);
				exit(EXIT_FAILURE);
			}
			sqlite3_finalize(stmt);
		}

		if (sqlite3_prepare_v2(r.db, "SELECT max(rowid) from tiles;", -1, &stmt, NULL) == SQLITE_OK) {
			if (sqlite3_step(stmt) == SQLITE_ROW) {
				total_rows += sqlite3_column_int(stmt, 0);
			}
			sqlite3_finalize(stmt);
		}

		const char *sql = "SELECT zoom_level, tile_column, tile_row, tile_data from tiles order by zoom_level, tile_column, tile_row;";
		if (sqlite3_prepare_v2(r.db, sql, -1, &r.stmt, NULL) != SQLITE_OK) {
			fprintf(stderr, "%s: select failed: %s\n", fnames[i], sqlite3_errmsg(r.db));
			exit(EXIT_FAILURE);
		}

		if (sqlite3_step(r.stmt) == SQLITE_ROW) {
			r.zoom = sqlite3_column_int(r.stmt, 0);
			r.x = sqlite3_column_int(r.stmt, 1);
			r.sorty = sqlite3_column_int(r.stmt, 2);
			r.layername = layername;

			const char *data = (const char *) sqlite3_column_blob(r.stmt, 3);
			size_t len = sqlite3_column_bytes(r.stmt, 3);
			r.data = std::string(data, len);

			if (r.zoom == zooms - 1 && r.data.size() > biggest) {
				biggest = r.data.size();
				projection->unproject(r.x, r.y(), r.zoom, &midlon, &midlat);
			}

			readers.push_back(r);
			seq++;

			// This check is here instead of above so the bounding box is only affected if there are
			// tiles in the tileset being merged.

			if (sqlite3_prepare_v2(r.db, "SELECT value from metadata where name = 'bounds';", -1, &stmt, NULL) == SQLITE_OK) {
				if (sqlite3_step(stmt) == SQLITE_ROW) {
					const unsigned char *bbox = sqlite3_column_text(stmt, 0);
					double o1, a1, o2, a2;
					if (sscanf((const char *) bbox, "%lf,%lf,%lf,%lf", &o1, &a1, &o2, &a2) == 4) {
						if (o1 < minlon) {
							minlon = o1;
						}
						if (a1 < minlat) {
							minlat = a1;
						}
						if (o2 > maxlon) {
							maxlon = o2;
						}
						if (a2 > maxlat) {
							maxlat = a2;
						}
					}
				}
				sqlite3_finalize(stmt);
			}
		} else {
			sqlite3_finalize(r.stmt);

			if (sqlite3_close(r.db) != SQLITE_OK) {
				fprintf(stderr, "%s: could not close database: %s\n", r.name.c_str(), sqlite3_errmsg(r.db));
				exit(EXIT_FAILURE);
			}
		}
	}

	// XXX This doesn't detect if two sources brighten the same pixel together
	for (size_t i = 0; i < readers.size(); i++) {
		if (readers[i].max_density.size() > zoom_max.size()) {
			zoom_max.resize(readers[i].max_density.size());
		}

		for (size_t j = 0; j < readers[i].max_density.size(); j++) {
			if (readers[i].max_density[j] > zoom_max[j]) {
				zoom_max[j] = readers[i].max_density[j];
			}
		}
	}

	std::priority_queue<tile_reader> reader_q;
	for (size_t i = 0; i < readers.size(); i++) {
		readers[i].global_density = zoom_max;
		reader_q.push(readers[i]);
	}
	readers.clear();

	std::vector<tile_reader> to_merge;
	while (reader_q.size() != 0) {
		tile_reader r = reader_q.top();
		reader_q.pop();

		if (to_merge.size() > 50 * cpus) {
			tile_reader &last = to_merge[to_merge.size() - 1];
			if (r.x != last.x || r.y() != last.y() || r.zoom != last.zoom) {
				merge(to_merge, cpus, layermaps);
				to_merge.clear();
			}
		}

		to_merge.push_back(r);

		if (sqlite3_step(r.stmt) == SQLITE_ROW) {
			r.zoom = sqlite3_column_int(r.stmt, 0);
			r.x = sqlite3_column_int(r.stmt, 1);
			r.sorty = sqlite3_column_int(r.stmt, 2);
			r.layername = layername;

			const char *data = (const char *) sqlite3_column_blob(r.stmt, 3);
			size_t len = sqlite3_column_bytes(r.stmt, 3);
			r.data = std::string(data, len);

			if (r.zoom == zooms - 1 && r.data.size() > biggest) {
				biggest = r.data.size();
				projection->unproject(r.x, r.y(), r.zoom, &midlon, &midlat);
			}

			reader_q.push(r);
			seq++;

			size_t progress = 100 * seq / total_rows;
			if (progress != oprogress) {
				if (!quiet) {
					fprintf(stderr, "  %zu%%\r", progress);
					oprogress = progress;
				}
			}
		} else {
			sqlite3_finalize(r.stmt);

			if (sqlite3_close(r.db) != SQLITE_OK) {
				fprintf(stderr, "%s: could not close database: %s\n", r.name.c_str(), sqlite3_errmsg(r.db));
				exit(EXIT_FAILURE);
			}
		}
	}

	merge(to_merge, cpus, layermaps);
}

int main(int argc, char **argv) {
	extern int optind;
	extern char *optarg;

	char *outfile = NULL;
	int minzoom = 0;
	int maxzoom = -1;
	int bin = -1;
	bool force = false;
	size_t detail = 9;
	size_t cpus = sysconf(_SC_NPROCESSORS_ONLN);
	std::string layername = "count";

	int i;
	while ((i = getopt(argc, argv, "fz:Z:s:a:o:p:d:l:m:M:g:bwc:qn:y:1kKP")) != -1) {
		switch (i) {
		case 'f':
			force = true;
			break;

		case 'z':
			maxzoom = atoi(optarg);
			break;

		case 'Z':
			minzoom = atoi(optarg);
			break;

		case 's':
			bin = atoi(optarg);
			break;

		case 'p':
			cpus = atoi(optarg);
			break;

		case 'd':
			detail = atoi(optarg);
			break;

		case 'q':
			quiet = true;
			break;

		case 'k':
			limit_tile_sizes = false;
			break;

		case 'K':
			increment_threshold = true;
			break;

		case 'l':
			levels = atoi(optarg);
			if (levels < 1 || levels > 256) {
				fprintf(stderr, "%s: Levels (-l%s) cannot exceed 256\n", argv[0], optarg);
				exit(EXIT_FAILURE);
			}
			break;

		case 'n':
			layername = optarg;
			break;

		case 'm':
			first_level = atoi(optarg);
			break;

		case 'M':
			first_count = atoi(optarg);
			break;

		case 'y':
			if (strcmp(optarg, "count") == 0) {
				include_count = true;
			} else if (strcmp(optarg, "density") == 0) {
				include_density = true;
			} else {
				fprintf(stderr, "Unknown attribute: -y %s\n", optarg);
				exit(EXIT_FAILURE);
			}

			break;

		case 'g':
			count_gamma = atof(optarg);
			break;

		case 'b':
			bitmap = 1;
			break;

		case 'c':
			color = strtoul(optarg, NULL, 16);
			break;

		case 'P':
			points = true;
			break;

		case 'w':
			white = 1;
			break;

		case 'o':
			outfile = optarg;
			break;

		case '1':
			single_polygons = true;
			break;

		default:
			usage(argv);
			exit(EXIT_FAILURE);
		}
	}

	if (outfile == NULL) {
		fprintf(stderr, "%s: must specify -o output.mbtiles\n", argv[0]);
		usage(argv);
		exit(EXIT_FAILURE);
	}

	if (optind >= argc) {
		fprintf(stderr, "%s: must specify -o input.mbtiles or input.count\n", argv[0]);
		usage(argv);
		exit(EXIT_FAILURE);
	}

	if (!(include_count || include_density)) {
		include_density = true;
	}

	if (force) {
		unlink(outfile);
	}
	sqlite3 *outdb = mbtiles_open(outfile, argv, false);

	double minlat = 90, minlon = 180, maxlat = -90, maxlon = -180, midlat = 0, midlon = 0;
	std::vector<long long> zoom_max;
	size_t zooms = 0;

	std::vector<std::map<std::string, layermap_entry>> layermaps;
	for (size_t cpu = 0; cpu < cpus; cpu++) {
		layermap_entry lme(0);
		lme.minzoom = 0;
		lme.maxzoom = zooms - 1;

		std::map<std::string, layermap_entry> m;
		m.insert(std::pair<std::string, layermap_entry>(layername, lme));

		layermaps.push_back(m);
	}

	// Try opening input as mbtiles for retiling.
	// If that doesn't work, it must be new counts.

	sqlite3 *firstdb;
	if (sqlite3_open(argv[optind], &firstdb) == SQLITE_OK) {
		sqlite3_stmt *stmt;
		if (sqlite3_prepare_v2(firstdb, "SELECT value from metadata where name = 'maxzoom'", -1, &stmt, NULL) == SQLITE_OK) {
			if (sqlite3_step(stmt) == SQLITE_ROW) {
				zooms = sqlite3_column_int(stmt, 0) + 1;
			}
			sqlite3_finalize(stmt);
		}
		if (sqlite3_close(firstdb) != SQLITE_OK) {
			fprintf(stderr, "Could not close database: %s\n", sqlite3_errmsg(firstdb));
			exit(EXIT_FAILURE);
		}
	}

	if (zooms == 0) {
		if (optind + 1 != argc || (maxzoom < 0 && bin < 0) || outfile == NULL) {
			usage(argv);
		}

		if (maxzoom < 0 && bin < 0) {
			fprintf(stderr, "%s: Must specify either maxzoom (-z) or bin size (-s)\n", argv[0]);
			exit(EXIT_FAILURE);
		}

		if (maxzoom >= 0) {
			zooms = maxzoom + 1;
		} else {
			if (bin < (signed) (detail + 1)) {
				fprintf(stderr, "%s: Detail (%zu) too low for bin size (%d)\n", argv[0], detail, bin);
				exit(EXIT_FAILURE);
			}

			zooms = bin - detail + 1;
		}

		struct stat st;
		if (stat(argv[optind], &st) != 0) {
			perror(optind[argv]);
			exit(EXIT_FAILURE);
		}

		FILE *fps[cpus];
		for (size_t j = 0; j < cpus; j++) {
			fps[j] = fopen(argv[optind], "rb");
			if (fps[j] == NULL) {
				perror(argv[optind]);
				exit(EXIT_FAILURE);
			}
		}

		char buf[HEADER_LEN];
		if (fread(buf, HEADER_LEN, 1, fps[0]) != 1) {
			perror("fread file header");
			exit(EXIT_FAILURE);
		}
		if (memcmp(buf, header_text, HEADER_LEN) != 0) {
			fprintf(stderr, "%s: not a tile-count file\n", argv[optind]);
			exit(EXIT_FAILURE);
		}

		for (size_t pass = 0; pass < 2; pass++) {
			volatile int progress[cpus];
			std::vector<tiler> tilers;
			tilers.resize(cpus);

			for (size_t j = 0; j < cpus; j++) {
				for (size_t z = 0; z < zooms; z++) {
					tilers[j].tiles.push_back(tile(detail, z));
					tilers[j].max.push_back(0);
				}
				tilers[j].bbox[0] = tilers[j].bbox[1] = UINT_MAX;
				tilers[j].bbox[2] = tilers[j].bbox[3] = 0;
				tilers[j].midx = tilers[j].midy = 0;
				tilers[j].zooms = zooms;
				tilers[j].minzoom = minzoom;
				tilers[j].detail = detail;
				tilers[j].outdb = outdb;
				tilers[j].progress = progress;
				tilers[j].progress[j] = 0;
				tilers[j].cpus = cpus;
				tilers[j].shard = j;
				tilers[j].maxzoom = zooms - 1;
				tilers[j].pass = pass;
				tilers[j].zoom_max = zoom_max;
				tilers[j].layername = layername;
				tilers[j].layermap = &layermaps[j];
			}

			size_t records = (st.st_size - HEADER_LEN) / RECORD_BYTES;
			for (size_t j = 0; j < cpus; j++) {
				tilers[j].fp = fps[j];
				tilers[j].start = j * records / cpus;
				if (j > 0) {
					tilers[j - 1].end = tilers[j].start;
				}
			}
			tilers[cpus - 1].end = records;

			pthread_t pthreads[cpus];
			for (size_t j = 0; j < cpus; j++) {
				if (pthread_create(&pthreads[j], NULL, run_tile, &tilers[j]) != 0) {
					perror("pthread_create");
					exit(EXIT_FAILURE);
				}
			}

			for (size_t j = 0; j < cpus; j++) {
				void *retval;

				if (pthread_join(pthreads[j], &retval) != 0) {
					perror("pthread_join");
					exit(EXIT_FAILURE);
				}
			}

			// Collect and consolidate partially counted tiles

			std::map<std::vector<unsigned>, tile> partials;
			for (size_t j = 0; j < cpus; j++) {
				for (size_t k = 0; k < tilers[j].partial_tiles.size(); k++) {
					std::vector<unsigned> key;
					key.push_back(tilers[j].partial_tiles[k].z);
					key.push_back(tilers[j].partial_tiles[k].x);
					key.push_back(tilers[j].partial_tiles[k].y);

					auto a = partials.find(key);

					if (a == partials.end()) {
						partials.insert(std::pair<std::vector<unsigned>, tile>(key, tilers[j].partial_tiles[k]));
					} else {
						for (size_t x = 0; x < (1U << detail) * (1U << detail); x++) {
							a->second.count[x] += tilers[j].partial_tiles[k].count[x];
						}
					}
				}
			}

			for (auto a = partials.begin(); a != partials.end(); a++) {
				if (pass == 0) {
					gather_quantile(a->second, detail, tilers[0].max[a->second.z]);
				} else {
					make_tile(outdb, a->second, a->second.z, detail, zoom_max[a->second.z], layername, &layermaps[0]);
				}
			}

			if (pass == 0) {
				for (size_t z = 0; z < zooms; z++) {
					long long max = 0;

					for (size_t c = 0; c < tilers.size(); c++) {
						if (tilers[c].max[z] / 2 > max) {
							max = tilers[c].max[z] / 2;
						}
					}

					zoom_max.push_back(max);
				}

				regress(zoom_max, minzoom);
			} else {
				long long file_bbox[4] = {UINT_MAX, UINT_MAX, 0, 0};
				for (size_t j = 0; j < cpus; j++) {
					if (tilers[j].bbox[0] < file_bbox[0]) {
						file_bbox[0] = tilers[j].bbox[0];
					}
					if (tilers[j].bbox[1] < file_bbox[1]) {
						file_bbox[1] = tilers[j].bbox[1];
					}
					if (tilers[j].bbox[2] > file_bbox[2]) {
						file_bbox[2] = tilers[j].bbox[2];
					}
					if (tilers[j].bbox[3] > file_bbox[3]) {
						file_bbox[3] = tilers[j].bbox[3];
					}
				}

				long long max = 0;
				for (size_t j = 0; j < cpus; j++) {
					if (tilers[j].atmid > max) {
						max = tilers[j].atmid;
						tile2lonlat(tilers[j].midx, tilers[j].midy, 32, &midlon, &midlat);
					}
				}

				tile2lonlat(file_bbox[0], file_bbox[1], 32, &minlon, &maxlat);
				tile2lonlat(file_bbox[2], file_bbox[3], 32, &maxlon, &minlat);
			}
		}
	} else {
		fprintf(stderr, "going to merge %zu zoom levels\n", zooms);
		merge_tiles(argv + optind, argc - optind, cpus, outdb, zooms, zoom_max, midlat, midlon, minlat, minlon, maxlat, maxlon, layername, layermaps);
	}

	std::map<std::string, layermap_entry> lm = merge_layermaps(layermaps);

	mbtiles_write_metadata(outdb, NULL, outfile, 0, zooms - 1, minlat, minlon, maxlat, maxlon, midlat, midlon, false, "", lm, !bitmap, outfile);

	write_meta(zoom_max, outdb);

	mbtiles_close(outdb, argv[0]);
}
