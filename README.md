tile-count
==========

A tool for accumulating point counts by tile.

Installation
------------

Tile-count requires sqlite3 and libpng.

```
$ sudo apt-get install libsqlite3-dev libpng-dev
$ make install
```

Creating a count
----------------

    tile-count-create [-q] [-s binsize] -o out.count [file.csv ...] [file.json ...]

* The `-s` option specifies the maximum precision of the data, so that duplicates
beyond this precision can be pre-summed to make the data file smaller.
* The `-q` option silences the progress indicator.

If the input is CSV, it is a list of records in the form:

    lon,lat

or

    lon,lat,count

If it is JSON, any array of two or more numbers will be treated as a longitude-latitude
pair. This includes GeoJSON Points as well as the points that make up GeoJSON MultiPoints,
LineStrings, MultiLineStrings, Polygons, and MultiPolygons. Beware that it also includes
anything else that might be mistaken for a longitude-latitude pair.

The input is first streamed into the internal format specified below (minus the header)
and then
sorted and merged into the same format in quadkey order, with adjacent duplicates
summed.

Merging counts
--------------

    tile-count-merge [-q] [-s binsize] -o out.count in1.count [in2.count ...]

Produces a new count file from the specified count files, summing the counts for any points
duplicated between the two.

* `-s` *binsize*: The precision of all locations in the output file will be reduced as specified.
* `-q`: Silence the progress indicator

Decoding counts
---------------

    tile-count-decode in.count ...

Outputs the `lon,lat,count` CSV that would recreate `in.count`.

Tiling
------

    tile-count-tile [options] -o out.mbtiles in.count
    tile-count-tile [options] -o out.mbtiles in.mbtiles [ … in.mbtiles ]

The features in the `mbtiles` are a grid of squares with a `density` attribute
indicating how many original points were accumulated into that binned point,
normalized according the densest point in the zoom level.

If you are merging existing `.mbtiles` files, they all have to have been created
with the same minzoom, maxzoom, and detail. The merged output can be either
bitmap or vector as desired. The `.mbtiles` files being merged should be separated
spatially, not temporally, because merging does not recalculate the reference
brightness where tilesets overlap. Use `tile-count-merge` to combine data sets
that are for the same area.

### Output tileset

* `-n` *layername*: Specify the layer name in vector tile output. The default is `count`.
* `-o` *out.mbtiles*: Specify the name of the output file.
* `-f`: Delete the output file if it already exists

### Zoom levels

* `-d` *detail*: Make the grid within each tile 2^detail points on each side. The default is 9.
* `-Z` *minzoom*: Specify the minzoom of the tileset. The default is 0.
* `-z` *maxzoom*: Specify the maxzoom of the tileset.
* `-s` *binsize*: Specify the zoom level whose tiles are used as bins.

You must specify either `-z` (maxzoom) or `-s` (bin size) if you are creating a new tileset
instead of merging existing tilesets. The *maxzoom* plus the *detail* always equals the *bin size*.

### Level bucketing

* `-l` *levels*: Quantize the normalized counts within each tile into the specified number of levels. The default is 50.
* `-m` *level*: Don't include normalized counts that are quantized below the specified level.
* `-M` *count*: don't include absolute counts that are below the specified count.
* `-g` *gamma*: Scale the counts within each tile to the gamma'th root of their linear value. The default is 2.5.
* `-y density`: Include an attribute in each vector feature indicating the normalized density of points within each bin. This is the default.
* `-y count`: Include an attribute in each vector feature indicating the count of points within each bin. The count is only approximate because the levels are bucketed.

### Bitmap tiles

* `-b`: Create PNG raster tiles instead of vectors. If you are not planning to use these tiles with Mapbox GL,
        you will probably also want to specify `-d8` for normal 256x256 web map tile resolution.
* `-c` *rrggbb*: Specify the color to use in raster tiles as a hex color.
* `-w`: Make tiles for a white background instead of a black background.

### Vector tiles

* `-1`: Output an individual polygon for each bin instead of combining them into MultiPolygons.

### Tile size

* `-k`: Don't enforce the 500K limit on tile size
* `-K`: Raise the minimum count threshold on each tile if necessary to keep it under 500K.

### Miscellaneous controls

* `-p` *cpus*: Use the specified number of parallel tasks.
* `-q`: Silence the progress indicator

Relationship between bin size, maxzoom, and detail
--------------------------------------------------

What exactly the "detail" parameter means is often the source of confusion.
It is the difference between the maxzoom and the bin size.

So, for example, if you have data with a bin size of 23 and want to
tile it with a maxzoom of 16, you should specify a detail of 7, because
16+7=23.

Within each tile, the resolution of the tile is 2^detail, so if you
specify a detail of 7, each tile will be a 128x128 grid of pixels or
features, because 2^7=128.

It is often more useful to work backward from the bin size
to the maxzoom: if you have data with a bin size of 24, and you
want 256x256 tiles, 2^8=256 so you should specify a detail of 8,
and the maxzoom will be 16 because 24-8=16.

Internal file format
--------------------

The `.count` files contain a header for versioning and identification
followed (currently) by a simple list of 12-byte records containing:

   * 64-bit location quadkey
   * 32-bit count
