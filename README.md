tile-count
==========

A tool for accumulating point counts by tile.

Creating a count
----------------

    tile-count-create [-z zoom] -o out.count [file.csv ...] [file.json ...]

The `-z` option specifies the maximum precision of the data, so that duplicates
beyond this precision can be pre-summed to make the data file smaller.

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

    tile-count-merge -o out.count [-z zoom] in1.count [in2.count ...]

Produces a new count file from the specified count files, summing the counts for any points
duplicated between the two.

If you specify a `zoom`, the precision of all locations in the output file will be reduced
as specified.

Decoding counts
---------------

    tile-count-decode in.count ...

Outputs the `lon,lat,count` CSV that would recreate `in.count`.

Tiling
------

    tile-count-tile [options] -o out.mbtiles -z zoom in.count

The _zoom_ is the size of the bins, not the maximum zoom of the tileset,
so for instance a `-z24` run would produce tiles of zooms 0 through 15.

The maxzoom of the output tileset is _zoom_ minus _detail_.
The default _detail_ is 9 if you don't specify one.

The features in the `mbtiles` are a grid of squares with a `density` attribute
indicating how many original points were accumulated into that binned point,
normalized according the densest point in the zoom level.

* `-f`: Delete any existing file named `out.mbtiles`.
* `-z` *zoom*: Use buckets the size of a tile in zoom level *zoom*.
* `-d` *detail*: Make the grid within each tile 2^detail points on each side. The default is 9.
* `-l` *levels*: Quantize the normalized counts within each tile into the specified number of levels. The default is 50.
* `-m` *level*: Don't include normalized counts that are quantized below the specified level. The default is 6. In bitmap tiles, all levels are included.
* `-g` *gamma*: Scale the counts within each tile to the gamma'th root of their linear value. The default is 2.5.

* `-b`: Create PNG raster tiles instead of vectors. If you are not planning to use these tiles with Mapbox GL,
        You will probably also want to specify `-d8` for normal 256x256 web map tile resolution.
* `-c` *rrggbb*: Specify the color to use in raster tiles as a hex color.
* `-w`: Make tiles for a white background instead of a black background.

Internal file format
--------------------

The `.count` files contain a header for versioning and identification
followed (currently) by a simple list of 12-byte records containing:

   * 64-bit location quadkey
   * 32-bit count
