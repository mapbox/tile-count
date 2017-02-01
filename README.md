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

    tile-count-merge -o out.count in1.count [in2.count ...]

Produces a new count file from the specified count files, summing the counts for any points
duplicated between the two.

Decoding counts
---------------

    tile-count-decode in.count ...

Outputs the `lon,lat,count` CSV that would recreate `in.count`.

Tiling
------

    tile-count-tile [-fs] -o out.mbtiles -z zoom in.count

The _zoom_ is the size of the bins, not the maximum zoom of the tileset,
so for instance a `-z24` run would produce tiles of zooms 0 through 15.

The features in the `mbtiles` are a grid of points with a `count` attribute
indicating how many original points were accumulated into that binned point.
The `-s` option generates square polygon bins instead of points.

The `-f` option deletes any existing file named `out.mbtiles`.

Internal file format
--------------------

The `.count` files contain a header for versioning and identification
followed (currently) by a simple list of 12-byte records containing:

   * 64-bit location quadkey
   * 32-bit count
