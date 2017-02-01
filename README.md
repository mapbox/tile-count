tile-count
==========

A tool for accumulating point counts by tile.

Creating a count
----------------

    tile-count-create [-z zoom] -o out.count [file.csv ...]

The `-z` option specifies the maximum precision of the data, so that duplicates
beyond this precision can be pre-summed to make the data file smaller.

The input CSV is a list of records in the form:

    lon,lat

or

    lon,lat,count

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

    tile-count-tile -o out.mbtiles -z zoom in.count

The _zoom_ is the size of the bins, not the maximum zoom of the tileset,
so for instance a `-z24` run would produce tiles of zooms 0 through 17.
The features in the `mbtiles` are a grid of points with a `count` attribute
indicating how many original points were accumulated into that binned point.

Internal file format
--------------------

The `.count` files contain a header for versioning and identification
followed (currently) by a simple list of 128-bit records containing:

   * 64-bit location quadkey
   * 64-bit count
