tile-bin
========

A tool for accumulating point counts by tile.

Creating a bin
--------------

    tile-bin-create -z zoom -o out.bin [file.csv]

The `-z` option specifies the maximum precision of the data, so that duplicates
can be pre-summed to make the data file smaller.

The input CSV is a list of records in the form:

    lon,lat

or

    lon,lat,count

The input is first streamed into the internal format specified below and then
sorted and merged into the same format in quadkey order, with adjacent duplicates
summed.

Merging bins
------------

    tile-bin-merge -o out.bin in1.bin [in2.bin ...]

Produces a new bin from the specified bins, summing the counts for any points
duplicated between the two.

Decoding bins
-------------

    tile-bin-decode in.bin

Outputs the `lon,lat,count` CSV that would recreate `in.bin`.

Tiling
------

    tile-bin-tile -o out.mbtiles -z zoom in.bin

The _zoom_ is the size of the bins, not the maximum zoom of the tileset,
so for instance a `-z24` run would produce tiles of zooms 0 through 16.
The features in the `mbtiles` are a grid of points with a `count` attribute
indicating how many original points were accumulated into that binned point.

Internal file format
--------------------

The `.bin` files contain a magic number for versioning and identification
followed (currently) by a simple list of 128-bit records containing:

   * 64-bit location quadkey
   * 64-bit count
