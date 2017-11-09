#!/usr/bin/env node

'use strict';

var fs = require('fs');

var data = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
var min = process.argv[3];

function check(data) {
	if (data.type === 'FeatureCollection') {
		var i;
		for (i = 0; i < data.features.length; i++) {
			check(data.features[i]);
		}
	} else if (data.type === 'Feature') {
		if (data.properties.count < min) {
			console.error("Found " + data.properties.count + " < " + min + " in " + process.argv[2]);
			console.error(JSON.stringify(data));
			process.exit(1);
		}
	}
}

check(data);
