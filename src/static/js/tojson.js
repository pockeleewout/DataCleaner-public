function deleteColumn(column_id) {
   	var form = {};
	form.column_id = column_id;
	var json = JSON.stringify(form);
	return json;
}

function equiDistance(column_id, bins) {
   	var form = {};
	form.column_id = column_id;
	form.bins = bins;
	var json = JSON.stringify(form);
	return json;
}

function equiFrequency(column_id, bins) {
   	var form = {};
	form.column_id = column_id;
	form.bins = bins;
	var json = JSON.stringify(form);
	return json;
}

function extractDatetime(column_id) {
   	var form = {};
	form.column_id = column_id;
	var json = JSON.stringify(form);
	return json;
}

function findReplace(column_id, from, to) {
   	var form = {};
	form.column_id = column_id;
	form.from = from;
	form.to = to;
	var json = JSON.stringify(form);
	return json;
}

function normalise(column_id) {
   	var form = {};
	form.column_id = column_id;
	var json = JSON.stringify(form);
	return json;
}

function oneHotEncoding(column_id) {
   	var form = {};
	form.column_id = column_id;
	var json = JSON.stringify(form);
	return json;
}

function parseDateTime(column_id) {
   	var form = {};
	form.column_id = column_id;
	var json = JSON.stringify(form);
	return json;
}

function removeOutliers(column_id, range) {
   	var form = {};
	form.column_id = column_id;
	form.range = range;
	var json = JSON.stringify(form);
	return json;
}


