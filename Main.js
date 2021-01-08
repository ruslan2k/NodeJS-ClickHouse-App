const { ClickHouse } = require('clickhouse');

const clickhouse = new ClickHouse({
	url: 'http://localhost',
	port: 8123,
	debug: false,
    basicAuth: {
        username: 'default',
        password: 'hiclick10',
    },
	isUseGzip: false,
	format: "json", // "json" || "csv" || "tsv"
	config: {
		session_timeout                         : 60,
		output_format_json_quote_64bit_integers : 0,
		enable_http_compression                 : 0,
		database                                : 'WellData',
	}
});




var string = '';

clickhouse.query('INSERT INTO WellData.codec (* EXCEPT(val)) VALUES (1, 1)').stream()
	.on('data', function(data) {
		const stream = this;

		stream.pause();

		setTimeout(() => {
			stream.resume();
		}, 1000);
	})
	.on('error', err => {
        console.log("err");
	})
	.on('end', () => {

	});