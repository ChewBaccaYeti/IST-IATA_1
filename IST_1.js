const _ = require('lodash');
const async = require('async');
const express = require('express');
const moment = require('moment-timezone');
const request = require('request');

const port = 11289;
const base_url = 'https://www.istairport.com/umbraco/api/FlightInfo/GetFlightStatusBoard';
const headers = {
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Referer': 'https://www.istairport.com/en/flights/flight-info/departure-flights/?locale=en',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
};
const proxy = 'http://speej9xhkw:KbdCbB22xmdmxpG28k@dc.smartproxy.com:10000';
const frmt = 'YYYY-MM-DD HH:mm:ss';
const tmzn = 'Europe/Istanbul';
const day = moment().tz(tmzn);
const dates = [day.format(frmt), day.clone().add(1, 'd').format(frmt)];
const redis_url = 'redis://127.0.0.1:6379';
const redis_key = 'airports:istanbul_1';

const redis = require('redis')
    .createClient({ url: redis_url, })
    .on('connect', () => {
        console.log(`[${day}] [redis] connected.`);
        dataFlights();
    })
    .on('reconnecting', (p) => {
        console.log(`[${day}] [redis] reconnected.`, p);
    })
    .on('error', (err) => {
        console.error(`[${day}] [redis] error:`, err);
    });

redis.del(redis_key, (err, r) => {
    if (err) {
        console.error(`[${day}] [redis] deletion error:`, err);
    } else {
        console.log(`[${day}][redis] data deleted.`);
    }
});

const app = express();
app.get('/schedules', (req, res) => {
    redis.get(redis_key, (err, reply) => {
        const data = JSON.parse(reply);
        if (!reply) {
            return res.status(404).json({ error: 'Data not found.' });
        };
        try {
            res.json({
                message: 'success',
                status: 200,
                data: {
                    result: data
                }
            });
        } catch (err) {
            res.status(500).json({ err: 'Server error.' });
        }
    })
}).listen(port, () => { console.log(`Server started on port ${port}`); });

const redisSet = (redis_key, ist_flights, cb) => {
    redis.set(redis_key, JSON.stringify(ist_flights), (err) => {
        if (err) {
            console.error(`[${day}][redis] set error: %j`, err);
            return cb && cb(err);
        } else {
            console.log(`[${day}][redis] data set.`);
        }
        cb && cb();
    })
};

const killSignal = () => {
    console.log('Kill signal.')
    redis && redis.end && redis.end(true);
    setTimeout(() => {
        console.error('Forcing kill signal.');
        return process.exit(1);
    }, 7500);
};
process.once('SIGTERM', killSignal);
process.once('SIGINT', killSignal);

function dataFlights() {
    const body_size = 10;
    const page_size = 50;
    const retries = 5;

    let ist_flights = [];
    let done = false;
    let page = 1;
    let tries = 0;

    async.eachLimit(dates, 20, (date, next_date) => {
        async.each([1, 0], (status, next_status) => {
            async.each([0, 1], (type, next_type) => {
                async.until((test) => {
                    test(null, done)
                }, (until_done) => {
                    async.retry(retries, (retry_done) => {
                        if (tries) console.log(`[Retrying#${tries}] [${base_url}]`);
                        tries++;

                        request.post(
                            {
                                url: base_url,
                                proxy: proxy,
                                headers: headers,
                                formData: {
                                    pageNumber: page,
                                    pageSize: page_size,
                                    '': [
                                        `date=${dates[0]}`,
                                        `endDate=${dates[1]}`,
                                    ],
                                    flightNature: status,
                                    isInternational: type,
                                    searchTerm: 'changeflight',
                                    culture: 'en',
                                    prevFlightPage: '0',
                                    clickedButton: 'moreFlight',
                                },
                            }, (err, res, body) => {
                                if (err || !body || body.length < body_size) {
                                    return retry_done(true);
                                } page++;
                                try {
                                    const obj = JSON.parse(body);
                                    if (
                                        !obj.result ||
                                        !obj.result.data
                                    ) {
                                        return retry_done(true);
                                    }
                                    const flights_array = _.get(obj, 'result.data.flights', []);
                                    const flights_fields = _.flatMap(flights_array, (flight) => {
                                        const info_fields = {
                                            'airline_iata': status === 0 ? flight.airlineCode : status === 1 ? flight.airlineCode : null,
                                            'flight_iata': status === 0 ? flight.flightNumber : status === 1 ? flight.flightNumber : null,
                                            'flight_number': status === 0 ? flight.flightNumber.slice(2) : status === 1 ? flight.flightNumber.slice(2) : null,
                                            'status': flight.remark ? flight.remark.toLowerCase() : null,
                                            'duration':
                                                moment(status === 0 ? moment.tz(tmzn).utc(flight.scheduledDatetime).format(frmt) : null,)
                                                    .diff(moment(status === 1 ? moment.tz(tmzn).utc(flight.estimatedDatetime).format(frmt) : null,)) || null,
                                            'delayed':
                                                status === 0 ? (Math.abs(moment(flight.scheduledDatetime, frmt).diff(moment(flight.estimatedDatetime, frmt), 'minutes')) || null) : null ||
                                                    status === 1 ? (Math.abs(moment(flight.scheduledDatetime, frmt).diff(moment(flight.estimatedDatetime, frmt), 'minutes')) || null) : null,
                                        };
                                        const codeshare_fields = {
                                            'cs_airline_iata': null,
                                            'cs_flight_iata': null,
                                            'cs_flight_number': null,
                                        };
                                        const arrival_fields = {
                                            'arr_baggage': flight.carousel || null,
                                            'arr_delayed': status === 0 ? (Math.abs(moment(flight.scheduledDatetime, frmt).diff(moment(flight.estimatedDatetime, frmt), 'minutes')) || null) : null,
                                            'arr_gate': status === 0 ? flight.gate : null,
                                            'arr_iata': flight.toCityCode || null,
                                            'arr_terminal': status === 0 ? flight.gate.charAt(0) : null,
                                            'arr_time': status === 0 ? moment(flight.scheduledDatetime).format(frmt) : null,
                                            'arr_time_ts': status === 0 ? moment(flight.scheduledDatetime).tz(tmzn).unix() : null,
                                            'arr_time_utc': status === 0 ? moment.tz(tmzn).utc(flight.scheduledDatetime).format(frmt) : null,
                                        };
                                        const departure_fields = {
                                            'dep_checkin': status === 1 ? flight.counter : null,
                                            'dep_delayed': status === 1 ? (Math.abs(moment(flight.scheduledDatetime, frmt).diff(moment(flight.estimatedDatetime, frmt), 'minutes')) || null) : null,
                                            'dep_gate': status === 1 ? flight.gate : null,
                                            'dep_iata': flight.fromCityCode || null,
                                            'dep_terminal': status === 1 ? flight.gate.charAt(0) : null,
                                            'dep_time': status === 1 ? moment(flight.scheduledDatetime).format(frmt) : null,
                                            'dep_time_ts': status === 1 ? moment(flight.scheduledDatetime).tz(tmzn).unix() : null,
                                            'dep_time_utc': status === 1 ? moment.tz(tmzn).utc(flight.scheduledDatetime).format(frmt) : null,
                                        };
                                        const spread_fields = {
                                            ...info_fields,
                                            ...codeshare_fields,
                                            ...arrival_fields,
                                            ...departure_fields,
                                        };
                                        if (status === 1 ? flight : status === 0 ? flight : null) {
                                            ist_flights.push(spread_fields);
                                            if (flight.codeshare && flight.codeshare.length > 0) {
                                                ist_flights.push(...flight.codeshare.map((code) => ({
                                                    ...spread_fields,
                                                    'cs_airline_iata': spread_fields.airline_iata || null,
                                                    'cs_flight_number': spread_fields.flight_number || null,
                                                    'cs_flight_iata': spread_fields.flight_iata || null,
                                                    'airline_iata': status === 0 ? code.slice(0, 2) : status === 1 ? code.slice(0, 2) : null,
                                                    'flight_iata': status === 0 ? code : status === 1 ? code : null,
                                                    'flight_number': status === 0 ? code.slice(2, 6) : status === 1 ? code.slice(2, 6) : null,
                                                })))
                                            }
                                            return spread_fields;
                                        }
                                    });

                                    function unique_flights(arr, key) {
                                        const duplicates = [];
                                        const unique = [];
                                        for (let i = 0; i < arr.length; i++) {
                                            let is_duplicate = false;
                                            for (let j = i + 1; j < arr.length; j++) {
                                                if (arr[i][key] === arr[j][key]) {
                                                    duplicates.push({ duplicate1: arr[i], duplicate2: arr[j] });
                                                    is_duplicate = true;
                                                    break;
                                                }
                                            }
                                            if (!is_duplicate) {
                                                unique.push(arr[i]);
                                            }
                                        }
                                        return { duplicates, unique };
                                    }
                                    const { duplicates, unique } = unique_flights(ist_flights, 'flight_iata');
                                    if (duplicates.length > 0) {
                                        ist_flights = unique;
                                    } else if (flights_fields.length >= page_size) {
                                        finished = false;
                                    } else {
                                        finished = true;
                                    } retry_done();
                                    return ist_flights;

                                } catch (err) {
                                    console.error(`[error] [${err}]`);
                                    return retry_done(true);
                                }

                            }, retry_done);
                    }, until_done);
                }, () => {
                    redisSet(redis_key, ist_flights, (err) => {
                        if (err) {
                            console.error('Error saving data:', err);
                        } else {
                            console.log(`[${day}][redis] data saved.`);
                        }
                    });
                }, next_type());
            }, next_status());
        }, next_date());
    });
};