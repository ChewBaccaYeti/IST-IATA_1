const _ = require('lodash');
const { NotFound } = require('http-errors');
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
const redis_key = 'airports:istanbul';

const redis = require('redis')
    .createClient({ socket: { host: 'redis://127.0.0.1', port: 6379 } })
    .on('connect', () => {
        console.log(`${day} [redis] connected.`)
    })
    .on('reconnecting', () => {
        console.log(`${day} [redis] reconnected.`)
    })
    .on('error', (e) => {
        console.error(`${day} [redis] error.:`, e)
    });

redis.del(redis_key, (e, r) => {
    if (e) {
        console.error(`${day} [redis] deletion error:`, e)
    } else {
        console.log(`[${day}][redis] data deleted.`)
    }
});

const app = express();
app.get('/schedules', (req, res) => {
    redis.get(redis_key, (err, res) => {
        const data = JSON.parse(res);
        if (!data) {
            throw new NotFound(err, 'Data not found.')
        };
        try {
            response.json({
                message: 'success',
                status: 200,
                data: {
                    data
                }
            });
        } catch (e) {
            response.status(500).json({ e: 'Server error.' })
        }
    })
}).listen(port, () => { });

const redisSet = (redis_key, flights, cb) => {
    redis.set(redis_key, JSON.stringify(flights), (e) => {
        if (e) {
            return cb && cb(e);
        } else { }
        cb && cb();
    })
};

const killSignal = () => {
    console.log('Kill signal.')
    redis && redis.end(true);
    setTimeout(() => {
        console.error('Forcing kill signal.')
        return process.exit(1);
    }, 7500);
};
process.once('SIGTERM', killSignal);
process.once('SIGINT', killSignal);

function dataFlights() {
    const body_size = 10;
    const page_size = 50;
    const retries = 3;
    const r = {
        url: base_url,
        proxy: proxy,
        headers: headers,
    };
    const d = [
        `date=${dates[0]}`,
        `endDate=${dates[1]}`,
    ];

    let flights = [];
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
                                r,
                                formData: {
                                    pageNumber: page,
                                    pageSize: page_size,
                                    '': d,
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
                                    const data = _.get(obj, 'result.data.flights', []);
                                    if (
                                        !obj.result ||
                                        !obj.result.data
                                    ) {
                                        console.dir(obj);
                                        console.log('body:', body);
                                        return retry_done(true);
                                    }
                                    const flightsArray = data;

                                } catch (e) { }
                            })
                        retry_done()
                    })
                    until_done()
                })
                next_type()
            })
            next_status()
        })
        next_date()
    })
};