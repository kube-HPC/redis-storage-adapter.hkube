
const Factory = require('@hkube/redis-utils').Factory;
const MODULE_NAME = '@hkube/redis-adapter';
const pathLib = require('path');
let client;

class RedisAdapter {
    constructor() {
        this._isInit = false;
    }

    async init(options, log, prefixes, bootstrap = false) {
        if (!this._isInit) {
            client = Factory.getClient(options);
            this._isInit = true;
        }
    }

    async put(options) {
        return this._set({ ...options, path: pathLib.join('/', options.Path), data: options.Data });
    }

    async putResults(options) {
        return this._set({ path: pathLib.join('/', pathLib.join('/', options.Path)), data: options.Data });
    }


    async list(options) {
        return new Promise((resolve, reject) => {
            let keys = [];
            const stream = client.scanStream({ match: pathLib.join('/', options.Path, '*') });
            stream.on('data', (data) => {
                keys = keys.concat(data);
            });
            stream.on('end', () => {
                return resolve(keys.map(k => ({ Path: k })));
            });
            stream.on('error', () => {
                return reject();
            });
        });
    }

    async delete(options) {
        return client.del(options.Path);
    }

    _set(options) {
        return new Promise((resolve, reject) => {
            const { data, path } = options;
            client.set(path, JSON.stringify(data), (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve({ path, moduleName: MODULE_NAME });
            });
        });
    }

    async get(options) {
        return this._get(options);
    }

    _get(options) {
        return new Promise((resolve, reject) => {
            const { path } = options;
            client.get(path, (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve(this._tryParseJSON(res));
            });
        });
    }

    _tryParseJSON(json) {
        let parsed = json;
        try {
            parsed = JSON.parse(json);
        }
        catch (e) { }
        return parsed;
    }
}

module.exports = new RedisAdapter();
