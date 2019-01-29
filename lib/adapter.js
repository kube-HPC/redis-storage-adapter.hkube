
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
        return this._set({ ...options, path: pathLib.join('/', options.path), data: options.data });
    }

    async list(options) {
        return new Promise((resolve, reject) => {
            let keys = [];
            const stream = client.scanStream({ match: pathLib.join('/', options.path + '*') });
            stream.on('data', (data) => {
                keys = keys.concat(data);
            });
            stream.on('end', () => {
                return resolve(keys.map(path => ({ path })));
            });
            stream.on('error', () => {
                return reject();
            });
        });
    }

    async listPrefixes(options) {
        const res = await this.list(options);
        return res.map(r => r.path.match(options.path + '/(.*)/')[1]);
    }

    async delete(options) {
        let { path } = options;
        if (!path.startsWith('/')) {
            path = pathLib.join('/', path);
        }
        const jobIds = await this.list(options);
        const keysToDelete = jobIds.map(x => ['del', x.path]);
        return new Promise((resolve, reject) => {
            client.multi(keysToDelete).exec().then(data => resolve(data), error => reject(error));
        });
    }

    _set(options) {
        return new Promise((resolve, reject) => {
            const { data, path } = options;
            client.set(path, JSON.stringify(data), (err, res) => {
                if (err) {
                    return reject(err);
                }
                return resolve({ path });
            });
        });
    }

    async get(options) {
        let { path } = options;
        if (!path.startsWith('/')) {
            path = pathLib.join('/', path);
        }
        const res = await this._get({ path });
        if (!res) {
            return { error: new Error('failed to get from storage') };
        }
        return res;
    }

    _get(options) {
        return new Promise((resolve, reject) => {
            client.get(options.path, (err, res) => {
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
