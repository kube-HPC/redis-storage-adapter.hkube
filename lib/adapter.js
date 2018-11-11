
const Factory = require('@hkube/redis-utils').Factory;
const MODULE_NAME = '@hkube/redis-adapter';
const pathLib = require('path');
let client;

class RedisAdapter {
    constructor() {
        this._isInit = false;
    }

    async init(options) {
        if (!this._isInit) {
            client = Factory.getClient(options);
            this._isInit = true;
        }
    }

    async put(options) {
        return this._set({ ...options, path: pathLib.join('/', 'hkube', options.jobId, options.taskId), data: options.data });
    }

    async putResults(options) {
        return this._set({ path: pathLib.join('/', 'hkube-results', options.jobId, 'result.json'), data: options.data });
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

    async jobPath(options) {
        return true;
    }

    async getStream(options) {
        return this.get(options);
    }

    _tryParseJSON(json) {
        let parsed = json;
        try {
            parsed = JSON.parse(json);
        }
        catch (e) {
        }
        return parsed;
    }
}

module.exports = new RedisAdapter();
