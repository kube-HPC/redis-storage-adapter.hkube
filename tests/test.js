
const { expect } = require('chai');
const redis = require('../index');
const uuidv4 = require('uuid/v4');
const moment = require('moment');
const path = require('path');
const DateFormat = 'YYYY-MM-DD';

describe('redis', () => {
    beforeEach(() => {
        redis.init({ host: 'localhost', port: 6379 });
    });
    describe('get/set', () => {
        it('should set a key and then get the same key', async () => {
            const jobId = uuidv4();
            const taskId = uuidv4();
            const data = { data: 'yes' };
            const res = await redis.put({ path: path.join('hkube', moment().format(DateFormat), jobId, taskId), data });
            const result = await redis.get(res);
            expect(result).to.deep.equal(data);
        });
        it('should set a key and then get the same key - result', async () => {
            const jobId = uuidv4();
            const data = { data: 'yes' };
            const res = await redis.put({ path: path.join('hkube-results', moment().format(DateFormat), jobId), data });
            const result = await redis.get(res);
            expect(result).to.deep.equal(data);
        });
    });
});
