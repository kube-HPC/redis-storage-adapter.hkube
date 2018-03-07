
const { expect } = require('chai');
const redis = require('../index');
const sinon = require('sinon');
const uuidv4 = require('uuid/v4');
let etcd;

describe('etcd', () => {
    beforeEach(() => {
        redis.init({ host: 'localhost', port: 6379 });
    });
    describe('get/set', () => {
        it('should set a key and then get the same key', async () => {
            const jobId = uuidv4();
            const taskId = uuidv4();
            const data = { data: 'yes' };
            const res = await redis.put({ jobId, taskId, data });
            const result = await redis.get(res);
            expect(result).to.deep.equal(data);
        });
    });
});