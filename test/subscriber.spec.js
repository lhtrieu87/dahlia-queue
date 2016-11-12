var Promise = require('bluebird')
var _ = require('lodash')
var subscriber = require('../lib/subscriber')

describe('subscriber', () => {
	describe('createWorker', () => {
		it('creates multiple workers for the same queue', done => {
			var numWorkers = 20

			Promise.all(_.range(numWorkers)
				.map(() => {
					return subscriber.createWorker('topic', 'queue1', message => {
						return Promise.resolve()
					})
				})
			)
			.then(workers => {
				workers = _.compact(workers)
				workers.length.should.equal(numWorkers)

				done()
			})
			.catch(error => {
				done(error)
			})
		})
	})
})