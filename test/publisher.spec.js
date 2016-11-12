var redis = require('redis')
var Promise = require('bluebird')
var should = require('should')
var _ = require('lodash')
var publisher = require('../lib/publisher')
var subscriber = require('../lib/subscriber')
var _ = require('lodash')

describe('MessageBroker', () => {
	var client

	beforeEach(done => {
		client = redis.createClient({
		  host: process.env.REDIS_HOST || 'localhost',
		  port: process.env.REDIS_PORT || 6379,
		  auth_pass: process.env.REDIS_PASS
		})

		client.flushallAsync()
			.then(() => done())
	})

	afterEach(done => {
		client.end(true)
		done()
	})

	describe('publish & message stream for per worker', () => {
		var worker, subscription
		var queueName = 'queue' + _.random(10000000)

		beforeEach(done => {
			subscriber.createWorker('aTopic', queueName, (message) => {
				return Promise.resolve(message)
			}, {
				pollInterval: 1,
				minWaitBetweenWorks: 1
			})
			.then(resultedWorker => {
				worker = resultedWorker

				done()
			})
		})

		afterEach(() => {
			if(subscription)
				subscription.dispose()
		})

		it('consumer consumes messages from a subscribing queue', done => {
			var numMessages = 10
			
			var count = 0
			subscription = worker
				.subscribe(data => {
					data.hello.should.equal('France' + count)
					should.not.exist(data.wasFailed)

					count = count + 1

					if(count === numMessages) {
						client.llenAsync(subscriber.getQueueName('aTopic', `${queueName}.inprocess`))
							.then(inProcessCount => {
								inProcessCount.should.be.equal(0)
								done()
							})
					}
				}, error => {
					done(error)
				})

			publisher.connect()
				.then(p => {
					_.range(numMessages)
						.map((index) => {
							return p.publish('aTopic', {
								hello: 'France' + index 
							})
						})
				})
		})
		.timeout(500)
		.slow(500)

		describe('broadcasts to all subscribed queues', done => {
			var secondWorker, secondSubscription
			var secondQueueName = 'queue' + _.random(10000000)

			beforeEach(done => {
				subscriber.createWorker('aTopic', secondQueueName, (message) => {
					return Promise.resolve(message)
				}, {
					pollInterval: 1,
					minWaitBetweenWorks: 1
				})
				.then(returnedWorker => {
					secondWorker = returnedWorker

					done()
				})
				.catch(error => {
					done(error)
				})
			})

			afterEach(() => {
				if(secondSubscription)
					secondSubscription.dispose()
			})

			it('a message is received by all queue workers', done => {
				var count = 0

				subscription = worker.subscribe(message => {
					message.should.eql({"hello":"Us"})

					count = count + 1
					if(count === 2) done()
				}, error => {
					done(error)
				})

				secondSubscription = secondWorker.subscribe(message => {
					message.should.eql({"hello":"Us"})

					count = count + 1
					if(count === 2) done()
				}, error => {
					done(error)
				})

				publisher.connect()
					.then(p => {
						p.publish('aTopic', {
							hello: 'Us'
						})
					})
					.catch(error => {
						done(error)
					})
			})
		})

		it('only one subscriber performs a task on a single queue', done => {
			var secondWorker, secondSubscription

			subscriber.createWorker('aTopic', queueName, (message) => {
				return Promise.resolve(message)
			}, {
				pollInterval: 1,
				minWaitBetweenWorks: 1
			})
			.then(returnedWorker => {
				secondWorker = returnedWorker

				var count = 0

				subscription = worker.subscribe(message => {
					message.should.eql({"hello":"Singapore"})

					count = count + 1

					if(count === 2) done(new Error('Task have been executed twice'))
				})

				secondSubscription = secondWorker.subscribe(message => {
					message.should.eql({"hello":"Singapore"})

					count = count + 1

					if(count === 2) done(new Error('Task have been executed twice'))
				})

				publisher.connect()
					.then(p => {
						p.publish('aTopic', {
							hello: 'Singapore'
						})
					})
			})
			.delay(100)
			.then(() => {
				secondSubscription.dispose()
				done()
			})
			.catch(error => {
				done(error)
			})
		})

		describe('retry cases', () => {
			it('moves correct message to error list', done => {
				var secondWorker, secondSubscription
				queueName = 'queue' + _.random(10000000)

				subscriber.createWorker('aTopic', queueName, (message) => {
					return Promise.resolve(message)
						.delay(100)
				}, {
					pollInterval: 1,
					minWaitBetweenWorks: 1
				})
				.then(returnedWorker => {
					worker = returnedWorker

					subscription = worker
						.subscribe(message => {
							message.should.eql({"hello":"Coffee0"})

							Promise.all([
								client.lrangeAsync(subscriber.getQueueName('aTopic', `${queueName}.errors`), 0, 10),
								client.llenAsync(subscriber.getQueueName('aTopic', `${queueName}.inprocess`))
							])
								.spread((errors, inProcessCount) => {
									errors.length.should.equal(1)
									errors[0].should.eql(JSON.stringify({"hello":"Coffee1", error: 'Error'}))

									inProcessCount.should.equal(0)

									secondSubscription.dispose()

									done()
								})
								.catch(error => {
									done(error)
								})
						})

					publisher.connect()
						.then(p => {
							p.publish('aTopic', {
								hello: 'Coffee0'
							})

							return p
						})
						.delay(10)
						.then(p => {
							return subscriber.createWorker('aTopic', queueName, (message) => {
									return Promise.reject(new Error('Error'))
								}, {
									pollInterval: 1,
									minWaitBetweenWorks: 1
								})
								.then(returnedWorker => {
									secondWorker = returnedWorker
									secondSubscription = secondWorker.subscribe(message => {
										message.should.eql({"hello":"Coffee1", wasFailed: true, error: 'Error'})
										message.wasFailed.should.equal(true)
									})

									p.publish('aTopic', {
										hello: 'Coffee1'
									})
						})
					})
				})
				.catch(error => {
					done(error)
				})
			})
			.slow(500)

			it('retries successfully', done => {
				var retryCount = 0
				var secondQueueName = 'queue' + _.random(10000000)

				subscriber.createWorker('aTopic', secondQueueName, (message) => {
					retryCount = retryCount + 1
					if(retryCount === 1)
						return Promise.reject(new Error('Error'))
					else {
						return Promise.resolve(message)
					}
				}, {
					pollInterval: 1,
					minWaitBetweenWorks: 1
				})
				.then((worker) => {
					subscription = worker
						.subscribe(data => {
							should.not.exist(data.wasFailed)

							retryCount.should.equal(2)

							client.llenAsync(subscriber.getQueueName('aTopic', `${secondQueueName}.inprocess`))
								.then(inProcessCount => {
									inProcessCount.should.be.equal(0)
									return Promise.all([
										client.llenAsync(subscriber.getQueueName('aTopic', `${secondQueueName}.errors`)),
										client.lrangeAsync(subscriber.getQueueName('aTopic', `${secondQueueName}`), 0, 0)
									])
								})
								.spread((errorCount, works) => {
									errorCount.should.equal(0)
									works.length.should.equal(0)

									done()
								})
								.catch(error => {
									done(error)
								})
						}, error => {
							done(error)
						})

					publisher.connect()
						.then(p => {
							p.publish('aTopic', {
								hello: 'Italy'
							})
						})
				})
				.catch(error => {
					done(error)
				})
			})

			it('work numRetries reaches max, puts into error queue', done => {
				var retryCount = 0
				var secondQueueName = 'queue' + _.random(10000000)

				subscriber.createWorker('aTopic', secondQueueName, (message) => {
					retryCount = retryCount + 1
					return Promise.reject(new Error('Error'))
				}, {
					pollInterval: 1,
					minWaitBetweenWorks: 1
				})
				.then((worker) => {
					subscription = worker
						.subscribe(data => {
							data.wasFailed.should.equal(true)

							retryCount.should.equal(2)

							client.llenAsync(subscriber.getQueueName('aTopic', `${secondQueueName}.inprocess`))
								.then(inProcessCount => {
									inProcessCount.should.be.equal(0)
									return Promise.all([
										client.llenAsync(subscriber.getQueueName('aTopic', `${secondQueueName}.errors`)),
										client.lrangeAsync(subscriber.getQueueName('aTopic', `${secondQueueName}.errors`), 0, 0)
									])
								})
								.spread((errorCount, errors) => {
									errorCount.should.equal(1)
									errors[0].should.equal(JSON.stringify({ hello: 'world', error: 'Error' }))

									done()
								})
								.catch(error => {
									done(error)
								})
						}, error => {
							done(error)
						})

					publisher.connect()
						.then(p => {
							p.publish('aTopic', {
								hello: 'world'
							})
						})
				})
				.catch(error => {
					done(error)
				})
			})

			it('work numRetries reaches max, message stream is still alive', done => {
				var numMessages = 5
				var count = 0
				var secondQueueName = 'queue' + _.random(10000000)

				subscriber.createWorker('aTopic', secondQueueName, (message) => {
					if(message.hello === 'Eu4') return Promise.resolve(message)
					else {
						return Promise.reject(new Error('Error'))
					}
				}, {
					pollInterval: 1,
					minWaitBetweenWorks: 1
				})
				.then((worker) => {
					subscription = worker
						.subscribe(data => {
							count = count + 1
							if(count === numMessages) {
								client.llenAsync(subscriber.getQueueName('aTopic', `${secondQueueName}.inprocess`))
									.then(inProcessCount => {
										inProcessCount.should.be.equal(0)
										return Promise.all([
											client.llenAsync(subscriber.getQueueName('aTopic', `${secondQueueName}.errors`)),
											client.lrangeAsync(`aTopic.${secondQueueName}.errors:Q`, 0, 3)
										])
									})
									.spread((errorCount, errors) => {
										errorCount.should.equal(4)

										errors.forEach((error, i) => {
											error.should.equal(JSON.stringify({ hello: 'Eu' + (numMessages - 2 - i), error: 'Error' }))
										})

										done()
									})
									.catch(error => {
										done(error)
									})
							}
						}, error => {
							done(error)
						})

					publisher.connect()
						.then(p => {
							_.range(numMessages)
								.map(i => {
									p.publish('aTopic', {
										hello: 'Eu' + i
									})
								})
						})
				})
				.catch(error => {
					done(error)
				})
			})
		})
	})
})