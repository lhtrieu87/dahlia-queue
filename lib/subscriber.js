var Promise = require('bluebird')
var redis = Promise.promisifyAll(require('redis'))
var Rx = require('rx')
var _ = require('lodash')

function getQueueName(topic, queueName, options) {
	options = _.defaults(options || {}, {
		prefix: 'cq'
	})

	return `${options.prefix}:${topic}.${queueName}:Q`
}

function getTopicName(topic, options) {
	options = _.defaults(options || {}, {
		prefix: 'cq'
	})

	return `${options.prefix}:${topic}:T`
}

function subscribeTopic(client, topic, queueName, options) {
	return client.saddAsync(getTopicName(topic, options), queueName)
}

module.exports = {
	getTopicName: getTopicName,
	getQueueName: getQueueName,

	// executeFunc (task to perform on each message):
	// returns a resolved promise to indicate success and remove the triggered message
	// from the specified queue; a rejected promise to retry the message again.
	createWorker: function(topic, queueName, executeFunc, options) {
		if(!topic) throw new Error('Invalid topic')
		if(!queueName) throw new Error('Invalid queueName')
		if(!executeFunc || !_.isFunction(executeFunc)) throw new Error('Invalid executeFunc')

		if(!options) options = {}
		options = _.defaults(options, {
			pollInterval: 5000,
			minWaitBetweenWorks: 1000,
			numRetries: 2
		})

		var client = redis.createClient({
		  host: process.env.REDIS_HOST || 'redis',
		  port: process.env.REDIS_PORT || 6379,
		  auth_pass: process.env.REDIS_PASS
		})

		function poll(next) {
			return Rx.Observable.create(observer => {
				// This step cannot be retried, else it will pop the next message.
				client.rpoplpushAsync(getQueueName(topic, queueName, options), getQueueName(topic, `${queueName}.inprocess`, options))
					.then(message => {
						if(message) {
							observer.onNext(message)
						} else {
							next(options.pollInterval)
						}
					})
			})
			.flatMap(message => {
				return Rx.Observable.create(observer => {
					executeFunc(JSON.parse(message))
						.then(() => {
							observer.onNext(message)
						})
						.catch(error => {
							observer.onError(error)
						})
				})
				.retry(options.numRetries)
				.flatMap((message) => {
					return client.lremAsync(getQueueName(topic, `${queueName}.inprocess`, options), -1, message)
						.then(() => {
							return JSON.parse(message)
						})
				})
				.catch(error => {
					var parsedMessage = JSON.parse(message)
					var failedMessage = _.merge(parsedMessage, {
						error: error.message
					})

					return client.lpushAsync(getQueueName(topic, `${queueName}.errors`, options), JSON.stringify(failedMessage))
						.then(() => {
							return client.lremAsync(getQueueName(topic, `${queueName}.inprocess`, options), -1, message)
						})
						.then(() => {
							parsedMessage.wasFailed = true
							return parsedMessage
						})
				})
			})
			.map(message => {
				next(options.minWaitBetweenWorks)
				return message
			})
		}

		return subscribeTopic(client, topic, queueName)
			.then(() => {
				return Rx.Observable.create(observer => {
					return Rx.Scheduler.default.scheduleRecursiveFuture(undefined, 0, (state, recurse) => {
						observer.onNext(function(interval) {
							recurse(undefined, interval)
						})
					})
				})
				.flatMap(poll)
			})		
	}
}