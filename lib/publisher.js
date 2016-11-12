var Promise = require('bluebird')
var redis = Promise.promisifyAll(require('redis'))
var _ = require('lodash')

module.exports = {
	connect: function() {
		var client = redis.createClient({
		  host: process.env.REDIS_HOST || 'localhost',
		  port: process.env.REDIS_PORT || 6379,
		  auth_pass: process.env.REDIS_PASS
		})

		return new Promise((resolve, reject) => {
			client.once('error', (error) => {
				reject(error)
			})

			client.once('connect', () => {
				resolve({
					disconnect: function() {
						client.quit()
					},

					publish: function(topic, payload, options) {
						options = _.defaults(options || {}, {
							prefix: 'cq'
						})

						return Promise.coroutine(function* () {
							var results = []
							var queues = yield client.smembersAsync(`${options.prefix}:${topic}:T`)
							for(var i = 0; i < queues.length; i++) {
								var queue = queues[i]

								var r = yield client.lpushAsync(`${options.prefix}:${topic}.${queue}:Q`, JSON.stringify(payload))
								results.push(r)
							}

							return results
						})()
					}
				})
			})
		})
	}
}
