package relay

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/rcrowley/go-metrics"
	"strings"
)

type Delivery interface {
	Payload() string
	Ack() bool
	Reject() bool
}

type RedisDelivery struct {
	payload     string
	queue       string
	unackedKey  string
	rejectedKey string
	redisClient *redis.Client
}

func newDelivery(payload, queue, unackedKey, rejectedKey string, redisClient *redis.Client) *RedisDelivery {
	return &RedisDelivery{
		payload:     payload,
		queue:       queue,
		unackedKey:  unackedKey,
		rejectedKey: rejectedKey,
		redisClient: redisClient,
	}
}

func (delivery *RedisDelivery) String() string {
	return fmt.Sprintf("[%s %s]", delivery.payload, delivery.unackedKey)
}

func (delivery *RedisDelivery) Payload() string {
	return delivery.payload
}

func (delivery *RedisDelivery) Ack() bool {
	// debug(fmt.Sprintf("delivery ack %s", delivery))

	result := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	if redisErrIsNil(result) {
		metrics.GetOrRegisterCounter(metricsDeliveryRemoveFailure, nil).Inc(1)
		return false
	}

	metrics.GetOrRegisterCounter(strings.Replace(metricsAckTemplate, phQueue, delivery.queue, 1), nil).Inc(1)

	return result.Val() == 1
}

func (delivery *RedisDelivery) Reject() bool {
	metrics.GetOrRegisterCounter(strings.Replace(metricsNAckTemplate, phQueue, delivery.queue, 1), nil).Inc(1)
	return delivery.move(delivery.rejectedKey)
}

func (delivery *RedisDelivery) move(key string) bool {
	if redisErrIsNil(delivery.redisClient.LPush(key, delivery.payload)) {
		return false
	}

	if redisErrIsNil(delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)) {
		return false
	}

	// debug(fmt.Sprintf("delivery rejected %s", delivery))
	return true
}

//RedisDelivery impl Delivery
var _ Delivery = (*RedisDelivery)(nil)
