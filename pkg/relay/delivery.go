package relay

import (
	"fmt"
	"github.com/go-redis/redis"
)

type Delivery interface {
	Payload() string
	Ack() bool
	Reject() bool
}

type RedisDelivery struct {
	payload     string
	unackedKey  string
	rejectedKey string
	redisClient *redis.Client
}

func newDelivery(payload, unackedKey, rejectedKey string, redisClient *redis.Client) *RedisDelivery {
	return &RedisDelivery{
		payload:     payload,
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
		return false
	}

	return result.Val() == 1
}

func (delivery *RedisDelivery) Reject() bool {
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
