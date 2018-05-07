package relay

import (
	"github.com/go-redis/redis"
)

type RedisSource struct {
	Endpoint, Password string
	Pattern string
}

func (r Relayer) createClient() *redis.Client {

	client := redis.NewClient(&redis.Options{
		Addr:     r.Source.Endpoint,
		Password: r.Source.Password,
		DB:       0, // use default DB
	})

	return client
}

func (r Relayer) getQueues(client *redis.Client) ([]string, error) {
	queues, err := client.Keys(r.Source.Pattern).Result()
	return queues , err
}

// redisErrIsNil returns false if there is no error, true if the result error is nil and panics if there's another error
func redisErrIsNil(result redis.Cmder) bool {
	switch result.Err() {
	case nil:
		return false
	case redis.Nil:
		return true
	default:
		//log.Panicf("rmq redis error is not nil %s", result.Err())
		return false
	}
}
