package relay

import (
	"fmt"
	"log"
	"strings"
	"time"
	"github.com/go-redis/redis"
	"github.com/adjust/uniuri"
)

const (
	queueUnackedTemplate  = "queue::[{queue}]::[{consumer}]::unacked" // List of deliveries consumers of {connection} are currently consuming
	queueRejectedTemplate = "rmq::queue::[{queue}]::rejected"         // List of rejected deliveries from that {queue}

	phQueue    = "{queue}"    // queue name
	phConsumer = "{consumer}" // consumer name (consisting of tag and token)

	lpoprpush  = `
	local src_list, dst_list = ARGV[1], ARGV[2];
	local value = redis.call('lpop', src_list)
	if value then -- avoid pushing nils
	  redis.call('rpush', dst_list, value)
	end
	return value`
)

type Queue interface {
	Publish(payload string) bool
	PublishBytes(payload []byte) bool
	StartConsuming(prefetchLimit int, pollDuration time.Duration) bool
	StopConsuming() bool
	AddConsumer(tag string, consumer RedisQueueConsumer) string
	ReturnRejected(count int) int
	ReturnAllRejected() int
	Size() int
	//Close() bool
}

type redisQueue struct {
	name             string
	connectionName   string
	queueKey         string //actual queue
	rejectedKey      string // key to list of rejected deliveries
	unackedKey       string // key to list of currently consuming deliveries
	redisClient      *redis.Client
	deliveryChan     chan Delivery // nil for publish channels, not nil for consuming channels
	prefetchLimit    int           // max number of prefetched deliveries number of unacked can go up to
	pollDuration     time.Duration
	consumingStopped bool
}

func newQueue(name, connectionName, queueKey string, rejectedKey *string, redisClient *redis.Client) *redisQueue {

	_rejectedKey := strings.Replace(queueRejectedTemplate, phQueue, name, 1)
	if rejectedKey != nil {
		_rejectedKey = *rejectedKey
	}

	unackedKey := strings.Replace(queueUnackedTemplate, phQueue, queueKey, 1)
	unackedKey = strings.Replace(unackedKey, phConsumer, connectionName, 1)

	queue := &redisQueue{
		name:           name,
		connectionName: connectionName,
		queueKey:       queueKey,
		rejectedKey:    _rejectedKey,
		unackedKey:     unackedKey,
		redisClient:    redisClient,
	}
	return queue
}

func (queue *redisQueue) String() string {
	return fmt.Sprintf("[%s conn:%s]", queue.name, queue.connectionName)
}

// Publish adds a delivery with the given payload to the queue
func (queue *redisQueue) Publish(payload string) bool {
	// debug(fmt.Sprintf("publish %s %s", payload, queue)) 
	return !redisErrIsNil(queue.redisClient.LPush(queue.queueKey, payload))
}

// PublishBytes just casts the bytes and calls Publish
func (queue *redisQueue) PublishBytes(payload []byte) bool {
	return queue.Publish(string(payload))
}

func (queue *redisQueue) Size() int {
	result := queue.redisClient.LLen(queue.queueKey)
	if redisErrIsNil(result) {
		return 0
	}
	return int(result.Val())
}


func (queue *redisQueue) UnackedCount() int {
	result := queue.redisClient.LLen(queue.unackedKey)
	if redisErrIsNil(result) {
		return 0
	}
	return int(result.Val())
}
// ReturnAllUnacked moves all unacked deliveries back to the ready in front of the queue
// queue and deletes the unacked key afterwards, returns number of returned
// deliveries
func (queue *redisQueue) ReturnAllUnacked() int {
	unackedCount := queue.UnackedCount()

	for i := 0; i < unackedCount; i++ {
		if redisErrIsNil(queue.redisClient.Eval(lpoprpush, []string{}, queue.unackedKey, queue.queueKey)) {
			return i
		}
		// debug(fmt.Sprintf("rmq queue returned unacked delivery %s %s", result.Val(), queue.readyKey))
	}

	return unackedCount
}


// ReturnAllRejected moves all rejected deliveries back to the ready
// list and returns the number of returned deliveries
func (queue *redisQueue) ReturnAllRejected() int {
	result := queue.redisClient.LLen(queue.rejectedKey)
	if redisErrIsNil(result) {
		return 0
	}

	rejectedCount := int(result.Val())
	return queue.ReturnRejected(rejectedCount)
}

// ReturnRejected tries to return count rejected deliveries back to
// the ready list and returns the number of returned deliveries
func (queue *redisQueue) ReturnRejected(count int) int {
	if count == 0 {
		return 0
	}

	for i := 0; i < count; i++ {
		result := queue.redisClient.Eval(lpoprpush, []string{}, queue.rejectedKey, queue.queueKey)
		if redisErrIsNil(result) {
			return i
		}
		// debug(fmt.Sprintf("rmq queue returned rejected delivery %s %s", result.Val(), queue.readyKey))
	}

	return count
}


// StartConsuming starts consuming into a channel of size prefetchLimit
// must be called before consumers can be added!
// pollDuration is the duration the queue sleeps before checking for new deliveries
func (queue *redisQueue) StartConsuming(prefetchLimit int, pollDuration time.Duration) bool {
	if queue.deliveryChan != nil {
		return false // already consuming
	}

	queue.prefetchLimit = prefetchLimit
	queue.pollDuration = pollDuration
	queue.deliveryChan = make(chan Delivery, prefetchLimit)
	// log.Printf("rmq queue started consuming %s %d %s", queue, prefetchLimit, pollDuration)
	go queue.consume()
	return true
}

func (queue *redisQueue) StopConsuming() bool {
	if queue.deliveryChan == nil || queue.consumingStopped {
		return false // not consuming or already stopped
	}

	queue.consumingStopped = true
	return true
}

// AddConsumer adds a consumer to the queue and returns its internal name
// panics if StartConsuming wasn't called before!
func (queue *redisQueue) AddConsumer(tag string, consumer RedisQueueConsumer) string {
	name := queue.addConsumer(tag)
	go queue.consumerConsume(consumer)
	return name
}

func (queue *redisQueue) addConsumer(tag string) string {
	if queue.deliveryChan == nil {
		log.Panicf("rmq queue failed to add consumer, call StartConsuming first! %s", queue)
	}

	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))

	log.Printf("rmq queue added consumer %s %s", queue, name)
	return name
}

func (queue *redisQueue) consume() {
	for {
		batchSize := queue.batchSize()
		wantMore := queue.consumeBatch(batchSize)

		if !wantMore {
			time.Sleep(queue.pollDuration)
		}

		if queue.consumingStopped {
			log.Printf("rmq queue stopped consuming %s", queue)
			return
		}
	}
}

func (queue *redisQueue) batchSize() int {
	prefetchCount := len(queue.deliveryChan)
	prefetchLimit := queue.prefetchLimit - prefetchCount
	// TODO: ignore ready count here and just return prefetchLimit?
	if readyCount := queue.Size(); readyCount < prefetchLimit {
		return readyCount
	}
	return prefetchLimit
}



// consumeBatch tries to read batchSize deliveries, returns true if any and all were consumed
func (queue *redisQueue) consumeBatch(batchSize int) bool {
	if batchSize == 0 {
		return false
	}

	for i := 0; i < batchSize; i++ {
		result := queue.redisClient.RPopLPush(queue.queueKey, queue.unackedKey)
		if redisErrIsNil(result) {
			debug(fmt.Sprintf("rmq queue consumed last batch %s %d", queue, i)) 
			return false
		}

		debug(fmt.Sprintf("consume %d/%d %s %s", i, batchSize, result.Val(), queue)) 
		queue.deliveryChan <- newDelivery(result.Val(), queue.unackedKey, queue.rejectedKey, queue.redisClient)
	}

	debug(fmt.Sprintf("rmq queue consumed batch %s %d", queue, batchSize)) 
	return true
}

func (queue *redisQueue) consumerConsume(consumer RedisQueueConsumer) {
	for delivery := range queue.deliveryChan {
		debug(fmt.Sprintf("consumer consume %s %s", delivery, consumer)) 
		consumer.Consume(delivery)
	}
}


func debug(message string) {
	log.Printf("rmq debug: %s", message) 
}

var _ Queue = (*redisQueue)(nil)
