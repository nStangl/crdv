package riak_engine

import (
	"benchmarks/util"
	"fmt"
	"sync"

	"github.com/basho/riak-go-client"
)

type Counter struct {
	getBuilder    func() *riak.FetchCounterCommandBuilder
	updBuilder    func() *riak.UpdateCounterCommandBuilder
	getAllBuilder func() *riak.ListKeysCommandBuilder
	client        *riak.Client
}

func populateCounters(wg *sync.WaitGroup, client *riak.Client, size int) {
	defer wg.Done()

	semaphore := make(chan struct{}, 32)
	var wg_ sync.WaitGroup
	for i := 0; i < size; i++ {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int) {
			defer wg.Done()
			cmd := util.Try(riak.NewUpdateCounterCommandBuilder().
				WithBucketType("counters").
				WithBucket("counters").
				WithKey(fmt.Sprintf("c-%d", i)).
				WithIncrement(0).
				Build())
			util.CheckErr(client.Execute(cmd))
			<-semaphore
		}(i)
	}
	wg_.Wait()
}

func newCounter(client *riak.Client) *Counter {
	c := &Counter{}
	c.client = client
	c.getBuilder = func() *riak.FetchCounterCommandBuilder {
		return riak.NewFetchCounterCommandBuilder().WithBucketType("counters").WithBucket("counters")
	}
	c.updBuilder = func() *riak.UpdateCounterCommandBuilder {
		return riak.NewUpdateCounterCommandBuilder().WithBucketType("counters").WithBucket("counters")
	}
	c.getAllBuilder = func() *riak.ListKeysCommandBuilder {
		return riak.NewListKeysCommandBuilder().WithBucketType("counters").WithBucket("counters")
	}
	return c
}

func (c *Counter) Get(id string) (int64, error) {
	cmd := util.Try(c.getBuilder().WithKey(id).Build())
	util.CheckErr(c.client.Execute(cmd))
	return cmd.(*riak.FetchCounterCommand).Response.CounterValue, nil
}

func (c *Counter) Inc(id string, delta int) error {
	cmd := util.Try(c.updBuilder().WithKey(id).WithIncrement(int64(delta)).Build())
	util.CheckErr(c.client.Execute(cmd))
	return nil
}

func (c *Counter) Dec(id string, delta int) error {
	cmd := util.Try(c.updBuilder().WithKey(id).WithIncrement(int64(-delta)).Build())
	util.CheckErr(c.client.Execute(cmd))
	return nil
}

func (c *Counter) GetAll() (map[string]int64, error) {
	// get all the keys from the bucket
	cmd := util.Try(c.getAllBuilder().Build())
	util.CheckErr(c.client.Execute(cmd))

	// get the values of each key. this is parallelized to reduce latency.
	// although getting each key one by one is not ideal, this is still less expensive than using
	// riak's map reduce.
	results := map[string]int64{}
	resultsLock := sync.Mutex{}
	semaphore := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for _, id := range cmd.(*riak.ListKeysCommand).Response.Keys {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(id string) {
			defer wg.Done()
			r, _ := c.Get(id)
			resultsLock.Lock()
			results[id] = r
			resultsLock.Unlock()
			<-semaphore
		}(id)
	}
	wg.Wait()

	return results, nil
}

func (c *Counter) GetMultiple(ids []string) (map[string]int64, error) {
	// get the values of each key. this is parallelized to reduce latency.
	// although getting each key one by one is not ideal, this is still less expensive than using
	// riak's map reduce.
	results := map[string]int64{}
	resultsLock := sync.Mutex{}
	semaphore := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(id string) {
			defer wg.Done()
			r, _ := c.Get(id)
			resultsLock.Lock()
			results[id] = r
			resultsLock.Unlock()
			<-semaphore
		}(id)
	}
	wg.Wait()

	return results, nil
}
