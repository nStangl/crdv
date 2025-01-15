package riak_engine

import (
	"benchmarks/util"
	"errors"
	"fmt"
	"sync"

	"github.com/basho/riak-go-client"
)

type Register struct {
	getBuilder func() *riak.FetchValueCommandBuilder
	setBuilder func() *riak.StoreValueCommandBuilder
	client     *riak.Client
}

func populateRegisters(wg *sync.WaitGroup, client *riak.Client, size int, valueLength int) {
	defer wg.Done()

	obj := &riak.Object{
		Value:  []byte(util.RandomString(valueLength)),
		Bucket: "",
		Key:    "",
	}

	semaphore := make(chan struct{}, 32)
	var wg_ sync.WaitGroup
	for i := 0; i < size; i++ {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int) {
			defer wg.Done()
			cmd := util.Try(riak.NewStoreValueCommandBuilder().
				WithBucketType("registers").
				WithBucket("registers").
				WithKey(fmt.Sprintf("r-%d", i)).
				WithContent(obj).
				Build())
			util.CheckErr(client.Execute(cmd))
			<-semaphore
		}(i)
	}
	wg_.Wait()
}

func newRegister(client *riak.Client) *Register {
	r := &Register{}
	r.client = client
	r.getBuilder = func() *riak.FetchValueCommandBuilder {
		return riak.NewFetchValueCommandBuilder().WithBucketType("registers").WithBucket("registers")
	}
	r.setBuilder = func() *riak.StoreValueCommandBuilder {
		return riak.NewStoreValueCommandBuilder().WithBucketType("registers").WithBucket("registers")
	}
	return r
}

func (r *Register) Get(id string) (string, error) {
	cmd := util.Try(r.getBuilder().WithKey(id).Build())
	util.CheckErr(r.client.Execute(cmd))
	result := cmd.(*riak.FetchValueCommand).Response.Values

	// should only happen in large deployments, when this is executed immediately after the populate
	// and not all data is yet available in all sites.
	if len(result) == 0 {
		return "", errors.New("register not found")
	}

	return string(result[0].Value), nil
}

func (r *Register) Set(id string, value string) error {
	obj := &riak.Object{
		Value:  []byte(value),
		Bucket: "",
		Key:    "",
	}
	cmd := util.Try(r.setBuilder().WithKey(id).WithContent(obj).Build())
	util.CheckErr(r.client.Execute(cmd))
	return nil
}
