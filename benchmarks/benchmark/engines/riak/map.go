package riak_engine

import (
	"benchmarks/util"
	"errors"
	"fmt"
	"sync"

	"github.com/basho/riak-go-client"
)

type Map struct {
	getBuilder func() *riak.FetchMapCommandBuilder
	updBuilder func() *riak.UpdateMapCommandBuilder
	client     *riak.Client
}

func populateMaps(wg *sync.WaitGroup, client *riak.Client, nMaps int, size int, valueLength int) {
	defer wg.Done()

	op := &riak.MapOperation{}
	for i := 0; i < size; i++ {
		op.SetRegister(fmt.Sprintf("%d", i), []byte(util.RandomString(valueLength)))
	}

	semaphore := make(chan struct{}, 32)
	var wg_ sync.WaitGroup
	for i := 0; i < nMaps; i++ {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int) {
			defer wg.Done()
			cmd := util.Try(riak.NewUpdateMapCommandBuilder().
				WithBucketType("maps").
				WithBucket("maps").
				WithKey(fmt.Sprintf("m-%d", i)).
				WithMapOperation(op).
				Build())
			util.CheckErr(client.Execute(cmd))
			<-semaphore
		}(i)
	}
	wg_.Wait()
}

func newMap(client *riak.Client) *Map {
	m := &Map{}
	m.client = client
	m.getBuilder = func() *riak.FetchMapCommandBuilder {
		return riak.NewFetchMapCommandBuilder().WithBucketType("maps").WithBucket("maps")
	}
	m.updBuilder = func() *riak.UpdateMapCommandBuilder {
		return riak.NewUpdateMapCommandBuilder().WithBucketType("maps").WithBucket("maps")
	}
	return m
}

func (m *Map) Get(id string) (map[string]string, error) {
	cmd := util.Try(m.getBuilder().WithKey(id).Build())
	util.CheckErr(m.client.Execute(cmd))
	r := cmd.(*riak.FetchMapCommand).Response.Map

	map_ := map[string]string{}
	for k, v := range r.Registers {
		map_[k] = string(v)
	}

	return map_, nil
}

func (m *Map) Value(id string, key string) (string, error) {
	cmd := util.Try(m.getBuilder().WithKey(id).Build())
	util.CheckErr(m.client.Execute(cmd))
	r := cmd.(*riak.FetchMapCommand).Response.Map

	if value, ok := r.Registers[key]; ok {
		return string(value), nil
	} else {
		return "", nil
	}
}

func (m *Map) Contains(id string, key string) (bool, error) {
	cmd := util.Try(m.getBuilder().WithKey(id).Build())
	util.CheckErr(m.client.Execute(cmd))
	r := cmd.(*riak.FetchMapCommand).Response.Map

	if _, ok := r.Registers[key]; ok {
		return true, nil
	} else {
		return false, nil
	}
}

func (m *Map) Add(id string, key string, value string) error {
	op := &riak.MapOperation{}
	op.SetRegister(key, []byte(value))
	cmd := util.Try(m.updBuilder().WithKey(id).WithMapOperation(op).Build())
	util.CheckErr(m.client.Execute(cmd))
	return nil
}

func (m *Map) Rmv(id string, key string) error {
	// when removing a key from a map we need to get the context
	cmd := util.Try(m.getBuilder().WithKey(id).Build())
	util.CheckErr(m.client.Execute(cmd))
	context := cmd.(*riak.FetchMapCommand).Response.Context

	op := &riak.MapOperation{}
	op.RemoveRegister(key)
	cmd = util.Try(m.updBuilder().WithKey(id).WithMapOperation(op).WithContext(context).Build())
	util.CheckErr(m.client.Execute(cmd))

	return nil
}

func (m *Map) Clear(id string) error {
	return errors.New("not implemented")
}
