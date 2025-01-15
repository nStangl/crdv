package riak_engine

import (
	"benchmarks/util"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/basho/riak-go-client"
)

type Set struct {
	getBuilder func() *riak.FetchSetCommandBuilder
	updBuilder func() *riak.UpdateSetCommandBuilder
	client     *riak.Client
}

func populateSets(wg *sync.WaitGroup, client *riak.Client, nSets int, size int) {
	defer wg.Done()

	elems := [][]byte{}
	for i := 0; i < size; i++ {
		elems = append(elems, []byte(strconv.Itoa(i)))
	}

	semaphore := make(chan struct{}, 32)
	var wg_ sync.WaitGroup
	for i := 0; i < nSets; i++ {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int) {
			defer wg.Done()
			cmd := util.Try(riak.NewUpdateSetCommandBuilder().
				WithBucketType("sets").
				WithBucket("sets").
				WithKey(fmt.Sprintf("s-%d", i)).
				WithAdditions(elems...).
				Build())
			util.CheckErr(client.Execute(cmd))
			<-semaphore
		}(i)
	}
	wg_.Wait()
}

func newSet(client *riak.Client) *Set {
	s := &Set{}
	s.client = client
	s.getBuilder = func() *riak.FetchSetCommandBuilder {
		return riak.NewFetchSetCommandBuilder().WithBucketType("sets").WithBucket("sets")
	}
	s.updBuilder = func() *riak.UpdateSetCommandBuilder {
		return riak.NewUpdateSetCommandBuilder().WithBucketType("sets").WithBucket("sets").WithW(1).WithDw(1).WithPw(0)
	}
	return s
}

func (s *Set) Get(id string) ([]string, error) {
	cmd := util.Try(s.getBuilder().WithKey(id).Build())
	util.CheckErr(s.client.Execute(cmd))

	result := []string{}
	for _, v := range cmd.(*riak.FetchSetCommand).Response.SetValue {
		result = append(result, string(v))
	}

	return result, nil
}

func (s *Set) Contains(id string, value string) (bool, error) {
	cmd := util.Try(s.getBuilder().WithKey(id).Build())
	util.CheckErr(s.client.Execute(cmd))

	for _, v := range cmd.(*riak.FetchSetCommand).Response.SetValue {
		s := string(v)
		if s == value {
			return true, nil
		}
	}

	return false, nil
}

func (s *Set) Add(id string, value string) error {
	cmd := util.Try(s.updBuilder().WithKey(id).WithAdditions([]byte(value)).Build())
	util.CheckErr(s.client.Execute(cmd))
	return nil
}

func (s *Set) Rmv(id string, value string) error {
	// rmvs of non-existing keys without retrieving the context take a long time, so we first
	// retrieve it, as recommended by riak
	// (https://docs.riak.com/riak/kv/2.2.3/developing/data-types/sets/index.html#remove-from-a-set)
	cmd := util.Try(s.getBuilder().WithKey(id).Build())
	util.CheckErr(s.client.Execute(cmd))
	context := cmd.(*riak.FetchSetCommand).Response.Context

	cmd = util.Try(s.updBuilder().WithKey(id).WithRemovals([]byte(value)).WithContext(context).Build())
	s.client.Execute(cmd)
	return nil
}

func (s *Set) Clear(id string) error {
	return errors.New("not implemented")
}
