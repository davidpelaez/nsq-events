package lookupd

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/semver"
)

// GetLookupdTopics returns a []string containing a union of all the topics
// from all the given lookupd
func GetLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allTopics := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/topics", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			// {"data":{"topics":["test"]}}
			topics, _ := data.Get("topics").StringArray()
			allTopics = util.StringUnion(allTopics, topics)
		}(endpoint)
	}
	wg.Wait()
	sort.Strings(allTopics)
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allTopics, nil
}

// GetLookupdTopicChannels returns a []string containing a union of the channels
// from all the given lookupd for the given topic
func GetLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allChannels := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/channels?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)
		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			// {"data":{"channels":["test"]}}
			channels, _ := data.Get("channels").StringArray()
			allChannels = util.StringUnion(allChannels, channels)
		}(endpoint)
	}
	wg.Wait()
	sort.Strings(allChannels)
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allChannels, nil
}

// GetLookupdProducers returns a slice of pointers to Producer structs
// containing metadata for each node connected to given lookupds
func GetLookupdProducers(lookupdHTTPAddrs []string) ([]*Producer, error) {
	success := false
	allProducers := make(map[string]*Producer, 0)
	output := make([]*Producer, 0)
	maxVersion, _ := semver.Parse("0.0.0")
	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/nodes", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)
		go func(addr string, endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true

			producers := data.Get("producers")
			producersArray, _ := producers.Array()
			for i := range producersArray {
				producer := producers.GetIndex(i)
				remoteAddress := producer.Get("remote_address").MustString()
				if remoteAddress == "" {
					remoteAddress = "NA"
				}
				hostname := producer.Get("hostname").MustString()
				broadcastAddress := producer.Get("broadcast_address").MustString()
				httpPort := producer.Get("http_port").MustInt()
				tcpPort := producer.Get("tcp_port").MustInt()
				key := fmt.Sprintf("%s:%d:%d", broadcastAddress, httpPort, tcpPort)
				p, ok := allProducers[key]
				if !ok {
					var tombstones []bool
					var topics ProducerTopics

					topicList, _ := producer.Get("topics").Array()
					tombstoneList, err := producer.Get("tombstones").Array()
					if err != nil {
						// backwards compatibility with nsqlookupd < v0.2.22
						tombstones = make([]bool, len(topicList))
					} else {
						for _, t := range tombstoneList {
							tombstones = append(tombstones, t.(bool))
						}
					}

					for i, t := range topicList {
						topics = append(topics, ProducerTopic{
							Topic:      t.(string),
							Tombstoned: tombstones[i],
						})
					}

					sort.Sort(topics)

					version := producer.Get("version").MustString("unknown")
					versionObj, err := semver.Parse(version)
					if err != nil {
						versionObj = maxVersion
					}
					if maxVersion.Less(versionObj) {
						maxVersion = versionObj
					}

					p = &Producer{
						Hostname:         hostname,
						BroadcastAddress: broadcastAddress,
						TcpPort:          tcpPort,
						HttpPort:         httpPort,
						Version:          version,
						VersionObj:       versionObj,
						Topics:           topics,
					}
					allProducers[key] = p
					output = append(output, p)
				}
				p.RemoteAddresses = append(p.RemoteAddresses, fmt.Sprintf("%s/%s", addr, remoteAddress))
			}
		}(addr, endpoint)
	}
	wg.Wait()
	for _, producer := range allProducers {
		if producer.VersionObj.Less(maxVersion) {
			producer.OutOfDate = true
		}
	}
	sort.Sort(ProducersByHost{output})
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return output, nil
}

// GetLookupdTopicProducers returns a []string of the broadcast_address:http_port of all the
// producers for a given topic by unioning the results returned from the given lookupd
func GetLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allSources := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)

		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			producers := data.Get("producers")
			producersArray, _ := producers.Array()
			for i := range producersArray {
				producer := producers.GetIndex(i)
				broadcastAddress := producer.Get("broadcast_address").MustString()
				httpPort := producer.Get("http_port").MustInt()
				key := fmt.Sprintf("%s:%d", broadcastAddress, httpPort)
				allSources = util.StringAdd(allSources, key)
			}
		}(endpoint)
	}
	wg.Wait()
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allSources, nil
}

// GetNSQDTopics returns a []string containing all the topics
// produced by the given nsqd
func GetNSQDTopics(nsqdHTTPAddrs []string) ([]string, error) {
	topics := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			topicList, _ := data.Get("topics").Array()
			for i := range topicList {
				topicInfo := data.Get("topics").GetIndex(i)
				topics = util.StringAdd(topics, topicInfo.Get("topic_name").MustString())
			}
		}(endpoint)
	}
	wg.Wait()
	sort.Strings(topics)
	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}
	return topics, nil
}

// GetNSQDTopicProducers returns a []string containing the addresses of all the nsqd
// that produce the given topic out of the given nsqd
func GetNSQDTopicProducers(topic string, nsqdHTTPAddrs []string) ([]string, error) {
	addresses := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			topicList, _ := data.Get("topics").Array()
			for i := range topicList {
				topicInfo := data.Get("topics").GetIndex(i)
				if topicInfo.Get("topic_name").MustString() == topic {
					addresses = append(addresses, addr)
					return
				}
			}
		}(endpoint)
	}
	wg.Wait()
	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}
	return addresses, nil
}

// GetNSQDStats returns aggregate topic and channel stats from the given NSQD instances
//
// if selectedTopic is empty, this will return stats for *all* topic/channels
// and the ChannelStats dict will be keyed by topic + ':' + channel
func GetNSQDStats(nsqdHTTPAddrs []string, selectedTopic string) ([]*TopicStats, map[string]*ChannelStats, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup

	topicStatsList := make(TopicStatsList, 0)
	channelStatsMap := make(map[string]*ChannelStats)

	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string, addr string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()

			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true

			topics, _ := data.Get("topics").Array()
			for i := range topics {
				t := data.Get("topics").GetIndex(i)

				topicName := t.Get("topic_name").MustString()
				if selectedTopic != "" && topicName != selectedTopic {
					continue
				}
				depth := t.Get("depth").MustInt64()
				backendDepth := t.Get("backend_depth").MustInt64()
				channels := t.Get("channels").MustArray()

				e2eProcessingLatency := util.E2eProcessingLatencyAggregateFromJson(t.Get("e2e_processing_latency"), topicName, "", addr)

				topicStats := &TopicStats{
					HostAddress:  addr,
					TopicName:    topicName,
					Depth:        depth,
					BackendDepth: backendDepth,
					MemoryDepth:  depth - backendDepth,
					MessageCount: t.Get("message_count").MustInt64(),
					ChannelCount: len(channels),
					Paused:       t.Get("paused").MustBool(),

					E2eProcessingLatency: e2eProcessingLatency,
				}
				topicStatsList = append(topicStatsList, topicStats)

				for j := range channels {
					c := t.Get("channels").GetIndex(j)

					channelName := c.Get("channel_name").MustString()
					key := channelName
					if selectedTopic == "" {
						key = fmt.Sprintf("%s:%s", topicName, channelName)
					}

					channelStats, ok := channelStatsMap[key]
					if !ok {
						channelStats = &ChannelStats{
							HostAddress: addr,
							TopicName:   topicName,
							ChannelName: channelName,
						}
						channelStatsMap[key] = channelStats
					}

					depth := c.Get("depth").MustInt64()
					backendDepth := c.Get("backend_depth").MustInt64()
					clients := c.Get("clients").MustArray()

					e2eProcessingLatency := util.E2eProcessingLatencyAggregateFromJson(c.Get("e2e_processing_latency"), topicName, channelName, addr)

					hostChannelStats := &ChannelStats{
						HostAddress:   addr,
						TopicName:     topicName,
						ChannelName:   channelName,
						Depth:         depth,
						BackendDepth:  backendDepth,
						MemoryDepth:   depth - backendDepth,
						Paused:        c.Get("paused").MustBool(),
						InFlightCount: c.Get("in_flight_count").MustInt64(),
						DeferredCount: c.Get("deferred_count").MustInt64(),
						MessageCount:  c.Get("message_count").MustInt64(),
						RequeueCount:  c.Get("requeue_count").MustInt64(),
						TimeoutCount:  c.Get("timeout_count").MustInt64(),

						E2eProcessingLatency: e2eProcessingLatency,
						// TODO: this is sort of wrong; clients should be de-duped
						// client A that connects to NSQD-a and NSQD-b should only be counted once. right?
						ClientCount: len(clients),
					}
					channelStats.Add(hostChannelStats)

					for k := range clients {
						client := c.Get("clients").GetIndex(k)

						connected := time.Unix(client.Get("connect_ts").MustInt64(), 0)
						connectedDuration := time.Now().Sub(connected).Seconds()

						clientId := client.Get("clientId").MustString()
						if clientId == "" {
							// TODO: deprecated, remove in 1.0
							name := client.Get("name").MustString()
							remoteAddressParts := strings.Split(client.Get("remote_address").MustString(), ":")
							port := remoteAddressParts[len(remoteAddressParts)-1]
							if len(remoteAddressParts) < 2 {
								port = "NA"
							}
							clientId = fmt.Sprintf("%s:%s", name, port)
						}

						clientStats := &ClientStats{
							HostAddress:       addr,
							Version:           client.Get("version").MustString(),
							ClientID:          clientId,
							Hostname:          client.Get("hostname").MustString(),
							UserAgent:         client.Get("user_agent").MustString(),
							ConnectedDuration: time.Duration(int64(connectedDuration)) * time.Second, // truncate to second
							InFlightCount:     client.Get("in_flight_count").MustInt(),
							ReadyCount:        client.Get("ready_count").MustInt(),
							FinishCount:       client.Get("finish_count").MustInt64(),
							RequeueCount:      client.Get("requeue_count").MustInt64(),
							MessageCount:      client.Get("message_count").MustInt64(),
							SampleRate:        int32(client.Get("sample_rate").MustInt()),
							TLS:               client.Get("tls").MustBool(),
							Deflate:           client.Get("deflate").MustBool(),
							Snappy:            client.Get("snappy").MustBool(),
						}
						hostChannelStats.Clients = append(hostChannelStats.Clients, clientStats)
						channelStats.Clients = append(channelStats.Clients, clientStats)
					}
					sort.Sort(ClientsByHost{hostChannelStats.Clients})
					sort.Sort(ClientsByHost{channelStats.Clients})

					topicStats.Channels = append(topicStats.Channels, hostChannelStats)
				}
			}
			sort.Sort(TopicStatsByHost{topicStatsList})
		}(endpoint, addr)
	}
	wg.Wait()
	if success == false {
		return nil, nil, errors.New("unable to query any nsqd")
	}
	return topicStatsList, channelStatsMap, nil
}
