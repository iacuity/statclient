package statclient

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/llog"
)

const (
	MAX_CHANNEL_BUFFER = 100000
	REFRESH_INTERVAL   = 60 // in seconds
)

var (
	serviceEndpoint     string
	transport           *http.Transport
	hclient             *http.Client
	sChan               = make(chan *Pair, MAX_CHANNEL_BUFFER)
	maxIdelConnsPerHost = 10
	requestTimeout      = 500 // in miliseconds
)

type Pair struct {
	Key   string `json:"key,omitempty"`
	Value int64  `json:"val,omitempty"`
}

type Request struct {
	Pairs []Pair `json:"pairs,omitempty"`
}

func SetServiceEndpoint(endpoint string) {
	serviceEndpoint = endpoint
}

func SetMaxIdelConnsPerHost(connsPerHost int) {
	maxIdelConnsPerHost = connsPerHost
	refreshHttpClient()
}

func SetRequestTimeout(timeout int) {
	requestTimeout = requestTimeout
	refreshHttpClient()
}

func refreshHttpClient() {
	transport = &http.Transport{DisableKeepAlives: false,
		MaxIdleConnsPerHost: maxIdelConnsPerHost,
	}

	timeout := time.Duration(time.Duration(requestTimeout) * time.Millisecond)

	hclient = &http.Client{Transport: transport,
		Timeout: timeout,
	}
}

func init() {
	go sendStat()
}

func FlushImmediate(pairs []Pair) error {
	req := Request{Pairs: pairs}
	byts, err := json.Marshal(req)
	request, err := http.NewRequest("POST", serviceEndpoint, bytes.NewBuffer(byts))
	if nil != err {
		return err
	}

	resp, err := hclient.Do(request)
	if nil != err {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func flushStat(sMap map[string]int64) error {
	pairs := make([]Pair, 0)
	for key, val := range sMap {
		pairs = append(pairs, Pair{Key: key, Value: val})
	}

	return FlushImmediate(pairs)
}

func sendStat() {
	ticker := time.NewTicker(time.Second * (time.Duration)(REFRESH_INTERVAL))
	sMap := make(map[string]int64)
	for {
		select {
		case pair := <-sChan:
			if val, found := sMap[pair.Key]; !found {
				sMap[pair.Key] = pair.Value
			} else {
				sMap[pair.Key] = val + pair.Value
			}
		case <-ticker.C:
			if len(sMap) > 0 {
				// send stats to stat service
				if err := flushStat(sMap); nil != err {
					llog.Error("Flush error:%s", err.Error())
				} else {
					sMap = make(map[string]int64)
				}
			}
		}
	}
}

func PushStat(key string, value int64) {
	sChan <- &Pair{Key: key, Value: value}
}
