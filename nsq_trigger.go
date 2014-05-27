// this a go wrapper to replace a curl POST to nsqd's /put endpoint
package main

import (
  "flag"
  "fmt"
  "net/http"
  "io/ioutil"
  "bytes"
  "os"
  "strings"
)

var (
  topic         = flag.String("topic", "", "nsq topic")
  nsqdHTTPAddrs  = flag.String("nsqd-http-address", "", "nsqd HTTP address")
)

func failWithUsage() {
  flags := "[--topic=events] [--nsqd-http-address=127.0.0.1:4151]"
  arguments := "<event_name> [<event_body>]"
  fmt.Println("e.g: nsq_trigger", flags, arguments)
  os.Exit(1)
}

func main() {

  

  flag.Parse()

  if len(flag.Args()) == 0 {
  	fmt.Println("At least the event name is required as non-flag argument")
  	failWithUsage()
  	
  	os.Exit(1)
  }

  eventBody := strings.Join(flag.Args(), " ")

  if *topic == "" {
    *topic = "events"
  }

  if *nsqdHTTPAddrs == "" {
    *nsqdHTTPAddrs = "127.0.0.1:4151"
  }

  url := "http://" + *nsqdHTTPAddrs + "/put?topic=" + *topic

  body := bytes.NewBuffer([]byte(eventBody))
  r, _ := http.Post(url, "text/plain", body)
  response, _ := ioutil.ReadAll(r.Body)
  fmt.Println(*nsqdHTTPAddrs + ":",string(response))

}
