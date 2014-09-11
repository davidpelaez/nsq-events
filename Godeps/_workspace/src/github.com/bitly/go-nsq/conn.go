package nsq

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mreiferson/go-snappystream"
)

// IdentifyResponse represents the metadata
// returned from an IDENTIFY command to nsqd
type IdentifyResponse struct {
	MaxRdyCount int64 `json:"max_rdy_count"`
	TLSv1       bool  `json:"tls_v1"`
	Deflate     bool  `json:"deflate"`
	Snappy      bool  `json:"snappy"`
}

// Conn represents a connection to nsqd
//
// Conn exposes a set of callbacks for the
// various events that occur on a connection
type Conn struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesInFlight int64
	maxRdyCount      int64
	rdyCount         int64
	lastRdyCount     int64
	lastMsgTimestamp int64

	sync.Mutex

	topic   string
	channel string

	net.Conn
	tlsConn *tls.Conn
	addr    string

	r io.Reader
	w io.Writer

	// ResponseCB is called when the connection
	// receives a FrameTypeResponse from nsqd
	ResponseCB func(*Conn, []byte)

	// ErrorCB is called when the connection
	// receives a FrameTypeError from nsqd
	ErrorCB func(*Conn, []byte)

	// MessageCB is called when the connection
	// receives a FrameTypeMessage from nsqd
	MessageCB func(*Conn, *Message)

	// MessageProcessedCB is called when the connection
	// handles a FIN or REQ command from a message handler
	MessageProcessedCB func(*Conn, *FinishedMessage)

	// IOErrorCB is called when the connection experiences
	// a low-level TCP transport error
	IOErrorCB func(*Conn, error)

	// HeartbeatCB is called when the connection
	// receives a heartbeat from nsqd
	HeartbeatCB func(*Conn)

	// CloseCB is called when the connection
	// closes, after all cleanup
	CloseCB func(*Conn)

	cmdBuf bytes.Buffer

	flateWriter *flate.Writer

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	backoffCounter int32
	rdyRetryTimer  *time.Timer

	finishedMessages chan *FinishedMessage
	cmdChan          chan *Command
	exitChan         chan int
	drainReady       chan int

	ShortIdentifier string // an identifier to send to nsqd when connecting (defaults: short hostname)
	LongIdentifier  string // an identifier to send to nsqd when connecting (defaults: long hostname)

	HeartbeatInterval time.Duration // duration of time between heartbeats
	SampleRate        int32         // set the sampleRate of the client's messagePump (requires nsqd 0.2.25+)
	UserAgent         string        // a string identifying the agent for this client in the spirit of HTTP (default: "<client_library_name>/<version>")

	// transport layer security
	TLSv1     bool        // negotiate enabling TLS
	TLSConfig *tls.Config // client TLS configuration

	// compression
	Deflate      bool // negotiate enabling Deflate compression
	DeflateLevel int  // the compression level to negotiate for Deflate
	Snappy       bool // negotiate enabling Snappy compression

	// output buffering
	OutputBufferSize    int64         // size of the buffer (in bytes) used by nsqd for buffering writes to this connection
	OutputBufferTimeout time.Duration // timeout (in ms) used by nsqd before flushing buffered writes (set to 0 to disable). Warning: configuring clients with an extremely low (< 25ms) output_buffer_timeout has a significant effect on nsqd CPU usage (particularly with > 50 clients connected).

	stopFlag int32
	stopper  sync.Once
	wg       sync.WaitGroup

	readLoopRunning int32
}

// NewConn returns a new Conn instance
func NewConn(addr string, topic string, channel string) *Conn {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	return &Conn{
		addr: addr,

		topic:   topic,
		channel: channel,

		ReadTimeout:  DefaultClientTimeout,
		WriteTimeout: time.Second,

		maxRdyCount:      2500,
		lastMsgTimestamp: time.Now().UnixNano(),

		finishedMessages: make(chan *FinishedMessage),
		cmdChan:          make(chan *Command),
		exitChan:         make(chan int),
		drainReady:       make(chan int),

		ShortIdentifier: strings.Split(hostname, ".")[0],
		LongIdentifier:  hostname,

		DeflateLevel:        6,
		OutputBufferSize:    16 * 1024,
		OutputBufferTimeout: 250 * time.Millisecond,

		HeartbeatInterval: DefaultClientTimeout / 2,

		UserAgent: fmt.Sprintf("go-nsq/%s", VERSION),
	}
}

// Connect dials and bootstraps the nsqd connection
// (including IDENTIFY) and returns the IdentifyResponse
func (c *Conn) Connect() (*IdentifyResponse, error) {
	conn, err := net.DialTimeout("tcp", c.addr, time.Second)
	if err != nil {
		return nil, err
	}
	c.Conn = conn
	c.r = conn
	c.w = conn

	_, err = c.Write(MagicV2)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", c.addr, err)
	}

	resp, err := c.identify()
	if err != nil {
		return nil, err
	}

	c.wg.Add(2)
	atomic.StoreInt32(&c.readLoopRunning, 1)
	go c.readLoop()
	go c.writeLoop()
	return resp, nil
}

// Close idempotently closes the connection
func (c *Conn) Close() error {
	// so that external users dont need
	// to do this dance...
	// (would only happen if the dial failed)
	if c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

// Stop gracefully initiates connection close
// allowing in-flight messages to finish
func (c *Conn) Stop() {
	atomic.StoreInt32(&c.stopFlag, 1)
}

// IsStopping indicates whether or not the
// connection is currently in the processing of
// gracefully closing
func (c *Conn) IsStopping() bool {
	return atomic.LoadInt32(&c.stopFlag) == 1
}

// RDY returns the current RDY count
func (c *Conn) RDY() int64 {
	return atomic.LoadInt64(&c.rdyCount)
}

// LastRDY returns the previously set RDY count
func (c *Conn) LastRDY() int64 {
	return atomic.LoadInt64(&c.lastRdyCount)
}

// SetRDY stores the specified RDY count
func (c *Conn) SetRDY(rdy int64) {
	atomic.StoreInt64(&c.rdyCount, rdy)
	atomic.StoreInt64(&c.lastRdyCount, rdy)
}

// MaxRDY returns the nsqd negotiated maximum
// RDY count that it will accept for this connection
func (c *Conn) MaxRDY() int64 {
	return c.maxRdyCount
}

// LastMessageTime returns a time.Time representing
// the time at which the last message was received
func (c *Conn) LastMessageTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastMsgTimestamp))
}

// Address returns the configured destination nsqd address
func (c *Conn) Address() string {
	return c.addr
}

// String returns the fully-qualified address/topic/channel
func (c *Conn) String() string {
	return fmt.Sprintf("%s/%s/%s", c.addr, c.topic, c.channel)
}

// Read performs a deadlined read on the underlying TCP connection
func (c *Conn) Read(p []byte) (int, error) {
	c.SetReadDeadline(time.Now().Add(c.ReadTimeout))
	return c.r.Read(p)
}

// Write performs a deadlined write on the underlying TCP connection
func (c *Conn) Write(p []byte) (int, error) {
	c.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	return c.w.Write(p)
}

// SendCommand writes the specified Command to the underlying
// TCP connection according to the NSQ TCP protocol spec
func (c *Conn) SendCommand(cmd *Command) error {
	c.Lock()
	defer c.Unlock()

	c.cmdBuf.Reset()
	err := cmd.Write(&c.cmdBuf)
	if err != nil {
		return err
	}

	_, err = c.cmdBuf.WriteTo(c)
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}

// ReadUnpackedResponse reads and parses data from the underlying
// TCP connection according to the NSQ TCP protocol spec and
// returns the frameType, data or error
func (c *Conn) ReadUnpackedResponse() (int32, []byte, error) {
	resp, err := ReadResponse(c)
	if err != nil {
		return -1, nil, err
	}
	return UnpackResponse(resp)
}

func (c *Conn) identify() (*IdentifyResponse, error) {
	ci := make(map[string]interface{})
	ci["short_id"] = c.ShortIdentifier
	ci["long_id"] = c.LongIdentifier
	ci["tls_v1"] = c.TLSv1
	ci["deflate"] = c.Deflate
	ci["deflate_level"] = c.DeflateLevel
	ci["snappy"] = c.Snappy
	ci["feature_negotiation"] = true
	ci["heartbeat_interval"] = int64(c.HeartbeatInterval / time.Millisecond)
	ci["sample_rate"] = c.SampleRate
	ci["user_agent"] = c.UserAgent
	ci["output_buffer_size"] = c.OutputBufferSize
	ci["output_buffer_timeout"] = int64(c.OutputBufferTimeout / time.Millisecond)
	cmd, err := Identify(ci)
	if err != nil {
		return nil, ErrIdentify{Reason: err.Error()}
	}

	err = c.SendCommand(cmd)
	if err != nil {
		return nil, ErrIdentify{Reason: err.Error()}
	}

	frameType, data, err := c.ReadUnpackedResponse()
	if err != nil {
		return nil, ErrIdentify{Reason: err.Error()}
	}

	if frameType == FrameTypeError {
		return nil, ErrIdentify{string(data)}
	}

	// check to see if the server was able to respond w/ capabilities
	// i.e. it was a JSON response
	if data[0] != '{' {
		return nil, nil
	}

	resp := &IdentifyResponse{}
	err = json.Unmarshal(data, resp)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	c.maxRdyCount = resp.MaxRdyCount

	if resp.TLSv1 {
		err := c.upgradeTLS(c.TLSConfig)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Deflate {
		err := c.upgradeDeflate(c.DeflateLevel)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Snappy {
		err := c.upgradeSnappy()
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	// now that connection is bootstrapped, enable read buffering
	c.r = bufio.NewReader(c.r)

	return resp, nil
}

func (c *Conn) upgradeTLS(conf *tls.Config) error {
	c.tlsConn = tls.Client(c.Conn, conf)
	err := c.tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.r = c.tlsConn
	c.w = c.tlsConn
	frameType, data, err := c.ReadUnpackedResponse()
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from TLS upgrade")
	}
	return nil
}

func (c *Conn) upgradeDeflate(level int) error {
	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	c.r = flate.NewReader(conn)
	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.w = fw
	frameType, data, err := c.ReadUnpackedResponse()
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Deflate upgrade")
	}
	return nil
}

func (c *Conn) upgradeSnappy() error {
	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	c.r = snappystream.NewReader(conn, snappystream.SkipVerifyChecksum)
	c.w = snappystream.NewWriter(conn)
	frameType, data, err := c.ReadUnpackedResponse()
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Snappy upgrade")
	}
	return nil
}

func (c *Conn) readLoop() {
	for {
		if atomic.LoadInt32(&c.stopFlag) == 1 {
			goto exit
		}

		frameType, data, err := c.ReadUnpackedResponse()
		if err != nil {
			c.IOErrorCB(c, err)
			goto exit
		}

		if frameType == FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) {
			c.HeartbeatCB(c)
			err := c.SendCommand(Nop())
			if err != nil {
				c.IOErrorCB(c, err)
				goto exit
			}
			continue
		}

		switch frameType {
		case FrameTypeResponse:
			c.ResponseCB(c, data)
		case FrameTypeMessage:
			msg, err := DecodeMessage(data)
			if err != nil {
				c.IOErrorCB(c, err)
				goto exit
			}
			msg.cmdChan = c.cmdChan
			msg.responseChan = c.finishedMessages
			msg.exitChan = c.exitChan

			atomic.AddInt64(&c.rdyCount, -1)
			atomic.AddInt64(&c.messagesInFlight, 1)
			atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())

			c.MessageCB(c, msg)
		case FrameTypeError:
			c.ErrorCB(c, data)
		default:
			c.IOErrorCB(c, fmt.Errorf("unknown frame type %d", frameType))
		}
	}

exit:
	atomic.StoreInt32(&c.readLoopRunning, 0)
	// start the connection close
	messagesInFlight := atomic.LoadInt64(&c.messagesInFlight)
	if messagesInFlight == 0 {
		// if we exited readLoop with no messages in flight
		// we need to explicitly trigger the close because
		// writeLoop won't
		c.close()
	} else {
		log.Printf("[%s] delaying close, %d outstanding messages",
			c, messagesInFlight)
	}
	c.wg.Done()
	log.Printf("[%s] readLoop exiting", c)
}

func (c *Conn) writeLoop() {
	for {
		select {
		case <-c.exitChan:
			log.Printf("[%s] breaking out of writeLoop", c)
			// Indicate drainReady because we will not pull any more off finishedMessages
			close(c.drainReady)
			goto exit
		case cmd := <-c.cmdChan:
			err := c.SendCommand(cmd)
			if err != nil {
				log.Printf("[%s] error sending command %s - %s", c, cmd, err)
				c.close()
				continue
			}
		case finishedMsg := <-c.finishedMessages:
			// Decrement this here so it is correct even if we can't respond to nsqd
			msgsInFlight := atomic.AddInt64(&c.messagesInFlight, -1)

			if finishedMsg.Success {
				err := c.SendCommand(Finish(finishedMsg.Id))
				if err != nil {
					log.Printf("[%s] error finishing %s - %s", c, finishedMsg.Id, err.Error())
					c.close()
					continue
				}
			} else {
				err := c.SendCommand(Requeue(finishedMsg.Id, finishedMsg.RequeueDelayMs))
				if err != nil {
					log.Printf("[%s] error requeueing %s - %s", c, finishedMsg.Id, err.Error())
					c.close()
					continue
				}
			}

			c.MessageProcessedCB(c, finishedMsg)

			if msgsInFlight == 0 && atomic.LoadInt32(&c.stopFlag) == 1 {
				c.close()
				continue
			}
		}
	}

exit:
	c.wg.Done()
	log.Printf("[%s] writeLoop exiting", c)
}

func (c *Conn) close() {
	// a "clean" connection close is orchestrated as follows:
	//
	//     1. CLOSE cmd sent to nsqd
	//     2. CLOSE_WAIT response received from nsqd
	//     3. set c.stopFlag
	//     4. readLoop() exits
	//         a. if messages-in-flight > 0 delay close()
	//             i. writeLoop() continues receiving on c.finishedMessages chan
	//                 x. when messages-in-flight == 0 call close()
	//         b. else call close() immediately
	//     5. c.exitChan close
	//         a. writeLoop() exits
	//             i. c.drainReady close
	//     6a. launch cleanup() goroutine (we're racing with intraprocess
	//        routed messages, see comments below)
	//         a. wait on c.drainReady
	//         b. loop and receive on c.finishedMessages chan
	//            until messages-in-flight == 0
	//            i. ensure that readLoop has exited
	//     6b. launch waitForCleanup() goroutine
	//         b. wait on waitgroup (covers readLoop() and writeLoop()
	//            and cleanup goroutine)
	//         c. underlying TCP connection close
	//         d. trigger CloseCB()
	//
	c.stopper.Do(func() {
		log.Printf("[%s] beginning close", c)
		close(c.exitChan)

		c.wg.Add(1)
		go c.cleanup()

		go c.waitForCleanup()
	})
}

func (c *Conn) cleanup() {
	<-c.drainReady
	ticker := time.NewTicker(100 * time.Millisecond)
	// finishLoop has exited, drain any remaining in flight messages
	for {
		// we're racing with router which potentially has a message
		// for handling...
		//
		// infinitely loop until the connection's waitgroup is satisfied,
		// ensuring that both finishLoop and router have exited, at which
		// point we can be guaranteed that messagesInFlight accurately
		// represents whatever is left... continue until 0.
		var msgsInFlight int64
		select {
		case <-c.finishedMessages:
			msgsInFlight = atomic.AddInt64(&c.messagesInFlight, -1)
		case <-ticker.C:
			msgsInFlight = atomic.LoadInt64(&c.messagesInFlight)
		}
		if msgsInFlight > 0 {
			log.Printf("[%s] draining... waiting for %d messages in flight", c, msgsInFlight)
			continue
		}
		// until the readLoop has exited we cannot be sure that there
		// still won't be a race
		if atomic.LoadInt32(&c.readLoopRunning) == 1 {
			log.Printf("[%s] draining... readLoop still running", c)
			continue
		}
		goto exit
	}

exit:
	ticker.Stop()
	c.wg.Done()
	log.Printf("[%s] finished draining, cleanup exiting", c)
}

func (c *Conn) waitForCleanup() {
	// this blocks until readLoop and writeLoop
	// (and cleanup goroutine above) have exited
	c.wg.Wait()
	// actually close the underlying connection
	c.Close()
	log.Printf("[%s] clean close complete", c)
	c.CloseCB(c)
}
