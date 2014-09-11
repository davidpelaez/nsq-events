package nsq

import (
	"crypto/tls"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Writer is a high-level type to publish to NSQ.
//
// A Writer instance is 1:1 with a destination `nsqd`
// and will lazily connect to that instance (and re-connect)
// when Publish commands are executed.
type Writer struct {
	Addr string
	conn *Conn

	responseChan  chan []byte
	errorChan     chan []byte
	ioErrorChan   chan error
	heartbeatChan chan int
	closeChan     chan int

	// network deadlines
	ReadTimeout  time.Duration // the deadline set for network reads
	WriteTimeout time.Duration // the deadline set for network writes

	ShortIdentifier string // an identifier to send to nsqd when connecting (defaults: short hostname)
	LongIdentifier  string // an identifier to send to nsqd when connecting (defaults: long hostname)

	HeartbeatInterval time.Duration // duration of time between heartbeats
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

	concurrentWriters int32

	transactionChan chan *WriterTransaction
	transactions    []*WriterTransaction
	state           int32
	stopFlag        int32
	exitChan        chan int
	wg              sync.WaitGroup
}

// WriterTransaction is returned by the async publish methods
// to retrieve metadata about the command after the
// response is received.
type WriterTransaction struct {
	cmd       *Command
	doneChan  chan *WriterTransaction
	FrameType int32         // the frame type received in response to the publish command
	Data      []byte        // the response data of the publish command
	Error     error         // the error (or nil) of the publish command
	Args      []interface{} // the slice of variadic arguments passed to PublishAsync or MultiPublishAsync
}

func (t *WriterTransaction) finish() {
	if t.doneChan != nil {
		t.doneChan <- t
	}
}

// returned when a publish command is made against a Writer that is not connected
var ErrNotConnected = errors.New("not connected")

// returned when a publish command is made against a Writer that has been stopped
var ErrStopped = errors.New("stopped")

// NewWriter returns an instance of Writer for the specified address
func NewWriter(addr string) *Writer {
	return &Writer{
		Addr: addr,

		transactionChan: make(chan *WriterTransaction),
		exitChan:        make(chan int),
		responseChan:    make(chan []byte),
		errorChan:       make(chan []byte),
		ioErrorChan:     make(chan error),
		heartbeatChan:   make(chan int),
		closeChan:       make(chan int),
	}
}

// String returns the address of the Writer
func (w *Writer) String() string {
	return w.Addr
}

// Stop disconnects and permanently stops the Writer
func (w *Writer) Stop() {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return
	}
	close(w.exitChan)
	w.close()
	w.wg.Wait()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Writer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `WriterTransaction` instance with the supplied variadic arguments
// (and the response `FrameType`, `Data`, and `Error`)
func (w *Writer) PublishAsync(topic string, body []byte, doneChan chan *WriterTransaction,
	args ...interface{}) error {
	return w.sendCommandAsync(Publish(topic, body), doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
//
// When the Writer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `WriterTransaction` instance with the supplied variadic arguments
// (and the response `FrameType`, `Data`, and `Error`)
func (w *Writer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *WriterTransaction,
	args ...interface{}) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommandAsync(cmd, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// the response frameType, data, and error
func (w *Writer) Publish(topic string, body []byte) (int32, []byte, error) {
	return w.sendCommand(Publish(topic, body))
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// the response frameType, data, and error
func (w *Writer) MultiPublish(topic string, body [][]byte) (int32, []byte, error) {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return -1, nil, err
	}
	return w.sendCommand(cmd)
}

func (w *Writer) sendCommand(cmd *Command) (int32, []byte, error) {
	doneChan := make(chan *WriterTransaction)
	err := w.sendCommandAsync(cmd, doneChan, nil)
	if err != nil {
		close(doneChan)
		return -1, nil, err
	}
	t := <-doneChan
	return t.FrameType, t.Data, t.Error
}

func (w *Writer) sendCommandAsync(cmd *Command, doneChan chan *WriterTransaction,
	args []interface{}) error {
	// keep track of how many outstanding writers we're dealing with
	// in order to later ensure that we clean them all up...
	atomic.AddInt32(&w.concurrentWriters, 1)
	defer atomic.AddInt32(&w.concurrentWriters, -1)

	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}

	t := &WriterTransaction{
		cmd:       cmd,
		doneChan:  doneChan,
		FrameType: -1,
		Args:      args,
	}

	select {
	case w.transactionChan <- t:
	case <-w.exitChan:
		return ErrStopped
	}

	return nil
}

func (w *Writer) connect() error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return ErrStopped
	}

	if !atomic.CompareAndSwapInt32(&w.state, StateInit, StateConnected) {
		return ErrNotConnected
	}

	log.Printf("[%s] connecting...", w)

	conn := NewConn(w.Addr, "", "")
	if w.ReadTimeout > 0 {
		conn.ReadTimeout = w.ReadTimeout
	}
	if w.WriteTimeout > 0 {
		conn.WriteTimeout = w.WriteTimeout
	}
	conn.Deflate = w.Deflate
	if w.DeflateLevel > 0 {
		conn.DeflateLevel = w.DeflateLevel
	}
	conn.Snappy = w.Snappy
	conn.TLSv1 = w.TLSv1
	conn.TLSConfig = w.TLSConfig
	if w.ShortIdentifier != "" {
		conn.ShortIdentifier = w.ShortIdentifier
	}
	if w.LongIdentifier != "" {
		conn.LongIdentifier = w.LongIdentifier
	}
	if w.HeartbeatInterval != 0 {
		conn.HeartbeatInterval = w.HeartbeatInterval
	}
	if w.OutputBufferSize != 0 {
		conn.OutputBufferSize = w.OutputBufferSize
	}
	if w.OutputBufferTimeout != 0 {
		conn.OutputBufferTimeout = w.OutputBufferTimeout
	}
	if w.UserAgent != "" {
		conn.UserAgent = w.UserAgent
	}

	conn.ResponseCB = func(c *Conn, data []byte) {
		w.responseChan <- data
	}

	conn.ErrorCB = func(c *Conn, data []byte) {
		w.errorChan <- data
	}

	conn.HeartbeatCB = func(c *Conn) {
		w.heartbeatChan <- 1
	}

	conn.IOErrorCB = func(c *Conn, err error) {
		w.ioErrorChan <- err
	}

	conn.CloseCB = func(c *Conn) {
		w.closeChan <- 1
	}

	resp, err := conn.Connect()
	if err != nil {
		conn.Close()
		log.Printf("ERROR: [%s] failed to IDENTIFY - %s", w, err)
		atomic.StoreInt32(&w.state, StateInit)
		return err
	}

	if resp != nil {
		log.Printf("[%s] IDENTIFY response: %+v", w, resp)
		if resp.TLSv1 {
			log.Printf("[%s] upgrading to TLS", w)
		}
		if resp.Deflate {
			log.Printf("[%s] upgrading to Deflate", w)
		}
		if resp.Snappy {
			log.Printf("[%s] upgrading to Snappy", w)
		}
	}

	w.conn = conn

	w.wg.Add(1)
	go w.router()

	return nil
}

func (w *Writer) close() {
	if !atomic.CompareAndSwapInt32(&w.state, StateConnected, StateDisconnected) {
		return
	}
	w.conn.Close()
	go func() {
		// we need to handle this in a goroutine so we don't
		// block the caller from making progress
		w.wg.Wait()
		atomic.StoreInt32(&w.state, StateInit)
	}()
}

func (w *Writer) router() {
	for {
		select {
		case t := <-w.transactionChan:
			w.transactions = append(w.transactions, t)
			err := w.conn.SendCommand(t.cmd)
			if err != nil {
				log.Printf("ERROR: [%s] failed writing %s", w, err)
				w.close()
			}
		case data := <-w.responseChan:
			w.popTransaction(FrameTypeResponse, data)
		case data := <-w.errorChan:
			w.popTransaction(FrameTypeError, data)
		case <-w.heartbeatChan:
			log.Printf("[%s] heartbeat received", w)
		case err := <-w.ioErrorChan:
			log.Printf("ERROR: [%s] %s", w, err)
			w.close()
		case <-w.closeChan:
			goto exit
		case <-w.exitChan:
			goto exit
		}
	}

exit:
	w.transactionCleanup()
	w.wg.Done()
	log.Printf("[%s] exiting messageRouter()", w)
}

func (w *Writer) popTransaction(frameType int32, data []byte) {
	t := w.transactions[0]
	w.transactions = w.transactions[1:]
	t.FrameType = frameType
	t.Data = data
	t.Error = nil
	t.finish()
}

func (w *Writer) transactionCleanup() {
	// clean up transactions we can easily account for
	for _, t := range w.transactions {
		t.Error = ErrNotConnected
		t.finish()
	}
	w.transactions = w.transactions[:0]

	// spin and free up any writes that might have raced
	// with the cleanup process (blocked on writing
	// to transactionChan)
	for {
		select {
		case t := <-w.transactionChan:
			t.Error = ErrNotConnected
			t.finish()
		default:
			// keep spinning until there are 0 concurrent writers
			if atomic.LoadInt32(&w.concurrentWriters) == 0 {
				return
			}
			// give the runtime a chance to schedule other racing goroutines
			time.Sleep(5 * time.Millisecond)
			continue
		}
	}
}
