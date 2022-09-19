package kucoinapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
)

const NullPrice = "null"

type StreamTickerBranch struct {
	bid    tobBranch
	ask    tobBranch
	cancel *context.CancelFunc
	reCh   chan error
}

type tobBranch struct {
	mux   sync.RWMutex
	price string
	qty   string
}

type wS struct {
	Channel       string
	OnErr         bool
	ConnectID     string
	Logger        *log.Logger
	Conn          *websocket.Conn
	LastUpdatedId decimal.Decimal
}

type pingPong struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type subscribe struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Topic          string `json:"topic"`
	PrivateChannel bool   `json:"privateChannel"`
	Response       bool   `json:"response"`
}

// func SwapStreamTicker(symbol string, logger *log.Logger) *StreamTickerBranch {
// 	return localStreamTicker("swap", symbol, logger)
// }

// ex: symbol = btcusdt
func SpotStreamTicker(symbol string, logger *log.Logger) *StreamTickerBranch {
	return localStreamTicker("spot", symbol, logger)
}

func localStreamTicker(product, symbol string, logger *log.Logger) *StreamTickerBranch {
	var s StreamTickerBranch
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = &cancel
	ticker := make(chan map[string]interface{}, 50)
	errCh := make(chan error, 5)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := kucoinTickerSocket(ctx, product, symbol, "ticker", logger, &ticker, &errCh); err == nil {
					return
				} else {
					logger.Warningf("Reconnect %s %s ticker stream with err: %s\n", symbol, product, err.Error())
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.maintainStreamTicker(ctx, product, symbol, &ticker, &errCh); err == nil {
					return
				} else {
					logger.Warningf("Refreshing %s %s ticker stream with err: %s\n", symbol, product, err.Error())
				}
			}
		}
	}()
	return &s
}

func (s *StreamTickerBranch) Close() {
	(*s.cancel)()
	s.bid.mux.Lock()
	s.bid.price = NullPrice
	s.bid.mux.Unlock()
	s.ask.mux.Lock()
	s.ask.price = NullPrice
	s.ask.mux.Unlock()
}

func (s *StreamTickerBranch) GetBid() (price, qty string, st time.Time, ok bool) {
	s.bid.mux.RLock()
	defer s.bid.mux.RUnlock()
	price = s.bid.price
	qty = s.bid.qty
	if price == NullPrice || price == "" {
		return price, qty, time.Time{}, false
	}
	return price, qty, time.Now(), true
}

func (s *StreamTickerBranch) GetAsk() (price, qty string, st time.Time, ok bool) {
	s.ask.mux.RLock()
	defer s.ask.mux.RUnlock()
	price = s.ask.price
	qty = s.ask.qty
	if price == NullPrice || price == "" {
		return price, qty, time.Time{}, false
	}
	return price, qty, time.Now(), true
}

func (s *StreamTickerBranch) updateBidData(price, qty string) {
	s.bid.mux.Lock()
	defer s.bid.mux.Unlock()
	s.bid.price = price
	s.bid.qty = qty
}

func (s *StreamTickerBranch) updateAskData(price, qty string) {
	s.ask.mux.Lock()
	defer s.ask.mux.Unlock()
	s.ask.price = price
	s.ask.qty = qty
}

func (s *StreamTickerBranch) maintainStreamTicker(
	ctx context.Context,
	product, symbol string,
	ticker *chan map[string]interface{},
	errCh *chan error,
) error {
	lastUpdate := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-(*ticker):
			var bidPrice, askPrice, bidQty, askQty string
			if bid, ok := message["bestBid"].(string); ok {
				bidPrice = bid
			} else {
				bidPrice = NullPrice
			}
			if ask, ok := message["bestAsk"].(string); ok {
				askPrice = ask
			} else {
				askPrice = NullPrice
			}
			if bidqty, ok := message["bestBidSize"].(string); ok {
				bidQty = bidqty
			}
			if askqty, ok := message["bestAskSize"].(string); ok {
				askQty = askqty
			}
			s.updateBidData(bidPrice, bidQty)
			s.updateAskData(askPrice, askQty)
			lastUpdate = time.Now()
		default:
			if time.Now().After(lastUpdate.Add(time.Second * 60)) {
				// 60 sec without updating
				err := errors.New("reconnect because of time out")
				*errCh <- err
				return err
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func kucoinTickerSocket(
	ctx context.Context,
	product, symbol, channel string,
	logger *log.Logger,
	mainCh *chan map[string]interface{},
	errCh *chan error,
) error {
	var w wS
	var duration time.Duration = 30
	innerErr := make(chan error, 1)
	w.Logger = logger
	w.OnErr = false
	symbol = strings.ToUpper(symbol)
	var endpoint, token string
	switch product {
	case "spot":
		client := New("", "", "")
		if res, err := client.PublicToken(); err != nil {
			return err
		} else {
			endpoint = res.Data.Instanceservers[0].Endpoint
			token = res.Data.Token
		}
	case "swap":
		// pass
	default:
		return errors.New("not supported product, cancel socket connection")
	}
	//template => wss://push1-v2.kucoin.com/endpoint?token=xxx&[connectId=xxxxx]
	url := fmt.Sprintf("%s?token=%s&[connectId=5243]", endpoint, token)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	logger.Infof("KuCoin %s %s %s socket connected.\n", symbol, product, channel)
	w.Conn = conn
	defer conn.Close()
	err = w.subscribeTo(channel, symbol)
	if err != nil {
		return err
	}
	if err := w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
		return err
	}
	w.Conn.SetPingHandler(nil)
	go func() {
		PingManaging := time.NewTicker(time.Second * 30)
		defer PingManaging.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-innerErr:
				return
			case <-PingManaging.C:
				if err := w.sendPingPong(); err == nil {
					w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration))
				}
			default:
				time.Sleep(time.Second)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-*errCh:
			return err
		default:
			_, buf, err := conn.ReadMessage()
			if err != nil {
				d := w.outKuCoinErr()
				*mainCh <- d
				innerErr <- errors.New("restart")
				return err
			}
			res, err1 := decodingMap(buf, logger)
			if err1 != nil {
				d := w.outKuCoinErr()
				*mainCh <- d
				innerErr <- errors.New("restart")
				return err
			}
			err2 := w.handleKuCoinSocketData(res, mainCh)
			if err2 != nil {
				d := w.outKuCoinErr()
				*mainCh <- d
				innerErr <- errors.New("restart")
				return err2
			}
			if err := w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
				return err
			}
		}
	}
}

func decodingMap(message []byte, logger *log.Logger) (res map[string]interface{}, err error) {
	if message == nil {
		err = errors.New("the incoming message is nil")
		return nil, err
	}
	err = json.Unmarshal(message, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (w *wS) subscribeTo(channel, symbol string) error {
	var topic string
	switch channel {
	case "ticker":
		topic = fmt.Sprintf("/market/ticker:%s", symbol)
	default:
		return errors.New("un-support channel")
	}
	sub := subscribe{
		ID:             w.ConnectID,
		Type:           "subscribe",
		Topic:          topic,
		PrivateChannel: false,
		Response:       true,
	}
	message, err := json.Marshal(sub)
	if err != nil {
		return err
	}
	if err := w.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
		return err
	}
	return nil
}

func (w *wS) outKuCoinErr() map[string]interface{} {
	w.OnErr = true
	m := make(map[string]interface{})
	return m
}

func formatingTimeStamp(timeFloat float64) time.Time {
	t := time.Unix(int64(timeFloat/1000), 0)
	return t
}

func (w *wS) handleKuCoinSocketData(res map[string]interface{}, mainCh *chan map[string]interface{}) error {
	typ, ok := res["type"].(string)
	if !ok {
		return errors.New("get nil type from an event")
	}

	switch typ {
	case "welcome":
		if id, ok := res["id"].(string); !ok {
			return errors.New("get nil id from an event")
		} else {
			w.ConnectID = id
		}
	case "pong":
		//
	case "message":
		data, ok := res["data"].(map[string]interface{})
		if !ok {
			return nil
		}
		if st, ok := data["time"].(float64); !ok {
			m := w.outKuCoinErr()
			*mainCh <- m
			return errors.New("got nil when updating event time")
		} else {
			stamp := time.Unix(int64(st), 0)
			if time.Now().After(stamp.Add(time.Second * 5)) {
				m := w.outKuCoinErr()
				*mainCh <- m
				return errors.New("websocket data delay more than 5 sec")
			}
		}
		*mainCh <- data
	}
	return nil
}

func (w *wS) sendPingPong() error {
	ping := pingPong{ID: w.ConnectID, Type: "ping"}
	message, err := json.Marshal(ping)
	if err != nil {
		return err
	}
	if err := w.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
		w.Conn.SetReadDeadline(time.Now().Add(time.Second))
		return err
	}
	return nil
}
