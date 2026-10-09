package transporttest

import (
	"context"

	"github.com/GRIDAPPSD/gridappsd-go/internal/reqresp"
	"github.com/GRIDAPPSD/gridappsd-go/message"
	"github.com/GRIDAPPSD/gridappsd-go/transport"
)

// ReplyBus answers one GetResponse over a FakeConn with a canned reply,
// running the real request/reply path so the recorded SEND is what a
// connected bus would put on the wire. It serves a single request.
type ReplyBus struct {
	Conn  *FakeConn
	reply []byte
}

// NewReplyBus returns a ReplyBus whose one reply is reply.
func NewReplyBus(reply []byte) *ReplyBus {
	conn := NewFakeConn()
	conn.SendNotify = make(chan struct{})
	return &ReplyBus{Conn: conn, reply: reply}
}

// GetResponse sends body to destination and delivers the canned reply on the
// reply destination the request names.
func (b *ReplyBus) GetResponse(ctx context.Context, destination, contentType string, body []byte) ([]byte, error) {
	go func() {
		select {
		case <-b.Conn.SendNotify:
		case <-ctx.Done():
			return
		}
		last, err := b.Conn.LastSend()
		if err != nil {
			return
		}
		if sub := b.Conn.SubForDest("/queue/" + last.Headers[message.HeaderReplyTo]); sub != nil {
			sub.Push(transport.Msg{Body: b.reply})
		}
	}()
	return reqresp.GetResponse(ctx, b.Conn, destination, contentType, body, nil)
}
