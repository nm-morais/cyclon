package protocol

import (
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"sort"
	"time"

	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/protocolManager"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const (
	protoID = 1000
	name    = "cyclon"
)

type CyclonConfig struct {
	SelfPeer struct {
		AnalyticsPort int    `yaml:"analyticsPort"`
		Port          int    `yaml:"port"`
		Host          string `yaml:"host"`
	} `yaml:"self"`
	BootstrapPeers []struct {
		Port          int    `yaml:"port"`
		Host          string `yaml:"host"`
		AnalyticsPort int    `yaml:"analyticsPort"`
	} `yaml:"bootstrapPeers"`

	DialTimeoutMiliseconds int    `yaml:"dialTimeoutMiliseconds"`
	LogFolder              string `yaml:"logFolder"`
	CacheViewSize          int    `yaml:"cacheViewSize"`
	ShuffleTimeSeconds     int    `yaml:"shuffleTimerSeconds"`
	L                      int    `yaml:"l"`
}
type Cyclon struct {
	babel                  protocolManager.ProtocolManager
	logger                 *logrus.Logger
	conf                   *CyclonConfig
	selfIsBootstrap        bool
	bootstrapNodes         []peer.Peer
	cyclonView             *View
	pendingCyclonExchanges map[string][]*PeerState
}

func NewCyclonProtocol(babel protocolManager.ProtocolManager, conf *CyclonConfig) protocol.Protocol {
	logger := logs.NewLogger(name)
	selfIsBootstrap := false
	bootstrapNodes := []peer.Peer{}
	for _, p := range conf.BootstrapPeers {
		boostrapNode := peer.NewPeer(net.ParseIP(p.Host), uint16(p.Port), uint16(p.AnalyticsPort))
		bootstrapNodes = append(bootstrapNodes, boostrapNode)
		if peer.PeersEqual(babel.SelfPeer(), boostrapNode) {
			selfIsBootstrap = true
		}
	}

	logger.Infof("Starting with selfPeer:= %+v", babel.SelfPeer())
	logger.Infof("Starting with bootstraps:= %+v", bootstrapNodes)
	logger.Infof("Starting with selfIsBootstrap:= %+v", selfIsBootstrap)

	return &Cyclon{
		babel:                  babel,
		logger:                 logger,
		conf:                   conf,
		selfIsBootstrap:        selfIsBootstrap,
		bootstrapNodes:         bootstrapNodes,
		cyclonView:             &View{capacity: conf.CacheViewSize, asArr: []*PeerState{}, asMap: map[string]*PeerState{}},
		pendingCyclonExchanges: make(map[string][]*PeerState),
	}
}

func (c *Cyclon) ID() protocol.ID {
	return protoID
}

func (c *Cyclon) Name() string {
	return name
}

func (c *Cyclon) Logger() *logrus.Logger {
	return c.logger
}

func (c *Cyclon) Init() {
	c.babel.RegisterTimerHandler(protoID, DebugTimerID, c.HandleDebugTimer)
	// CYCLON
	c.babel.RegisterTimerHandler(protoID, ShuffleTimerID, c.HandleShuffleTimer)
	c.babel.RegisterMessageHandler(protoID, &ShuffleMessage{}, c.HandleShuffleMessage)
	c.babel.RegisterMessageHandler(protoID, &ShuffleMessageReply{}, c.HandleShuffleMessageReply)
}

func (c *Cyclon) Start() {
	c.logger.Infof("Starting with confs: %+v", c.conf)
	c.babel.RegisterPeriodicTimer(c.ID(), DebugTimer{duration: time.Duration(5 * time.Second)}, false)
	c.babel.RegisterPeriodicTimer(c.ID(), ShuffleTimer{duration: time.Duration(c.conf.ShuffleTimeSeconds) * time.Second}, true)
	for _, bootstrap := range c.bootstrapNodes {
		if peer.PeersEqual(bootstrap, c.babel.SelfPeer()) {
			continue
		}
		if c.cyclonView.isFull() {
			break
		}
		c.cyclonView.add(&PeerState{
			Peer: bootstrap,
			age:  0,
		}, false)
		return
	}
}

// ---------------- Cyclon----------------

func (c *Cyclon) HandleShuffleTimer(t timer.Timer) {
	if c.cyclonView.size() == 0 {
		if c.selfIsBootstrap {
			return
		}
		c.logger.Warn("Had no neighbors in shuffle reply, adding bootstrap peer")
		for _, bootstrap := range c.bootstrapNodes {
			if c.cyclonView.isFull() {
				break
			}
			c.cyclonView.add(&PeerState{
				Peer: bootstrap,
				age:  0,
			}, false)
		}
	}

	for _, p := range c.cyclonView.asArr {
		p.age++
	}
	viewAsArr := c.cyclonView.asArr
	sort.Sort(viewAsArr)
	q := viewAsArr[0]
	// c.logger.Infof("Oldest level peer: %s:%d", q.Peer.String(), q.age)
	delete(c.pendingCyclonExchanges, q.String())
	subset := append(c.cyclonView.getRandomElementsFromView(c.conf.L-1, q), &PeerState{
		Peer: c.babel.SelfPeer(),
		age:  0,
	})
	c.pendingCyclonExchanges[q.String()] = subset
	c.cyclonView.remove(q)
	toSend := NewShuffleMsg(subset)
	c.sendMessageTmpTransport(toSend, q)
	// c.logger.Infof("Sending shuffle message %+v to %s", toSend, q)
}

func (c *Cyclon) HandleShuffleMessage(sender peer.Peer, msg message.Message) {
	shuffleMsg := msg.(*ShuffleMessage)
	// c.logger.Infof("Received shuffle message %+v from %s", shuffleMsg, sender)
	peersToReply := c.cyclonView.getRandomElementsFromView(len(shuffleMsg.peers), shuffleMsg.peers...)
	toSend := NewShuffleMsgReply(peersToReply)
	c.sendMessageTmpTransport(toSend, sender)
	c.mergeCyclonViewWith(shuffleMsg.ToPeerStateArr(), peersToReply, sender)
}

func (c *Cyclon) HandleShuffleMessageReply(sender peer.Peer, msg message.Message) {
	shuffleMsgReply := msg.(*ShuffleMessageReply)
	// c.logger.Infof("Received shuffle reply message %+v from %s", shuffleMsgReply, sender)
	c.mergeCyclonViewWith(shuffleMsgReply.ToPeerStateArr(), c.pendingCyclonExchanges[sender.String()], sender)
	delete(c.pendingCyclonExchanges, sender.String())
}

func (c *Cyclon) mergeCyclonViewWith(sample []*PeerState, sentPeers []*PeerState, sender peer.Peer) {
	for _, p := range sample {
		if peer.PeersEqual(c.babel.SelfPeer(), p.Peer) {
			continue // discard all entries pointing to self
		}

		if c.cyclonView.contains(p) {
			continue
		}

		// if theere is space, just add
		if !c.cyclonView.isFull() {
			c.cyclonView.add(p, false)
			c.babel.SendNotification(NeighborUpNotification{
				PeerUp: p,
				View:   c.getView(),
			})
			continue
		}

		// attempt to drop sent peers, if there is any
		for len(sentPeers) > 0 {
			first := sentPeers[0]
			if c.cyclonView.contains(first) {
				c.cyclonView.remove(first)
				c.babel.SendNotification(NeighborDownNotification{
					PeerDown: first,
					View:     c.getView(),
				})

				c.cyclonView.add(p, false)
				c.babel.SendNotification(NeighborUpNotification{
					PeerUp: p,
					View:   c.getView(),
				})
				continue
			}
			sentPeers = sentPeers[1:]
		}
		if dropped := c.cyclonView.add(p, true); dropped != nil {
			c.babel.SendNotification(NeighborDownNotification{
				PeerDown: dropped.Peer,
				View:     c.getView(),
			})
		}
	}
}

func (c *Cyclon) logInView() {
	type viewWithLatencies []struct {
		IP      string `json:"ip,omitempty"`
		Latency int    `json:"latency,omitempty"`
	}
	toPrint := viewWithLatencies{}
	for _, p := range c.cyclonView.asArr {
		toPrint = append(toPrint, struct {
			IP      string "json:\"ip,omitempty\""
			Latency int    "json:\"latency,omitempty\""
		}{
			IP:      p.IP().String(),
			Latency: int(p.age),
		})
	}
	res, err := json.Marshal(toPrint)
	if err != nil {
		panic(err)
	}
	c.logger.Infof("<inView> %s", string(res))
}

func (c *Cyclon) getView() map[string]peer.Peer {
	toRet := map[string]peer.Peer{}
	for _, p := range c.cyclonView.asArr {
		toRet[p.String()] = p
	}
	return toRet
}

// ---------------- Networking Handlers ----------------

func (c *Cyclon) InConnRequested(dialerProto protocol.ID, p peer.Peer) bool {
	if dialerProto != c.ID() {
		c.logger.Warnf("Denying connection from peer %+v", p)
		return false
	}
	return false
}

func (c *Cyclon) OutConnDown(p peer.Peer) {
	c.logger.Errorf("Peer %s out connection went down", p.String())
}

func (c *Cyclon) DialFailed(p peer.Peer) {
	c.logger.Errorf("Failed to dial peer %s", p.String())
}

func (c *Cyclon) DialSuccess(sourceProto protocol.ID, p peer.Peer) bool {
	return false
}

func (c *Cyclon) MessageDelivered(msg message.Message, p peer.Peer) {
	c.logger.Infof("Message of type [%s] was delivered to %s", reflect.TypeOf(msg), p.String())
}

func (c *Cyclon) MessageDeliveryErr(msg message.Message, p peer.Peer, err errors.Error) {
	c.logger.Warnf("Message %s was not sent to %s because: %s", reflect.TypeOf(msg), p.String(), err.Reason())
}

// ---------------- Auxiliary functions ----------------

func (c *Cyclon) logCyclonState() {
	c.logger.Info("------------- Cyclon state -------------")
	var toLog string
	toLog = "Cyclon view : "

	for idx, p := range c.cyclonView.asArr {
		toLog += fmt.Sprintf("%s:%d", p.String(), p.age)
		if idx < len(c.cyclonView.asArr)-1 {
			toLog += ", "
		}
	}
	c.logger.Info(toLog)
}

func (c *Cyclon) sendMessageTmpTransport(msg message.Message, target peer.Peer) {
	c.babel.SendMessageSideStream(msg, target, target.ToUDPAddr(), c.ID(), c.ID())
}

func (c *Cyclon) HandleDebugTimer(t timer.Timer) {
	c.logCyclonState()
	c.logInView()
}
