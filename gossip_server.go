package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/r-moraru/sir-gossip/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type message struct {
	sender  string
	content string
}

type serverState struct {
	pb.UnimplementedGossipServer
	lock sync.Mutex

	gossipCycle           int
	serverId              string
	peers                 map[string]pb.GossipClient
	messages              map[string]message
	removed_state_message map[string]bool
}

func dissemination_loop(s *serverState, messageHash string) {
	_, has_message := s.messages[messageHash]
	_, is_removed := s.removed_state_message[messageHash]
	for has_message && !is_removed {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		updateMessage := pb.UpdateMessage{
			Sender:         new(string),
			MessageHash:    new(string),
			MessageSender:  new(string),
			MessageContent: new(string),
		}
		*updateMessage.Sender = s.serverId
		*updateMessage.MessageHash = messageHash
		*updateMessage.MessageSender = s.messages[messageHash].sender
		*updateMessage.MessageContent = s.messages[messageHash].content

		source := rand.NewSource(time.Now().UnixMicro())
		rng := rand.New(source)
		k := rng.Intn(len(s.peers))
		for _, peerConn := range s.peers {
			if k == 0 {
				_, err := peerConn.Update(ctx, &updateMessage)
				if err != nil {
					log.Printf("Failed to send feedback: %v\n", err)
				}
				break
			}
			k--
		}

		time.Sleep(time.Duration(s.gossipCycle) * time.Millisecond)
		_, has_message = s.messages[messageHash]
		_, is_removed = s.removed_state_message[messageHash]
	}
}

func sendFeedbackMessage(peerStub pb.GossipClient, messageHash string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	feedbackMessage := pb.FeedbackMessage{MessageHash: new(string)}
	*feedbackMessage.MessageHash = messageHash
	_, err := peerStub.Feedback(ctx, &feedbackMessage)
	if err != nil {
		log.Printf("Failed to send feedback: %v\n", err)
	}
}

func storeMessage(s *serverState, messageHash string, m message) {
	s.lock.Lock()
	s.messages[messageHash] = m
	s.lock.Unlock()
	dissemination_loop(s, messageHash)
}

func (s *serverState) Update(context context.Context, request *pb.UpdateMessage) (*pb.UpdateResponse, error) {
	response := new(pb.UpdateResponse)

	_, has_message := s.messages[request.GetMessageHash()]
	_, is_removed := s.removed_state_message[request.GetMessageHash()]
	if has_message || is_removed {
		sendFeedbackMessage(s.peers[request.GetSender()], request.GetMessageHash())
	} else {
		m := message{
			sender:  request.GetMessageSender(),
			content: request.GetMessageContent(),
		}
		storeMessage(s, request.GetMessageHash(), m)
	}

	return response, nil
}

func (s *serverState) Feedback(context context.Context, request *pb.FeedbackMessage) (*pb.FeedbackResponse, error) {
	response := new(pb.FeedbackResponse)

	source := rand.NewSource(time.Now().UnixMicro())
	rng := rand.New(source)
	if rng.Intn(len(s.peers)) == 0 {
		s.lock.Lock()
		s.removed_state_message[*request.MessageHash] = true
		s.lock.Unlock()
	}

	return response, nil
}

type Peer struct {
	Ip   string `json:"ip"`
	Port int    `json:"port"`
}

type ServerConfig struct {
	Ip    string `json:"ip"`
	Port  int    `json:"port"`
	Peers []Peer `json:"peers"`
}

func readServerConfig(configFileName string) (ServerConfig, error) {
	var serverConfig ServerConfig
	configFile, err := os.Open(configFileName)
	if err != nil {
		log.Fatalln("Unable to open config file.")
		return serverConfig, err
	}
	defer configFile.Close()
	byteValue, _ := io.ReadAll(configFile)
	json.Unmarshal(byteValue, &serverConfig)
	return serverConfig, nil
}

func main() {
	configFile := flag.String("config_file", "config.json", "")
	flag.Parse()

	serverConfig, err := readServerConfig(*configFile)
	if err != nil {
		return
	}

	lis, err := net.Listen("tcp", "localhost:"+strconv.FormatInt(int64(serverConfig.Port), 10))
	if err != nil {
		log.Fatalf("failed to listed: %v", err)
	}
	s := grpc.NewServer()
	sState := serverState{
		serverId:              fmt.Sprintf("%s:%d", serverConfig.Ip, serverConfig.Port),
		messages:              make(map[string]message),
		removed_state_message: make(map[string]bool),
		peers:                 make(map[string]pb.GossipClient),
	}
	pb.RegisterGossipServer(s, &sState)

	log.Printf("server listening at %v", lis.Addr())
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	for _, peer := range serverConfig.Peers {
		peerAddress := fmt.Sprintf("%s:%d", peer.Ip, peer.Port)
		conn, err := grpc.Dial(
			peerAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Unable to establish connection with peer: %v", err)
		}
		defer conn.Close()
		c := pb.NewGossipClient(conn)
		fmt.Printf("Added peer %s\n", peerAddress)
		sState.peers[peerAddress] = c
	}

	fmt.Println("Type help to get available commands.")
	var line string
	input := bufio.NewScanner(os.Stdin)

	fmt.Print(">> ")
	for input.Scan() {
		line = input.Text()
		splitLine := strings.Split(line, " ")
		if len(splitLine) == 1 {
			if splitLine[0] == "help" {
				fmt.Println("Available commands:\n" +
					"get-messages - print all messages\n" +
					"send-message <message> - send a message to the network\n" +
					"help - show available commands\n" +
					"quit - stop the program")
			} else if splitLine[0] == "send-message" {
				fmt.Println("message cannot be empty")
			} else if splitLine[0] == "quit" {
				break
			} else if splitLine[0] == "get-messages" {
				fmt.Println(sState.messages)
			} else {
				fmt.Println("Unrecognized command.")
			}
		} else {
			if splitLine[0] == "send-message" {
				messageContent := line[strings.Index(line, " "):]
				m := message{
					sender:  sState.serverId,
					content: messageContent,
				}
				h := sha256.New()
				h.Write([]byte(messageContent + sState.serverId))
				messageHash := fmt.Sprintf("%x", h.Sum(nil))
				storeMessage(&sState, messageHash, m)
			} else {
				fmt.Println("Unrecognized command.")
			}
		}
		fmt.Print(">> ")
	}
}
