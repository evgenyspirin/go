package main

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

//	Conditions:
//	1. Global vars are prohibited
//	2. Implementation in one file

type key int

const (
	ConnectionTimeKey key = 1
	MethodCallTimeKey key = 2

	BizService   = "/main.Biz/"
	AdminService = "/main.Admin/"
	LogMethod    = AdminService + "Logging"
	StatMethod   = AdminService + "Statistics"

	AccessDenied = "access denied"
)

func StartMyMicroservice(ctx context.Context, listenAddr string, ACLData string) (err error) {
	aclRaw := map[string]interface{}{}
	if err = json.NewDecoder(strings.NewReader(ACLData)).Decode(&aclRaw); err != nil {
		log.Println("JSON: Decode() err: " + err.Error())
		return
	}

	go func() {
		var (
			grpcServer *grpc.Server
			host       string
		)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		ed := &EventDispatcher{
			EventCh:       make(chan *Event, 1),
			LogListeners:  make(map[int64]chan *Event),
			StatListeners: make(map[int64]*Stat),
		}
		ed.Notify(ctx)

		addrPort := strings.SplitAfter(listenAddr, ":")
		if len(addrPort) > 1 {
			host = addrPort[0]
		}

		cancelSignal := make(chan os.Signal, 1)
		signal.Notify(cancelSignal, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)
		defer signal.Stop(cancelSignal)

		g, ctx := errgroup.WithContext(ctx)

		g.Go(func() error {
			ln, err := net.Listen("tcp", listenAddr)
			if err != nil {
				log.Println("gRPC server: failed to listen error:", err)
				os.Exit(2)
			}

			grpcServer = grpc.NewServer(
				grpc.InTapHandle(inTapHandler),
				grpc.UnaryInterceptor(unaryInterceptor),
				grpc.StreamInterceptor(streamInterceptor))

			RegisterBizServer(grpcServer, NewBiz(getAclByService(aclRaw, BizService), ed, host))
			RegisterAdminServer(grpcServer, NewAdmin(getAclByService(aclRaw, AdminService), ed, host))

			log.Println(fmt.Sprintf("gRPC server serving at %s", listenAddr))

			return grpcServer.Serve(ln)
		})

		select {
		case <-cancelSignal:
			break
		case <-ctx.Done():
			break
		}

		log.Println("Received shutdown signal")

		if grpcServer != nil {
			grpcServer.GracefulStop()
		}

		if err = g.Wait(); err != nil {
			log.Println("errgroup error: ", err)
			os.Exit(2)
		}
	}()

	return
}

type EventDispatcher struct {
	EventCh       chan *Event
	LogListeners  map[int64]chan *Event
	StatListeners map[int64]*Stat
	sync.RWMutex
}

func (ld *EventDispatcher) Notify(ctx context.Context) {
	go func() {
	mainLoop:
		for {
			select {
			case <-ctx.Done():
				break mainLoop
			case e := <-ld.EventCh:
				ld.RLock()
				if len(ld.LogListeners) == 0 && len(ld.StatListeners) == 0 {
					ld.RUnlock()
					continue
				}

				for connIdTimestamp, eCh := range ld.LogListeners {
					if e.Timestamp <= connIdTimestamp || len(eCh) == cap(eCh) {
						continue
					}

					eCh <- e
				}
				ld.RUnlock()

				ld.Lock()
				for connIdTimestamp, s := range ld.StatListeners {
					if e.Timestamp <= connIdTimestamp {
						continue
					}
					s.ByMethod[e.Method]++
					s.ByConsumer[e.Consumer]++
				}
				ld.Unlock()
			}
		}
	}()
}

func (ld *EventDispatcher) Log(ctx context.Context, srv interface{}, consumer string, fullMethod string) error {
	switch t := srv.(type) {
	case *Biz:
		ld.EventCh <- &Event{
			Timestamp: ctx.Value(MethodCallTimeKey).(int64),
			Consumer:  consumer,
			Method:    fullMethod,
			Host:      t.Host}
	case *Admin:
		switch fullMethod {
		case LogMethod:
			ld.Lock()
			ld.LogListeners[ctx.Value(ConnectionTimeKey).(int64)] = make(chan *Event, 1)
			ld.Unlock()
		case StatMethod:
			ld.Lock()
			ld.StatListeners[ctx.Value(ConnectionTimeKey).(int64)] = &Stat{
				ByMethod: map[string]uint64{}, ByConsumer: map[string]uint64{},
			}
			ld.Unlock()
		}

		ld.EventCh <- &Event{
			Timestamp: ctx.Value(MethodCallTimeKey).(int64),
			Consumer:  consumer,
			Method:    fullMethod,
			Host:      t.Host}
	default:
		return fmt.Errorf("unable to cast server")
	}

	return nil
}

func getAclByService(aclRaw map[string]interface{}, service string) map[string][]string {
	aclByService := map[string][]string{}

mainLoop:
	for usr, methods := range aclRaw {
		tSLice, ok := methods.([]interface{})
		if !ok {
			continue
		}

		var allowedMethods []string
		for _, method := range tSLice {
			tString, ok := method.(string)
			if !ok || !strings.Contains(tString, service) {
				continue mainLoop
			}
			allowedMethods = append(allowedMethods, tString)
		}
		aclByService[usr] = allowedMethods
	}

	return aclByService
}

func inTapHandler(ctx context.Context, info *tap.Info) (context.Context, error) {
	// Here, we can easily choose the business-logic of what do we want to log first.
	ctx = context.WithValue(ctx, MethodCallTimeKey, time.Now().UnixNano())
	ctx = context.WithValue(ctx, ConnectionTimeKey, time.Now().UnixNano())

	return ctx, nil
}

func unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	c := md.Get("consumer")
	if c == nil {
		return nil, status.Error(codes.Unauthenticated, "please enter a consumer")
	}

	if err := handleMiddleware(ctx, info.Server, c[0], info.FullMethod); err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func streamInterceptor(srv interface{},
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler) (err error) {
	md, _ := metadata.FromIncomingContext(stream.Context())

	c := md.Get("consumer")
	if c == nil {
		return status.Error(codes.Unauthenticated, "please enter a consumer")
	}

	if err = handleMiddleware(stream.Context(), srv, c[0], info.FullMethod); err != nil {
		return
	}

	return handler(srv, stream)
}

func handleMiddleware(ctx context.Context, srv interface{}, consumer string, fullMethod string) error {
	switch t := srv.(type) {
	case *Biz:
		if err := t.ED.Log(ctx, srv, consumer, fullMethod); err != nil {
			return err
		}
		if err := checkAuth(t.ACL, consumer, fullMethod, BizService+"*"); err != nil {
			return err
		}
	case *Admin:
		if err := t.ED.Log(ctx, srv, consumer, fullMethod); err != nil {
			return err
		}
		if err := checkAuth(t.ACL, consumer, fullMethod, AdminService+"*"); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unable to cast server")
	}

	return nil
}

func checkAuth(acl map[string][]string, consumer string, fullMethod string, fullAccess string) error {
	permittedMethods, ok := acl[consumer]
	if !ok {
		return status.Error(codes.Unauthenticated, AccessDenied)
	}

	if !func() bool {
		for _, m := range permittedMethods {
			if m == fullMethod || m == fullAccess {
				return true
			}
		}
		return false
	}() {
		return status.Error(codes.Unauthenticated, AccessDenied)
	}

	return nil
}

// Microservices

type Biz struct {
	ACL  map[string][]string
	ED   *EventDispatcher
	Host string
}

func (b *Biz) Check(ctx context.Context, dummy *Nothing) (*Nothing, error) { return dummy, nil }
func (b *Biz) Add(ctx context.Context, dummy *Nothing) (*Nothing, error)   { return dummy, nil }
func (b *Biz) Test(ctx context.Context, dummy *Nothing) (*Nothing, error)  { return dummy, nil }

func NewBiz(acl map[string][]string, ed *EventDispatcher, host string) *Biz {
	return &Biz{ACL: acl, ED: ed, Host: host}
}

type Admin struct {
	ACL  map[string][]string
	ED   *EventDispatcher
	Host string
}

func (a *Admin) Logging(dummy *Nothing, als Admin_LoggingServer) error {
	ctx := als.Context()
	k := ctx.Value(ConnectionTimeKey).(int64)

	a.ED.RLock()
	lCh := a.ED.LogListeners[k]
	a.ED.RUnlock()

mainLoop:
	for {
		select {
		case <-ctx.Done():
			break mainLoop
		case e := <-lCh:
			if err := als.Send(e); err != nil {
				return err
			}
		}
	}

	a.ED.Lock()
	delete(a.ED.LogListeners, k)
	a.ED.Unlock()

	return nil
}

func (a *Admin) Statistics(si *StatInterval, ass Admin_StatisticsServer) error {
	ctx := ass.Context()
	t := time.NewTicker(time.Duration(si.IntervalSeconds) * time.Second)
	k := ctx.Value(ConnectionTimeKey).(int64)

mainLoop:
	for {
		select {
		case <-ctx.Done():
			t.Stop()
			break mainLoop
		case <-t.C:
			a.ED.RLock()
			out := a.ED.StatListeners[k]
			a.ED.RUnlock()

			if err := ass.Send(out); err != nil {
				return err
			}

			a.ED.Lock()
			a.ED.StatListeners[k] = &Stat{
				ByMethod:   map[string]uint64{},
				ByConsumer: map[string]uint64{},
			}
			a.ED.Unlock()
		}
	}

	a.ED.Lock()
	delete(a.ED.StatListeners, k)
	a.ED.Unlock()

	return nil
}

func NewAdmin(acl map[string][]string, ed *EventDispatcher, host string) *Admin {
	return &Admin{ACL: acl, ED: ed, Host: host}
}
