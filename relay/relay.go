package relay

import (
	"fmt"
	"log"
	"sync"
)

type Service struct {
	relays map[string]Relay
}

func New(config Config) (*Service, error) {
	s := new(Service)
	s.relays = make(map[string]Relay)

	for _, cfg := range config.HTTPRelays {
		h, err := NewHTTP(cfg)
		if err != nil {
			return nil, err
		}
		if s.relays[h.Name()] != nil {
			return nil, fmt.Errorf("duplicate relay: %q", h.Name())
		}
		s.relays[h.Name()] = h
	}

	for _, cfg := range config.UDPRelays {
		u, err := NewUDP(cfg)
		if err != nil {
			return nil, err
		}
		if s.relays[u.Name()] != nil {
			return nil, fmt.Errorf("duplicate relay: %q", u.Name())
		}
		s.relays[u.Name()] = u
	}

	return s, nil
}

func (s *Service) Run() {
	var wg sync.WaitGroup
	wg.Add(len(s.relays))

	for k := range s.relays {
		relay := s.relays[k]
		go func() {
			defer wg.Done()

			if err := relay.Run(); err != nil {
				log.Printf("Error running relay %q: %v", relay.Name(), err)
			}
		}()
	}

	wg.Wait()
}

func (s *Service) Stop() {
	for name, relay := range s.relays {
		if err := relay.Stop(); err != nil {
			log.Printf("停止 relay %q 失败: %v", name, err)
		}
	}
}

type Relay interface {
	Name() string
	Run() error
	Stop() error
}
