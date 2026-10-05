package discovery

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync/atomic"
)

type Endpoint struct {
	Scheme string
	Host   string
	Port   int
}

func (endpoint Endpoint) Address() string {
	return net.JoinHostPort(endpoint.Host, fmt.Sprintf("%d", endpoint.Port))
}

type Resolver interface {
	Resolve(ctx context.Context, service string) ([]Endpoint, error)
}

type DNSResolver struct {
	Resolver *net.Resolver
	Scheme   string
	Port     int
}

func (resolver DNSResolver) Resolve(ctx context.Context, service string) ([]Endpoint, error) {
	if service == "" {
		return nil, fmt.Errorf("service name is required")
	}
	if resolver.Port < 1 {
		return nil, fmt.Errorf("service port must be positive")
	}
	lookup := resolver.Resolver
	if lookup == nil {
		lookup = net.DefaultResolver
	}
	hosts, err := lookup.LookupHost(ctx, service)
	if err != nil {
		return nil, fmt.Errorf("resolve service %q: %w", service, err)
	}
	sort.Strings(hosts)
	endpoints := make([]Endpoint, 0, len(hosts))
	for _, host := range hosts {
		endpoints = append(endpoints, Endpoint{Scheme: resolver.Scheme, Host: host, Port: resolver.Port})
	}
	return endpoints, nil
}

type StaticResolver struct {
	Endpoints []Endpoint
}

func (resolver StaticResolver) Resolve(ctx context.Context, service string) ([]Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if service == "" {
		return nil, fmt.Errorf("service name is required")
	}
	if len(resolver.Endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints configured for service %q", service)
	}
	endpoints := make([]Endpoint, len(resolver.Endpoints))
	copy(endpoints, resolver.Endpoints)
	return endpoints, nil
}

type RoundRobin struct {
	resolver Resolver
	index    atomic.Uint64
}

func NewRoundRobin(resolver Resolver) (*RoundRobin, error) {
	if resolver == nil {
		return nil, fmt.Errorf("resolver is required")
	}
	return &RoundRobin{resolver: resolver}, nil
}

func (roundRobin *RoundRobin) Next(ctx context.Context, service string) (Endpoint, error) {
	endpoints, err := roundRobin.resolver.Resolve(ctx, service)
	if err != nil {
		return Endpoint{}, err
	}
	if len(endpoints) == 0 {
		return Endpoint{}, fmt.Errorf("no endpoints resolved for service %q", service)
	}
	index := roundRobin.index.Add(1) - 1
	return endpoints[index%uint64(len(endpoints))], nil
}
