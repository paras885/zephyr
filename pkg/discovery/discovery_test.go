package discovery

import (
	"context"
	"testing"
)

func TestRoundRobinSelectsResolvedEndpoints(t *testing.T) {
	resolver, err := NewRoundRobin(StaticResolver{Endpoints: []Endpoint{
		{Scheme: "http", Host: "engine-a", Port: 8080},
		{Scheme: "http", Host: "engine-b", Port: 8080},
	}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := resolver.Next(context.Background(), "zephyr-engine")
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolver.Next(context.Background(), "zephyr-engine")
	if err != nil {
		t.Fatal(err)
	}
	if first.Host != "engine-a" || second.Host != "engine-b" {
		t.Fatalf("endpoints = %s, %s; want engine-a, engine-b", first.Host, second.Host)
	}
	if first.Address() != "engine-a:8080" {
		t.Fatalf("address = %q, want engine-a:8080", first.Address())
	}
}

func TestStaticResolverHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (StaticResolver{Endpoints: []Endpoint{{Host: "engine-a", Port: 8080}}}).Resolve(ctx, "zephyr-engine")
	if err == nil {
		t.Fatal("Resolve accepted a cancelled context")
	}
}
