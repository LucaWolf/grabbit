package grabbit

import (
	"testing"
)

func TestTopologyOptions_GetRouting(t *testing.T) {
	tests := []struct {
		name            string
		opt             *TopologyOptions
		wantSource      string
		wantDestination string
	}{
		{
			"queue_from_exchange",
			&TopologyOptions{
				IsExchange:    false,
				IsDestination: true,
				Name:          "sell",
				Bind: TopologyBind{
					Peer: "trade",
				},
			},
			"trade",
			"sell",
		},
		{
			"exchange_to_queue",
			&TopologyOptions{
				IsExchange:    true,
				IsDestination: false,
				Name:          "trade",
				Bind: TopologyBind{
					Peer: "buy",
				},
			},
			"trade",
			"buy",
		},
		{
			"exchanges_forward",
			&TopologyOptions{
				IsExchange:    true,
				IsDestination: true,
				Name:          "A",
				Bind: TopologyBind{
					Peer: "B",
				},
			},
			"B",
			"A",
		},
		{
			"exchanges_backward",
			&TopologyOptions{
				IsExchange:    true,
				IsDestination: false,
				Name:          "A",
				Bind: TopologyBind{
					Peer: "B",
				},
			},
			"A",
			"B",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSource, gotDestination := tt.opt.GetRouting()
			if gotSource != tt.wantSource {
				t.Errorf("TopologyOptions.GetRouting() gotSource = %v, want %v", gotSource, tt.wantSource)
			}
			if gotDestination != tt.wantDestination {
				t.Errorf("TopologyOptions.GetRouting() gotDestination = %v, want %v", gotDestination, tt.wantDestination)
			}
		})
	}
}
