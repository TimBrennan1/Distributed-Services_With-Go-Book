package server

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/TimBrennan1/proglog/api/v1"
	"github.com/TimBrennan1/proglog/internal/log"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client api.LogClient, config *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})

	}
}

func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	//automatically assigned a free port when using :0
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	cLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{CommitLog: cLog}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGrpcServer(cfg)
	require.NoError(t, err)

	// serve is blocking, so we put in go routine
	// if not in go routine, out tests further down would not run
	go func() {
		server.Serve(l)
	}()

	//The client is the thing that allows us to make the GRPC connection
	return api.NewLogClient(cc), cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		cLog.Remove()
	}

}
func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	// empty context, that we use as mock
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)

	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	// empty context, that we use as mock
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{Value: []byte("hello world")},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	require.Nil(t, consume)

	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	require.Equal(t, got, want)
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	// empty context, that we use as mock
	ctx := context.Background()

	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}

	{
		// {}: tests produce stream

		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Offset, uint64(offset))
		}
	}

	{
		// {}: tests consume stream
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value: record.Value, Offset: uint64(i),
			})
		}
	}
}