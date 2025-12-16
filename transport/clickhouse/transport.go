package clickhouse

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"flag"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/netsampler/goflow2/v2/transport"
	"google.golang.org/protobuf/encoding/protodelim"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	flowpb "github.com/netsampler/goflow2/v2/pb"
)

//go:embed init.sql
var initSql string

type ClickhouseDriver struct {
	dsn string

	batchSize    int
	batchMaxTime int
	maxWorkers   int

	connection driver.Conn

	flows      chan *flowpb.FlowMessage
	flowsBatch chan []*Flow
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func (d *ClickhouseDriver) Prepare() error {
	flag.StringVar(&d.dsn, "transport.clickhouse.dsn", "clickhouse://127.0.0.1:9000/default", "ClickHouse connection string")
	flag.IntVar(&d.batchSize, "transport.clickhouse.batchsize", 10000, "Batch size")
	flag.IntVar(&d.batchMaxTime, "transport.clickhouse.batchmaxtime", 10, "Max time in seconds to wait for a batch to be filled")
	flag.IntVar(&d.maxWorkers, "transport.clickhouse.workers", 8, "Max number of pushing data workers")

	return nil
}

func (d *ClickhouseDriver) collectFlowBatch() {
	defer d.wg.Done()
	defer close(d.flowsBatch)

	batchMaxTime := time.Duration(d.batchMaxTime) * time.Second

	for {
		slog.Debug("start collecting flows")

		flowsBatch := make([]*Flow, 0, d.batchSize)

		t := time.NewTimer(batchMaxTime)

	inner:
		for len(flowsBatch) < d.batchSize {
			select {
			case <-d.ctx.Done():
				t.Stop()
				slog.Info("stopping collecting")
				return
			case <-t.C:
				break inner
			case msg := <-d.flows:
				flowsBatch = append(flowsBatch, &Flow{
					Type:                int32(msg.Type),
					TimeReceived:        renderTimestamp(msg.TimeReceivedNs),
					SequenceNum:         msg.SequenceNum,
					SamplingRate:        msg.SamplingRate,
					SamplerAddress:      renderIP(msg.SamplerAddress),
					TimeFlowStart:       renderTimestamp(msg.TimeFlowStartNs),
					TimeFlowEnd:         renderTimestamp(msg.TimeFlowEndNs),
					Bytes:               msg.Bytes,
					Packets:             msg.Packets,
					SrcAddr:             renderIP(msg.SrcAddr),
					DstAddr:             renderIP(msg.DstAddr),
					Etype:               uint16(msg.Etype),
					Proto:               uint8(msg.Proto),
					SrcPort:             uint16(msg.SrcPort),
					DstPort:             uint16(msg.DstPort),
					InIf:                msg.InIf,
					OutIf:               msg.OutIf,
					SrcMac:              renderMac(msg.SrcMac),
					DstMac:              renderMac(msg.DstMac),
					ForwardingStatus:    msg.ForwardingStatus,
					TcpFlags:            uint16(msg.TcpFlags),
					IcmpType:            uint16(msg.IcmpType),
					IcmpCode:            uint16(msg.IcmpCode),
					FragmentId:          msg.FragmentId,
					FragmentOffset:      msg.FragmentOffset,
					SrcAs:               msg.SrcAs,
					DstAs:               msg.DstAs,
					SrcNet:              uint8(msg.SrcNet),
					DstNet:              uint8(msg.DstNet),
					NextHop:             renderIP(msg.NextHop),
					NextHopAs:           msg.NextHopAs,
					BgpNextHop:          renderIP(msg.BgpNextHop),
					ObservationDomainId: msg.ObservationDomainId,
					ObservationPointId:  msg.ObservationPointId,
				})
			}
		}

		t.Stop()

		slog.Debug("collected flows", slog.Int("batch", len(flowsBatch)))

		if len(flowsBatch) > 0 {
			d.flowsBatch <- flowsBatch
		}
	}
}

func (d *ClickhouseDriver) pushFlows(workerID int) {
	defer d.wg.Done()

	slog.Info("push flows worker started", slog.Int("worker_id", workerID))

	for flowsBatch := range d.flowsBatch {
		now := time.Now()

		batch, err := d.connection.PrepareBatch(d.ctx, "INSERT INTO flows_sink")
		if err != nil {
			slog.Error("failed to prepare batch", slog.Int("worker_id", workerID), slog.String("error", err.Error()))
			continue
		}

		var lastTimeReceived time.Time

		for _, flow := range flowsBatch {
			err := batch.AppendStruct(flow)
			if err != nil {
				slog.Error("failed to append struct", slog.Int("worker_id", workerID), slog.String("error", err.Error()))
			}
			lastTimeReceived = flow.TimeReceived
		}

		err = batch.Send()
		if err != nil {
			slog.Error("failed to send batch", slog.Int("worker_id", workerID), slog.String("error", err.Error()))
			continue
		}

		diff := now.Sub(lastTimeReceived)
		if diff.Seconds() >= 30 {
			slog.Warn("delay between flows collection and pushing data", slog.Float64("seconds", diff.Seconds()))
		}
	}
}

func (d *ClickhouseDriver) Init() error {
	if d.batchSize <= 0 {
		d.batchSize = 10000
	}
	if d.batchMaxTime <= 0 {
		d.batchMaxTime = 10
	}
	if d.maxWorkers <= 0 {
		d.maxWorkers = 8
	}

	options, err := clickhouse.ParseDSN(d.dsn)
	if err != nil {
		return err
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return err
	}

	d.connection = conn

	err = d.connection.Exec(context.TODO(), initSql)
	if err != nil {
		return err
	}

	d.ctx, d.cancel = context.WithCancel(context.Background())

	d.wg.Add(1)
	go d.collectFlowBatch()

	for i := 0; i < d.maxWorkers; i++ {
		d.wg.Add(1)
		go d.pushFlows(i)
	}

	slog.Info("clickhouse transport initialized", slog.Int("workers", d.maxWorkers))

	return nil
}

func (d *ClickhouseDriver) Send(key, data []byte) error {
	r := bytes.NewReader(data)

	var flow flowpb.FlowMessage

	err := protodelim.UnmarshalFrom(r, &flow)
	if err != nil {
		slog.Error("failed to unmarshal flow message", slog.String("error", err.Error()))
		return nil
	}

	d.flows <- &flow

	return nil
}

func (d *ClickhouseDriver) Close() error {
	slog.Info("shutting down clickhouse transport")

	if d.cancel != nil {
		d.cancel()
	}

	d.wg.Wait()

	slog.Info("all workers stopped")

	d.connection.Close()

	return nil
}

func init() {
	d := &ClickhouseDriver{
		flows:      make(chan *flowpb.FlowMessage),
		flowsBatch: make(chan []*Flow),
	}

	transport.RegisterTransportDriver("clickhouse", d)
}

func renderTimestamp(data uint64) time.Time {
	return time.Unix(int64(data/1e9), int64(data%1e9)).UTC()
}

func renderMac(data uint64) string {
	var mac [8]byte
	binary.BigEndian.PutUint64(mac[:], data)
	return net.HardwareAddr(mac[2:]).String()
}

func renderIP(data []byte) string {
	return net.IP(data).String()
}
