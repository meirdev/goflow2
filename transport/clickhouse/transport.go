package clickhouse

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"log/slog"
	"net"
	"time"

	"github.com/netsampler/goflow2/v2/transport"
	"google.golang.org/protobuf/encoding/protodelim"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	flowpb "github.com/netsampler/goflow2/v2/pb"
)

type ClickhouseDriver struct {
	dsn string

	batchSize    int
	batchMaxTime int

	connection driver.Conn

	flows chan *flowpb.FlowMessage
}

func (d *ClickhouseDriver) Prepare() error {
	flag.StringVar(&d.dsn, "transport.clickhouse.dsn", "clickhouse://127.0.0.1:9000/default", "ClickHouse connection string")
	flag.IntVar(&d.batchSize, "transport.clickhouse.batchsize", 10000, "Batch size")
	flag.IntVar(&d.batchMaxTime, "transport.clickhouse.batchmaxtime", 10, "Max time in seconds to wait for a batch to be filled")

	return nil
}

func (d *ClickhouseDriver) pushFlows() {
	batchMaxTime := time.Duration(d.batchMaxTime) * time.Second

	for {
		slog.Debug("start collecting flows")

		flowsBatch := make([]*Flow, 0, d.batchSize)

		t := time.NewTimer(batchMaxTime)

	inner:
		for len(flowsBatch) < d.batchSize {
			select {
			case <-t.C:
				break inner
			case msg := <-d.flows:
				flowsBatch = append(flowsBatch, &Flow{
					Type:             int32(msg.Type),
					TimeReceived:     renderTimestamp(msg.TimeReceivedNs),
					SequenceNum:      msg.SequenceNum,
					SamplingRate:     msg.SamplingRate,
					SamplerAddress:   renderIP(msg.SamplerAddress),
					TimeFlowStart:    renderTimestamp(msg.TimeFlowStartNs),
					TimeFlowEnd:      renderTimestamp(msg.TimeFlowEndNs),
					Bytes:            msg.Bytes,
					Packets:          msg.Packets,
					SrcAddr:          renderIP(msg.SrcAddr),
					DstAddr:          renderIP(msg.DstAddr),
					Etype:            msg.Etype,
					Proto:            msg.Proto,
					SrcPort:          msg.SrcPort,
					DstPort:          msg.DstPort,
					InIf:             msg.InIf,
					OutIf:            msg.OutIf,
					SrcMac:           renderMac(msg.SrcMac),
					DstMac:           renderMac(msg.DstMac),
					ForwardingStatus: msg.ForwardingStatus,
					TcpFlags:         msg.TcpFlags,
					IcmpType:         msg.IcmpType,
					IcmpCode:         msg.IcmpCode,
					FragmentId:       msg.FragmentId,
					FragmentOffset:   msg.FragmentOffset,
					SrcAsn:           msg.SrcAsn,
					DstAsn:           msg.DstAsn,
					SrcCountry:       msg.SrcCountry,
					DstCountry:       msg.DstCountry,
					SrcPrefix:        msg.SrcPrefix,
					DstPrefix:        msg.DstPrefix,
				})
			}
		}

		slog.Debug("collected flows", slog.Int("batch", len(flowsBatch)))

		if len(flowsBatch) > 0 {
			batch, err := d.connection.PrepareBatch(context.TODO(), "INSERT INTO flows")
			if err != nil {
				slog.Error("failed to prepare batch", slog.String("error", err.Error()))
			}

			for _, flow := range flowsBatch {
				err := batch.AppendStruct(flow)
				if err != nil {
					slog.Error("failed to append struct", slog.String("error", err.Error()))
				}
			}

			err = batch.Send()
			if err != nil {
				slog.Error("failed to send batch", slog.String("error", err.Error()))
			}
		}
	}
}

func (d *ClickhouseDriver) Init() error {
	options, err := clickhouse.ParseDSN(d.dsn)
	if err != nil {
		return err
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return err
	}

	d.connection = conn

	go d.pushFlows()

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

	// slog.Debug("flow message sent", slog.Any("flow", &flow))

	d.flows <- &flow

	return nil
}

func (d *ClickhouseDriver) Close() error {
	d.connection.Close()

	return nil
}

func init() {
	d := &ClickhouseDriver{
		flows: make(chan *flowpb.FlowMessage),
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
