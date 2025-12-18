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

	"github.com/netsampler/goflow2/v2/metrics"
	"github.com/netsampler/goflow2/v2/transport"
	"google.golang.org/protobuf/encoding/protodelim"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"

	flowpb "github.com/netsampler/goflow2/v2/pb"
)

//go:embed init.sql
var initSql string

type ClickhouseDriver struct {
	address  string
	database string
	username string
	password string

	batchSize             int
	batchMaxTime          int
	maxWorkers            int
	poolSize              int
	maxRetries            int
	delayWarningThreshold int

	pool *chpool.Pool

	flows      chan *flowpb.FlowMessage
	flowsBatch chan []*Flow
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func (d *ClickhouseDriver) Prepare() error {
	flag.StringVar(&d.address, "transport.clickhouse.address", "127.0.0.1:9000", "ClickHouse server address")
	flag.StringVar(&d.database, "transport.clickhouse.database", "default", "ClickHouse database name")
	flag.StringVar(&d.username, "transport.clickhouse.username", "default", "ClickHouse username")
	flag.StringVar(&d.password, "transport.clickhouse.password", "", "ClickHouse password")
	flag.IntVar(&d.batchSize, "transport.clickhouse.batchsize", 10000, "Batch size")
	flag.IntVar(&d.batchMaxTime, "transport.clickhouse.batchmaxtime", 10, "Max time in seconds to wait for a batch to be filled")
	flag.IntVar(&d.maxWorkers, "transport.clickhouse.workers", 8, "Max number of pushing data workers")
	flag.IntVar(&d.poolSize, "transport.clickhouse.poolsize", 8, "Connection pool size")
	flag.IntVar(&d.maxRetries, "transport.clickhouse.maxretries", 3, "Max number of retries for failed batch inserts")
	flag.IntVar(&d.delayWarningThreshold, "transport.clickhouse.delaythreshold", 30, "Delay warning threshold in seconds")

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
					Type:                msg.Type.String(),
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
					IpTos:               msg.IpTos,
					IpTtl:               msg.IpTtl,
					IpFlags:             msg.IpFlags,
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

	const initialBackoff = 100 * time.Millisecond

	for flowsBatch := range d.flowsBatch {
		now := time.Now()

		batchLen := len(flowsBatch)

		var (
			colType                = proto.NewLowCardinality(new(proto.ColStr))
			colTimeReceived        = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
			colSequenceNum         proto.ColUInt32
			colSamplingRate        proto.ColUInt64
			colSamplerAddress      = proto.NewLowCardinality(new(proto.ColStr))
			colTimeFlowStart       = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
			colTimeFlowEnd         = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
			colBytes               proto.ColUInt64
			colPackets             proto.ColUInt64
			colSrcAddr             proto.ColStr
			colDstAddr             proto.ColStr
			colEtype               proto.ColUInt16
			colProto               proto.ColUInt8
			colSrcPort             proto.ColUInt16
			colDstPort             proto.ColUInt16
			colInIf                proto.ColUInt32
			colOutIf               proto.ColUInt32
			colSrcMac              proto.ColStr
			colDstMac              proto.ColStr
			colForwardingStatus    proto.ColUInt32
			colIpTos               proto.ColUInt32
			colIpTtl               proto.ColUInt32
			colIpFlags             proto.ColUInt32
			colTcpFlags            proto.ColUInt16
			colIcmpType            proto.ColUInt16
			colIcmpCode            proto.ColUInt16
			colFragmentId          proto.ColUInt32
			colFragmentOffset      proto.ColUInt32
			colSrcAs               proto.ColUInt32
			colDstAs               proto.ColUInt32
			colSrcNet              proto.ColUInt8
			colDstNet              proto.ColUInt8
			colNextHop             proto.ColStr
			colNextHopAs           proto.ColUInt32
			colBgpNextHop          proto.ColStr
			colObservationDomainId proto.ColUInt32
			colObservationPointId  proto.ColUInt32
		)

		var lastTimeReceived time.Time

		for _, flow := range flowsBatch {
			colType.Append(flow.Type)
			colTimeReceived.Append(flow.TimeReceived)
			colSequenceNum.Append(flow.SequenceNum)
			colSamplingRate.Append(flow.SamplingRate)
			colSamplerAddress.Append(flow.SamplerAddress)
			colTimeFlowStart.Append(flow.TimeFlowStart)
			colTimeFlowEnd.Append(flow.TimeFlowEnd)
			colBytes.Append(flow.Bytes)
			colPackets.Append(flow.Packets)
			colSrcAddr.Append(flow.SrcAddr)
			colDstAddr.Append(flow.DstAddr)
			colEtype.Append(flow.Etype)
			colProto.Append(flow.Proto)
			colSrcPort.Append(flow.SrcPort)
			colDstPort.Append(flow.DstPort)
			colInIf.Append(flow.InIf)
			colOutIf.Append(flow.OutIf)
			colSrcMac.Append(flow.SrcMac)
			colDstMac.Append(flow.DstMac)
			colForwardingStatus.Append(flow.ForwardingStatus)
			colIpTos.Append(flow.IpTos)
			colIpTtl.Append(flow.IpTtl)
			colIpFlags.Append(flow.IpFlags)
			colTcpFlags.Append(flow.TcpFlags)
			colIcmpType.Append(flow.IcmpType)
			colIcmpCode.Append(flow.IcmpCode)
			colFragmentId.Append(flow.FragmentId)
			colFragmentOffset.Append(flow.FragmentOffset)
			colSrcAs.Append(flow.SrcAs)
			colDstAs.Append(flow.DstAs)
			colSrcNet.Append(flow.SrcNet)
			colDstNet.Append(flow.DstNet)
			colNextHop.Append(flow.NextHop)
			colNextHopAs.Append(flow.NextHopAs)
			colBgpNextHop.Append(flow.BgpNextHop)
			colObservationDomainId.Append(flow.ObservationDomainId)
			colObservationPointId.Append(flow.ObservationPointId)

			lastTimeReceived = flow.TimeReceived
		}

		var err error
		query := ch.Query{
			Body: "INSERT INTO flows_sink VALUES",
			Input: proto.Input{
				{Name: "type", Data: colType},
				{Name: "time_received", Data: colTimeReceived},
				{Name: "sequence_num", Data: colSequenceNum},
				{Name: "sampling_rate", Data: colSamplingRate},
				{Name: "sampler_address", Data: colSamplerAddress},
				{Name: "time_flow_start", Data: colTimeFlowStart},
				{Name: "time_flow_end", Data: colTimeFlowEnd},
				{Name: "bytes", Data: colBytes},
				{Name: "packets", Data: colPackets},
				{Name: "src_addr", Data: colSrcAddr},
				{Name: "dst_addr", Data: colDstAddr},
				{Name: "etype", Data: colEtype},
				{Name: "proto", Data: colProto},
				{Name: "src_port", Data: colSrcPort},
				{Name: "dst_port", Data: colDstPort},
				{Name: "in_if", Data: colInIf},
				{Name: "out_if", Data: colOutIf},
				{Name: "src_mac", Data: colSrcMac},
				{Name: "dst_mac", Data: colDstMac},
				{Name: "forwarding_status", Data: colForwardingStatus},
				{Name: "ip_tos", Data: colIpTos},
				{Name: "ip_ttl", Data: colIpTtl},
				{Name: "ip_flags", Data: colIpFlags},
				{Name: "tcp_flags", Data: colTcpFlags},
				{Name: "icmp_type", Data: colIcmpType},
				{Name: "icmp_code", Data: colIcmpCode},
				{Name: "fragment_id", Data: colFragmentId},
				{Name: "fragment_offset", Data: colFragmentOffset},
				{Name: "src_as", Data: colSrcAs},
				{Name: "dst_as", Data: colDstAs},
				{Name: "src_net", Data: colSrcNet},
				{Name: "dst_net", Data: colDstNet},
				{Name: "next_hop", Data: colNextHop},
				{Name: "next_hop_as", Data: colNextHopAs},
				{Name: "bgp_next_hop", Data: colBgpNextHop},
				{Name: "observation_domain_id", Data: colObservationDomainId},
				{Name: "observation_point_id", Data: colObservationPointId},
			},
		}

		for attempt := range d.maxRetries {
			err = d.pool.Do(d.ctx, query)
			if err == nil {
				break
			}

			if attempt < d.maxRetries-1 {
				backoff := initialBackoff * (1 << attempt)
				slog.Warn("failed to send batch, retrying",
					slog.Int("worker_id", workerID),
					slog.Int("attempt", attempt+1),
					slog.Int("max_retries", d.maxRetries),
					slog.Duration("backoff", backoff),
					slog.String("error", err.Error()))

				select {
				case <-d.ctx.Done():
					return
				case <-time.After(backoff):
				}
			}
		}

		if err != nil {
			slog.Error("failed to send batch after retries, dropping batch",
				slog.Int("worker_id", workerID),
				slog.Int("batch_size", batchLen),
				slog.String("error", err.Error()))
			continue
		}

		metrics.ClickHouseFlowsInserted.Add(float64(batchLen))

		diff := now.Sub(lastTimeReceived)
		if diff.Seconds() >= float64(d.delayWarningThreshold) {
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
	if d.poolSize <= 0 {
		d.poolSize = 8
	}
	if d.maxRetries <= 0 {
		d.maxRetries = 3
	}
	if d.delayWarningThreshold <= 0 {
		d.delayWarningThreshold = 30
	}

	pool, err := chpool.Dial(context.Background(), chpool.Options{
		ClientOptions: ch.Options{
			Address:     d.address,
			Database:    d.database,
			User:        d.username,
			Password:    d.password,
			Compression: ch.CompressionLZ4,
		},
		MaxConns: int32(d.poolSize),
	})
	if err != nil {
		return err
	}

	d.pool = pool

	err = d.pool.Do(context.Background(), ch.Query{
		Body: initSql,
	})
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

	slog.Info("clickhouse transport initialized",
		slog.Int("workers", d.maxWorkers),
		slog.Int("pool_size", d.poolSize),
		slog.String("address", d.address))

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

	metrics.ClickHouseFlowsReceived.Inc()

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

	if d.pool != nil {
		d.pool.Close()
	}

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
	hardwareAddr := net.HardwareAddr(mac[2:])
	if hardwareAddr == nil {
		return ""
	}
	return hardwareAddr.String()
}

func renderIP(data []byte) string {
	ip := net.IP(data)
	if ip == nil {
		return ""
	}
	return ip.String()
}
