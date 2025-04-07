package clickhouse

import "time"

type Flow struct {
	Type         int32     `ch:"type"`
	TimeReceived time.Time `ch:"time_received"`
	SequenceNum  uint32    `ch:"sequence_num"`
	SamplingRate uint64    `ch:"sampling_rate"`

	SamplerAddress string `ch:"sampler_address"`

	TimeFlowStart time.Time `ch:"time_flow_start"`
	TimeFlowEnd   time.Time `ch:"time_flow_end"`

	Bytes   uint64 `ch:"bytes"`
	Packets uint64 `ch:"packets"`

	SrcAddr string `ch:"src_addr"`
	DstAddr string `ch:"dst_addr"`

	Etype uint16 `ch:"etype"`

	Proto uint8 `ch:"proto"`

	SrcPort uint16 `ch:"src_port"`
	DstPort uint16 `ch:"dst_port"`

	InIf  uint32 `ch:"in_if"`
	OutIf uint32 `ch:"out_if"`

	SrcMac string `ch:"src_mac"`
	DstMac string `ch:"dst_mac"`

	ForwardingStatus uint32 `ch:"forwarding_status"`
	TcpFlags         uint16 `ch:"tcp_flags"`
	IcmpType         uint16 `ch:"icmp_type"`
	IcmpCode         uint16 `ch:"icmp_code"`

	FragmentId     uint32 `ch:"fragment_id"`
	FragmentOffset uint32 `ch:"fragment_offset"`

	SrcAs uint32 `ch:"src_as"`
	DstAs uint32 `ch:"dst_as"`

	SrcNet uint8 `ch:"src_net"`
	DstNet uint8 `ch:"dst_net"`

	NextHop   string `ch:"next_hop"`
	NextHopAs uint32 `ch:"next_hop_as"`

	BgpNextHop string `ch:"bgp_next_hop"`

	ObservationDomainId uint32 `ch:"observation_domain_id"`
	ObservationPointId  uint32 `ch:"observation_point_id"`
}
