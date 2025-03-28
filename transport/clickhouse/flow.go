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

	Etype uint32 `ch:"etype"`

	Proto uint32 `ch:"proto"`

	SrcPort uint32 `ch:"src_port"`
	DstPort uint32 `ch:"dst_port"`

	InIf  uint32 `ch:"in_if"`
	OutIf uint32 `ch:"out_if"`

	SrcMac string `ch:"src_mac"`
	DstMac string `ch:"dst_mac"`

	ForwardingStatus uint32 `ch:"forwarding_status"`
	TcpFlags         uint32 `ch:"tcp_flags"`
	IcmpType         uint32 `ch:"icmp_type"`
	IcmpCode         uint32 `ch:"icmp_code"`

	FragmentId     uint32 `ch:"fragment_id"`
	FragmentOffset uint32 `ch:"fragment_offset"`

	ObservationDomainId uint32 `ch:"observation_domain_id"`
	ObservationPointId  uint32 `ch:"observation_point_id"`

	SrcAsn uint32 `ch:"src_asn"`
	DstAsn uint32 `ch:"dst_asn"`

	SrcCountry string `ch:"src_country"`
	DstCountry string `ch:"dst_country"`

	SrcPrefix string `ch:"src_prefix"`
	DstPrefix string `ch:"dst_prefix"`
}
