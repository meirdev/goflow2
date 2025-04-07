CREATE TABLE IF NOT EXISTS flows_sink
(
    type Int32,
    time_received DateTime64,
    sequence_num UInt32,
    sampling_rate UInt64,

    sampler_address String,

    time_flow_start DateTime64,
    time_flow_end DateTime64,

    bytes UInt64,
    packets UInt64,

    src_addr String,
    dst_addr String,

    etype UInt16,

    proto UInt8,

    src_port UInt16,
    dst_port UInt16,

    in_if UInt32,
    out_if UInt32,

    src_mac String,
    dst_mac String,

    forwarding_status UInt32,
    tcp_flags UInt16,
    icmp_type UInt16,
    icmp_code UInt16,

    fragment_id UInt32,
    fragment_offset UInt32,

    src_as UInt32,
    dst_as UInt32,

    src_net UInt8,
    dst_net UInt8,

    next_hop String,
    next_hop_as UInt32,

    bgp_next_hop String,

    observation_domain_id UInt32,
    observation_point_id UInt32
)
ENGINE = Null();