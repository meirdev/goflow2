CREATE TABLE IF NOT EXISTS flows
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

    etype UInt32,

    proto UInt32,

    src_port UInt32,
    dst_port UInt32,

    in_if UInt32,
    out_if UInt32,

    src_mac String,
    dst_mac String,

    forwarding_status UInt32,
    tcp_flags UInt32,
    icmp_type UInt32,
    icmp_code UInt32,

    fragment_id UInt32,
    fragment_offset UInt32,

    observation_domain_id UInt32,
    observation_point_id UInt32,

    src_asn UInt32,
    dst_asn UInt32,

    src_country String,
    dst_country String,

    src_prefix String,
    dst_prefix String
)
ENGINE = Null();
