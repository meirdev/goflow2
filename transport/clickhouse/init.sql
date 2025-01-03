CREATE TABLE IF NOT EXISTS flows
(
    type Int32,
    time_received_ns UInt64,
    sequence_num UInt32,
    sampling_rate UInt64,

    sampler_address String,

    time_flow_start_ns UInt64,
    time_flow_end_ns UInt64,

    bytes UInt64,
    packets UInt64,

    src_addr String,
    dst_addr String,

    etype UInt32,

    proto UInt32,

    src_port UInt32,
    dst_port UInt32,

    forwarding_status UInt32,
    tcp_flags UInt32,
    icmp_type UInt32,
    icmp_code UInt32,

    fragment_id UInt32,
    fragment_offset UInt32,

    src_asn UInt32,
    dst_asn UInt32,

    src_country String,
    dst_country String,

    src_prefix String,
    dst_prefix String
)
ENGINE = Null();
