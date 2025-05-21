CREATE TABLE IF NOT EXISTS flows
(
    type Int32,
    time_received_ns UInt64,
    sequence_num UInt32,
    sampling_rate UInt64,

    sampler_address FixedString(16),

    time_flow_start_ns UInt64,
    time_flow_end_ns UInt64,

    bytes UInt64,
    packets UInt64,

    src_addr FixedString(16),
    dst_addr FixedString(16),

    etype UInt32,
    proto UInt32,

    tcp_flags UInt32,

    src_port UInt32,
    dst_port UInt32
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9093',
    kafka_num_consumers = 1,
    kafka_topic_list = 'flows',
    kafka_group_name = 'clickhouse',
    kafka_format = 'Protobuf',
    kafka_schema = 'flow.proto:FlowMessage';

CREATE DATABASE IF NOT EXISTS dictionaries;

CREATE DICTIONARY IF NOT EXISTS dictionaries.protocols (
    proto UInt8,
    name String,
    description String
)
PRIMARY KEY proto
LAYOUT(FLAT())
SOURCE (FILE(path '/var/lib/clickhouse/user_files/protocols.csv' format 'CSVWithNames'))
LIFETIME(0);

CREATE DICTIONARY IF NOT EXISTS dictionaries.routers (
    name String,
    router_ip String,
    default_sampling UInt64
)
PRIMARY KEY router_ip
LAYOUT(HASHED())
SOURCE (FILE(path '/var/lib/clickhouse/user_files/routers.csv' format 'CSVWithNames'))
LIFETIME(0);

CREATE FUNCTION IF NOT EXISTS convertFixedStringIpToString AS (addr) ->
(
    -- if the first 12 bytes are zero, then it's an IPv4 address, otherwise it's an IPv6 address
    if(reinterpretAsUInt128(substring(reverse(addr), 1, 12)) = 0, IPv4NumToString(reinterpretAsUInt32(substring(reverse(addr), 13, 4))), IPv6NumToString(addr))
);

CREATE FUNCTION IF NOT EXISTS convertFlowTypeToString AS (type) ->
(
    transform(type, [0, 1, 2, 3, 4], ['UNKNOWN', 'SFLOW_5', 'NETFLOW_V5', 'NETFLOW_V9', 'IPFIX'], toString(type))
);

CREATE FUNCTION IF NOT EXISTS convertFlowETypeToString AS (etype) ->
(
    transform(etype, [0x800, 0x806, 0x86dd], ['IPv4', 'ARP', 'IPv6'], toString(etype))
);

CREATE FUNCTION IF NOT EXISTS convertFlowTcpFlagsToString AS (tcp_flags) ->
(
    if(tcp_flags = 0, 'EMPTY', arrayStringConcat(arrayMap(x -> transform(x, [1, 2, 4, 8, 16, 32, 64, 128, 256, 512], ['FIN', 'SYN', 'RST', 'PSH', 'ACK', 'URG', 'ECN', 'CWR', 'NONCE', 'RESERVED'], toString(x)), bitmaskToArray(tcp_flags)), '+'))
);

CREATE TABLE IF NOT EXISTS flows_raw
(
    type LowCardinality(String),
    time_received DateTime64(9),

    sampler_address LowCardinality(String),

    time_flow_start DateTime64(9),
    time_flow_end DateTime64(9),

    bytes UInt64,
    packets UInt64,

    src_addr String,
    dst_addr String,

    etype LowCardinality(String),
    proto LowCardinality(String),

    tcp_flags LowCardinality(String),

    src_port UInt16,
    dst_port UInt16
)
ENGINE = MergeTree()
PARTITION BY toDate(time_flow_start)
ORDER BY time_flow_start;

CREATE MATERIALIZED VIEW IF NOT EXISTS flows_raw_mv TO flows_raw AS
    WITH temp AS (
        SELECT
            convertFlowTypeToString(type) AS type,
            toDateTime64(time_received_ns/1000000000, 9) AS time_received,

            convertFixedStringIpToString(sampler_address) AS sampler_address_string,

            dictGetOrDefault('dictionaries.routers', 'default_sampling', sampler_address_string, sampling_rate) AS default_sampling_rate,

            toDateTime64(time_flow_start_ns/1000000000, 9) AS time_flow_start,
            toDateTime64(time_flow_end_ns/1000000000, 9) AS time_flow_end,

            bytes * default_sampling_rate AS bytes,
            packets * default_sampling_rate AS packets,

            convertFixedStringIpToString(src_addr) AS src_addr_string,
            convertFixedStringIpToString(dst_addr) AS dst_addr_string,

            convertFlowETypeToString(etype) AS etype,
            dictGetOrDefault('dictionaries.protocols', 'name', proto, toString(proto)) AS proto,

            convertFlowTcpFlagsToString(tcp_flags) AS tcp_flags,

            src_port,
            dst_port
        FROM flows
    )
    SELECT
        type,
        time_received,

        sampler_address_string AS sampler_address,

        time_flow_start,
        time_flow_end,

        bytes,
        packets,

        src_addr_string AS src_addr,
        dst_addr_string AS dst_addr,

        etype,
        proto,

        tcp_flags,

        src_port,
        dst_port
    FROM temp;
