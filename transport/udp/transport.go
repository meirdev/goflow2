package udp

import (
	"flag"
	"net"

	"github.com/netsampler/goflow2/v2/transport"
)

type UdpDriver struct {
	addr string

	connection *net.UDPConn
}

func (d *UdpDriver) Prepare() error {
	flag.StringVar(&d.addr, "transport.udp", "127.0.0.1:6000", "")

	return nil
}

func (d *UdpDriver) Init() error {
	var err error

	addr, err := net.ResolveUDPAddr("udp", d.addr)
	if err != nil {
		return err
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	d.connection = conn

	return nil
}

func (d *UdpDriver) Send(key, data []byte) error {
	d.connection.Write(data)

	return nil
}

func (d *UdpDriver) Close() error {
	d.connection.Close()

	return nil
}

func init() {
	d := &UdpDriver{}

	transport.RegisterTransportDriver("udp", d)
}
