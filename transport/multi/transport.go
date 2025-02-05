package udp

import (
	"flag"
	"log/slog"
	"strings"

	"github.com/netsampler/goflow2/v2/transport"
)

type MultiDriver struct {
	transports string

	transportDrivers []transport.TransportDriver
}

func (d *MultiDriver) Prepare() error {
	flag.StringVar(&d.transports, "transport.multi", "file,udp", "Specify the list of transports to use")

	return nil
}

func (d *MultiDriver) Init() error {
	for _, t := range strings.Split(d.transports, ",") {
		transport, err := transport.FindTransport(t)
		if err != nil {
			slog.Error("error transporter", slog.String("error", err.Error()))
			return err
		}

		d.transportDrivers = append(d.transportDrivers, transport)
	}

	return nil
}

func (d *MultiDriver) Send(key, data []byte) error {
	for _, t := range d.transportDrivers {
		t.Send(key, data);
	}

	return nil
}

func (d *MultiDriver) Close() error {
	for _, t := range d.transportDrivers {
		t.Close();
	}

	return nil
}

func init() {
	d := &MultiDriver{}

	transport.RegisterTransportDriver("multi", d)
}
