package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"strings"

	flowmessage "github.com/netsampler/goflow2/v2/pb"

	// import various formatters
	"github.com/netsampler/goflow2/v2/format"
	_ "github.com/netsampler/goflow2/v2/format/binary"
	_ "github.com/netsampler/goflow2/v2/format/json"
	_ "github.com/netsampler/goflow2/v2/format/text"

	// import various transports
	"github.com/netsampler/goflow2/v2/transport"
	_ "github.com/netsampler/goflow2/v2/transport/clickhouse"
	_ "github.com/netsampler/goflow2/v2/transport/file"
	_ "github.com/netsampler/goflow2/v2/transport/kafka"

	utils "github.com/netsampler/goflow2/v2/utils"

	"github.com/oschwald/geoip2-golang"
	"google.golang.org/protobuf/encoding/protodelim"
)

var (
	version    = ""
	buildinfos = ""
	AppVersion = "Enricher " + version + " " + buildinfos

	DbAsn     = flag.String("db.asn", "", "IP->ASN database")
	DbCountry = flag.String("db.country", "", "IP->Country database")

	Prefixes = flag.String("prefixes", "", "Prefixes file")

	LogLevel = flag.String("loglevel", "info", "Log level")
	LogFmt   = flag.String("logfmt", "normal", "Log formatter")

	Format    = flag.String("format", "json", fmt.Sprintf("Choose the format (available: %s)", strings.Join(format.GetFormats(), ", ")))
	Transport = flag.String("transport", "file", fmt.Sprintf("Choose the transport (available: %s)", strings.Join(transport.GetTransports(), ", ")))

	Version = flag.Bool("v", false, "Print version")
)

func MapAsn(db *geoip2.Reader, addr []byte, dest *uint32) {
	entry, err := db.ASN(net.IP(addr))
	if err != nil {
		return
	}
	*dest = uint32(entry.AutonomousSystemNumber)
}

func MapCountry(db *geoip2.Reader, addr []byte, dest *string) {
	entry, err := db.Country(net.IP(addr))
	if err != nil {
		return
	}
	*dest = entry.Country.IsoCode
}

func MapFlow(dbAsn, dbCountry *geoip2.Reader, msg *ProtoProducerMessage) {
	if dbAsn != nil {
		MapAsn(dbAsn, msg.SrcAddr, &(msg.FlowMessage.SrcAsn))
		MapAsn(dbAsn, msg.DstAddr, &(msg.FlowMessage.DstAsn))
	}
	if dbCountry != nil {
		MapCountry(dbCountry, msg.SrcAddr, &(msg.FlowMessage.SrcCountry))
		MapCountry(dbCountry, msg.DstAddr, &(msg.FlowMessage.DstCountry))
	}
}

type ProtoProducerMessage struct {
	flowmessage.FlowMessage
}

func (m *ProtoProducerMessage) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	_, err := protodelim.MarshalTo(buf, m)
	return buf.Bytes(), err
}

func main() {
	flag.Parse()

	if *Version {
		fmt.Println(AppVersion)
		os.Exit(0)
	}

	var loglevel slog.Level
	if err := loglevel.UnmarshalText([]byte(*LogLevel)); err != nil {
		log.Fatal("error parsing log level")
	}

	lo := slog.HandlerOptions{
		Level: loglevel,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &lo))

	switch *LogFmt {
	case "json":
		logger = slog.New(slog.NewJSONHandler(os.Stderr, &lo))
	}

	slog.SetDefault(logger)

	var dbAsn, dbCountry *geoip2.Reader
	var err error
	if *DbAsn != "" {
		dbAsn, err = geoip2.Open(*DbAsn)
		if err != nil {
			slog.Error("error opening asn db", slog.String("error", err.Error()))
			os.Exit(1)
		}
		defer dbAsn.Close()
	}

	if *DbCountry != "" {
		dbCountry, err = geoip2.Open(*DbCountry)
		if err != nil {
			slog.Error("error opening country db", slog.String("error", err.Error()))
			os.Exit(1)
		}
		defer dbCountry.Close()
	}

	var prefixes *utils.NetworksTree[string] = utils.NewNetworksTree[string]()
	if *Prefixes != "" {
		file, err := os.Open(*Prefixes)
		if err != nil {
			slog.Error("error opening prefixes file", slog.String("error", err.Error()))
			os.Exit(1)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}

			err := prefixes.Set(line, line)
			if err != nil {
				slog.Error("error setting prefix", slog.String("error", err.Error()))
				os.Exit(1)
			}
		}
	}

	formatter, err := format.FindFormat(*Format)
	if err != nil {
		log.Fatal(err)
	}

	transporter, err := transport.FindTransport(*Transport)
	if err != nil {
		slog.Error("error transporter", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer transporter.Close()

	logger.Info("starting enricher")

	rdr := bufio.NewReader(os.Stdin)

	var msg ProtoProducerMessage
	for {
		if err := protodelim.UnmarshalFrom(rdr, &msg); err != nil && errors.Is(err, io.EOF) {
			return
		} else if err != nil {
			slog.Error("error unmarshalling message", slog.String("error", err.Error()))
			continue
		}

		MapFlow(dbAsn, dbCountry, &msg)

		srcPrefix, err := prefixes.Get(net.IP(msg.SrcAddr).String())
		if err == nil {
			msg.SrcPrefix = srcPrefix
		}

		dstPrefix, err := prefixes.Get(net.IP(msg.DstAddr).String())
		if err == nil {
			msg.DstPrefix = dstPrefix
		}

		key, data, err := formatter.Format(&msg)
		if err != nil {
			slog.Error("error formatting message", slog.String("error", err.Error()))
			continue
		}

		err = transporter.Send(key, data)
		if err != nil {
			slog.Error("error sending message", slog.String("error", err.Error()))
			continue
		}

		msg.Reset()
	}
}
