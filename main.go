package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/daemon"
	influxdb "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/sirupsen/logrus"
)

type Entry struct {
	Count int    `json:"count"`
	Term  string `json:"term"`
}

type Field struct {
	Name        string
	Measurement string
}

var conf struct {
	Interval time.Duration
	Loggly   struct {
		Query   string
		User    string
		Pass    string
		Token   string
		Account string
		Fields  []Field
	}
	Influx struct {
		Prefix string
		URL    string
		Token  string
		Org    string
		Bucket string
	}
}

type logglyClient struct {
	baseURL string
	client  *http.Client
}

func (lc *logglyClient) FieldCount(
	ctx context.Context,
	field string,
	from, to time.Time,
) (map[string]int, error) {

	url := fmt.Sprintf("%s/fields/%s?q=%s&facet_size=20&from=%s&to=%s",
		lc.baseURL,
		field,
		url.QueryEscape(conf.Loggly.Query),
		from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339),
	)

	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	if conf.Loggly.Token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("bearer %s", conf.Loggly.Token))
	}

	resp, err := lc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("invalid response status: %s", resp.Status)
	}

	return parseLogglyFieldCount(field, resp.Body)
}

func parseLogglyFieldCount(field string, r io.Reader) (map[string]int, error) {
	rawData := make(map[string]json.RawMessage)
	if err := json.NewDecoder(r).Decode(&rawData); err != nil {
		return nil, err
	}

	fieldData, ok := rawData[field]
	if !ok {
		return nil, fmt.Errorf("missing %q field data in loggly response", field)
	}

	var entries []Entry
	if err := json.Unmarshal(fieldData, &entries); err != nil {
		return nil, err
	}

	counts := make(map[string]int)
	for _, entry := range entries {
		counts[entry.Term] += entry.Count
	}

	return counts, nil
}

func pollFields(
	ctx context.Context,
	rapi api.WriteAPI,
	lc *logglyClient,
	interval time.Duration,
) {

	var (
		lastTstamp time.Time = time.Now()
		currTstamp time.Time
	)

	tm := time.NewTimer(interval)
	defer func() {
		tm.Stop()
		select {
		case <-tm.C:
		default:
		}
	}()

	for ctx.Err() == nil {
		select {
		case currTstamp = <-tm.C:
		case <-ctx.Done():
			return
		}

		for _, field := range conf.Loggly.Fields {
			counts, err := lc.FieldCount(ctx, field.Name, lastTstamp, currTstamp)
			if err != nil {
				if errors.Is(err, ctx.Err()) {
					return
				}

				logrus.WithField("field", field.Name).
					WithError(err).
					Error("getting stats from loggly")

				continue
			}

			logrus.WithFields(logrus.Fields{
				"field":  field.Name,
				"counts": counts,
				"from":   lastTstamp,
				"to":     currTstamp,
			}).Info("received data from loggly")

			for name, count := range counts {
				point := write.NewPoint(
					fmt.Sprintf("%s%s", conf.Influx.Prefix, field.Measurement),
					map[string]string{field.Name: name},
					map[string]interface{}{"count": count},
					currTstamp,
				)

				rapi.WritePoint(point)
			}
		}

		rapi.Flush()
		lastTstamp = currTstamp
		tm.Reset(interval)
	}
}

func main() {
	var fields string

	flag.StringVar(&conf.Loggly.Account, "loggly-account", "", "Loggly account")
	flag.StringVar(&conf.Loggly.User, "loggly-user", "", "Loggly username")
	flag.StringVar(&conf.Loggly.Pass, "loggly-pass", "", "Loggly password")
	flag.StringVar(&conf.Loggly.Token, "loggly-token", "", "Loggly access token")
	flag.StringVar(&conf.Loggly.Query, "loggly-query", "", "Query for data lookup, can be empty")
	flag.StringVar(&fields, "loggly-fields", "tag:tag_entries", "Fields data separated by comma, syntax is as follows: loggly_field:influx_measurement_name")
	flag.StringVar(&conf.Influx.URL, "influx-url", "", "Influx URL")
	flag.StringVar(&conf.Influx.Token, "influx-token", "", "Influx token")
	flag.StringVar(&conf.Influx.Org, "influx-org", "", "Influx organization")
	flag.StringVar(&conf.Influx.Bucket, "influx-bucket", "", "Influx organization bucket")
	flag.StringVar(&conf.Influx.Prefix, "influx-prefix", "", "Prefix for all influx metrics names")
	flag.DurationVar(&conf.Interval, "interval", time.Minute, "Data poll interval")
	flag.Parse()

	for _, field := range strings.Split(fields, ",") {
		parts := strings.Split(field, ":")
		if len(parts) != 2 {
			logrus.Fatal("invalid loggly-fields")
		}

		conf.Loggly.Fields = append(conf.Loggly.Fields, Field{
			Name:        parts[0],
			Measurement: parts[1],
		})
	}

	baseURL := url.URL{
		Scheme: "https",
		Host:   fmt.Sprintf("%s.loggly.com", conf.Loggly.Account),
		Path:   "/apiv2",
	}

	if conf.Loggly.Pass != "" {
		baseURL.User = url.UserPassword(conf.Loggly.User, conf.Loggly.Pass)
	}

	lc := &logglyClient{
		baseURL: baseURL.String(),
		client:  http.DefaultClient,
	}

	client := influxdb.NewClientWithOptions(
		conf.Influx.URL,
		conf.Influx.Token,
		influxdb.DefaultOptions(),
	)

	rapi := client.WriteAPI(conf.Influx.Org, conf.Influx.Bucket)
	errCh := rapi.Errors()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errCh {
			logrus.WithError(err).
				Error("writing metrics batch to influx database")

		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer func() {
			client.Close()
			wg.Done()
		}()

		logrus.WithField("baseURL", baseURL.String()).
			Info("starting loggly poller")

		pollFields(ctx, rapi, lc, conf.Interval)
	}()

	// Listen for termination request.
	terminationCh := make(chan os.Signal, 1)
	signal.Notify(terminationCh, syscall.SIGINT, syscall.SIGTERM)
	daemon.SdNotify(false, daemon.SdNotifyReady)

	<-terminationCh
	daemon.SdNotify(false, daemon.SdNotifyStopping)

	cancel()
	wg.Wait()
	logrus.Info("stopped loggly poller")
}
