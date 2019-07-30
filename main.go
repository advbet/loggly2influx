package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/sirupsen/logrus"
)

type logglyClient struct {
	baseURL string
	client  *http.Client
}

var conf struct {
	interval time.Duration
	loggly   struct {
		account string
		user    string
		pass    string
		query   string
	}
	influx struct {
		url      string
		database string
		prefix   string
	}
}

//func (lc *logglyClient) MessageCount(from, to time.Time, tag string) (map[string]int, int, error) {
//}

func parseLogglyFieldCount(r io.Reader, field string) (map[string]int, error) {
	var data map[string]json.RawMessage

	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return nil, err
	}

	rawJSON, ok := data[field]
	if !ok {
		return nil, fmt.Errorf("missing field \"%s\" in response", field)
	}

	tagsJSON := []struct {
		Count int    `json:"count"`
		Term  string `json:"term"`
	}{}

	if err := json.Unmarshal(rawJSON, &tagsJSON); err != nil {
		return nil, err
	}

	tags := make(map[string]int)
	for _, tag := range tagsJSON {
		tags[tag.Term] += tag.Count
	}
	return tags, nil
}

func (lc *logglyClient) TagsCount(field string, query string, from, to time.Time) (map[string]int, error) {
	url := fmt.Sprintf("%s/fields/%s?q=%s&facet_size=20&from=%s&to=%s",
		lc.baseURL,
		field,
		url.QueryEscape(query),
		from.UTC().Format(time.RFC3339),
		to.UTC().Format(time.RFC3339),
	)

	resp, err := lc.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, errors.New(resp.Status)
	}
	return parseLogglyFieldCount(resp.Body, field)
}

func writeTags(influx client.Client, tags map[string]int, at time.Time) error {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  conf.influx.database,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	for tag, count := range tags {
		pt, err := client.NewPoint(
			fmt.Sprintf("%stag_entries", conf.influx.prefix),
			map[string]string{"tag": tag},
			map[string]interface{}{"count": count},
			at,
		)
		if err != nil {
			return err
		}
		bp.AddPoint(pt)
	}
	return influx.Write(bp)
}

func writeMessages(influx client.Client, tag string, msgs map[string]int, at time.Time) error {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  conf.influx.database,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	for msg, count := range msgs {
		pt, err := client.NewPoint(
			fmt.Sprintf("%smessage_entries", conf.influx.prefix),
			map[string]string{"tag": tag, "message": msg},
			map[string]interface{}{"count": count},
			at,
		)
		if err != nil {
			return err
		}
		bp.AddPoint(pt)
	}
	return influx.Write(bp)
}

func loop(influx client.Client, lc *logglyClient, interval time.Duration) {
	nextCheck := time.Now().Add(-1 * interval)

	for {
		logrus.WithField("duration", -1*time.Since(nextCheck)).Info("sleep")
		time.Sleep(-1 * time.Since(nextCheck))
		var tags map[string]int
		for {
			var err error
			tags, err = lc.TagsCount("tag", conf.loggly.query, nextCheck.Add(-1*interval), nextCheck)
			if err == nil {
				break
			}
			logrus.WithError(err).Error("getting field stats from loggly")
			time.Sleep(time.Second)
		}
		logrus.WithFields(logrus.Fields{
			"tags": tags,
			"from": nextCheck.Add(-1 * interval),
			"to":   nextCheck,
		}).Info("received data from loggly")
		for {
			if err := writeTags(influx, tags, nextCheck); err != nil {
				logrus.WithError(err).Error("writing data to influx")
				time.Sleep(time.Second)
				continue
			}
			break
		}
		/*
			for tag := range tags {
				var msgs map[string]int
				for {
					msgs, err = lc.TagsCount("json.message", "tag:"+tag, nextCheck.Add(-1*interval), nextCheck)
					if err == nil {
						break
					}
				}
				logrus.WithField("messages", msgs).Info("data from loggly")
				for {
					if err := writeMessages(influx, tag, msgs, nextCheck); err == nil {
						break
					}
					logrus.WithError(err).Error("writing data to influx")
					time.Sleep(time.Second)
				}

			}
		*/
		nextCheck = nextCheck.Add(interval)
	}
}

func main() {
	flag.StringVar(&conf.loggly.account, "loggly-account", "", "Loggly account")
	flag.StringVar(&conf.loggly.user, "loggly-user", "", "Loggly username")
	flag.StringVar(&conf.loggly.pass, "loggly-pass", "", "Loggly password")
	flag.StringVar(&conf.loggly.query, "loggly-query", "", "Query for data lookup, can be empty")
	flag.StringVar(&conf.influx.url, "influx-url", "", "Influx DB URL")
	flag.StringVar(&conf.influx.database, "influx-database", "", "Influx database name")
	flag.StringVar(&conf.influx.prefix, "influx-prefix", "", "Prefix for all influx metrics names")
	flag.DurationVar(&conf.interval, "interval", time.Minute, "Data poll interval")
	flag.Parse()

	lc := &logglyClient{
		baseURL: fmt.Sprintf("https://%s:%s@%s.loggly.com/apiv2", conf.loggly.user, conf.loggly.pass, conf.loggly.account),
		client:  http.DefaultClient,
	}

	influx, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: conf.influx.url,
	})
	if err != nil {
		logrus.WithError(err).Fatal("creating new influx client")
	}
	defer influx.Close()

	loop(influx, lc, conf.interval)
}
