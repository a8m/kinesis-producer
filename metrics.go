package producer

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const systemName = "go_kinesis_producer"

type prometheusMetrics struct {
	userRecordsPutCnt                     *prometheus.CounterVec
	userRecordsDataPutSz                  *prometheus.SummaryVec
	kinesisRecordsPutCnt                  *prometheus.CounterVec
	kinesisRecordsDataPutSz               *prometheus.SummaryVec
	errorsByCodeCnt                       *prometheus.CounterVec
	allErrorsCnt                          *prometheus.CounterVec
	retriesPerRecordSum                   *prometheus.SummaryVec
	bufferingTimeDur                      *prometheus.HistogramVec
	requestTimeDur                        *prometheus.HistogramVec
	userRecordsPerKinesisRecordSum        *prometheus.SummaryVec
	kinesisRecordsPerPutRecordsRequestSum *prometheus.SummaryVec
}

func getMetrics(logger Logger) *prometheusMetrics {
	var userRecordsPutCnt = &metric{
		ID:          "userRecordsPutCnt",
		Name:        "user_records_put_total",
		Description: "Count of how many logical user records were received by the KPL core for put operations.",
		Args:        []string{"StreamName"},
		Type:        "counter_vec",
	}

	var userRecordsDataPutSz = &metric{
		ID:          "userRecordsDataPutSz",
		Name:        "user_records_data_put_bytes_sum",
		Description: "Bytes in the logical user records were received by the KPL core for put operations.",
		Args:        []string{"StreamName"},
		Type:        "summary_vec",
	}

	var kinesisRecordsPutCnt = &metric{
		ID:          "kinesisRecordsPutCnt",
		Name:        "kinesis_records_put_total",
		Description: "Count of how many Kinesis Data Streams records were put successfully (each Kinesis Data Streams record can contain multiple user records).",
		Args:        []string{"StreamName", "ShardId"},
		Type:        "counter_vec",
	}

	var kinesisRecordsDataPutSz = &metric{
		ID:          "kinesisRecordsDataPutSz",
		Name:        "kinesis_records_data_put_bytes_sum",
		Description: "Bytes in the Kinesis Data Streams records.",
		Args:        []string{"StreamName"},
		Type:        "summary_vec",
	}

	var errorsByCodeCnt = &metric{
		ID:          "errorsByCodeCnt",
		Name:        "errors_by_code_total",
		Description: "Count of each type of error code.",
		Args:        []string{"StreamName", "ErrorCode"},
		Type:        "counter_vec",
	}

	var allErrorsCnt = &metric{
		ID:          "allErrorsCnt",
		Name:        "errors_total",
		Description: "This is triggered by the same errors as Errors by Code, but does not distinguish between types.",
		Args:        []string{"StreamName"},
		Type:        "counter_vec",
	}

	var retriesPerRecordSum = &metric{
		ID:          "retriesPerRecordSum",
		Name:        "retries_per_record_sum",
		Description: "Number of retries performed per kinesis record. Zero is emitted for records that succeed in one try.",
		Args:        []string{"StreamName"},
		Type:        "summary_vec",
	}

	var bufferingTimeDur = &metric{
		ID:          "bufferingTimeDur",
		Name:        "buffering_time_milliseconds",
		Description: "The time between a user record arriving at the KPL and leaving for the backend.",
		Args:        []string{"StreamName"},
		Type:        "histogram_vec",
	}

	var requestTimeDur = &metric{
		ID:          "requestTimeDur",
		Name:        "request_time_milliseconds",
		Description: "The time it takes to perform PutRecordsRequests.",
		Args:        []string{"StreamName"},
		Type:        "histogram_vec",
	}

	var userRecordsPerKinesisRecordSum = &metric{
		ID:          "userRecordsPerKinesisRecordSum",
		Name:        "user_records_per_kinesis_record_sum",
		Description: "The number of logical user records aggregated into a single Kinesis Data Streams record.",
		Args:        []string{"StreamName"},
		Type:        "summary_vec",
	}

	var kinesisRecordsPerPutRecordsRequestSum = &metric{
		ID:          "kinesisRecordsPerPutRecordsRequestSum",
		Name:        "kinesis_records_per_put_records_request_sum",
		Description: "The number of Kinesis Data Streams records aggregated into a single PutRecordsRequest.",
		Args:        []string{"StreamName"},
		Type:        "summary_vec",
	}

	metricList := []*metric{
		userRecordsPutCnt,
		userRecordsDataPutSz,
		kinesisRecordsPutCnt,
		kinesisRecordsDataPutSz,
		errorsByCodeCnt,
		allErrorsCnt,
		retriesPerRecordSum,
		bufferingTimeDur,
		requestTimeDur,
		userRecordsPerKinesisRecordSum,
		kinesisRecordsPerPutRecordsRequestSum,
	}

	p := &prometheusMetrics{}

	for _, metricDef := range metricList {
		metric := newMetric(metricDef, systemName)
		if err := prometheus.Register(metric); err != nil {
			logger.Error(fmt.Sprintf("%s could not be registered in Prometheus", metricDef.Name), err)
		}

		switch metricDef {
		case userRecordsPutCnt:
			p.userRecordsPutCnt = metric.(*prometheus.CounterVec)
		case userRecordsDataPutSz:
			p.userRecordsDataPutSz = metric.(*prometheus.SummaryVec)
		case kinesisRecordsPutCnt:
			p.kinesisRecordsPutCnt = metric.(*prometheus.CounterVec)
		case kinesisRecordsDataPutSz:
			p.kinesisRecordsDataPutSz = metric.(*prometheus.SummaryVec)
		case errorsByCodeCnt:
			p.errorsByCodeCnt = metric.(*prometheus.CounterVec)
		case allErrorsCnt:
			p.allErrorsCnt = metric.(*prometheus.CounterVec)
		case retriesPerRecordSum:
			p.retriesPerRecordSum = metric.(*prometheus.SummaryVec)
		case bufferingTimeDur:
			p.bufferingTimeDur = metric.(*prometheus.HistogramVec)
		case requestTimeDur:
			p.requestTimeDur = metric.(*prometheus.HistogramVec)
		case userRecordsPerKinesisRecordSum:
			p.userRecordsPerKinesisRecordSum = metric.(*prometheus.SummaryVec)
		case kinesisRecordsPerPutRecordsRequestSum:
			p.kinesisRecordsPerPutRecordsRequestSum = metric.(*prometheus.SummaryVec)
		}

		metricDef.MetricCollector = metric
	}

	return p
}

type metric struct {
	MetricCollector prometheus.Collector
	ID              string
	Name            string
	Description     string
	Type            string
	Args            []string
}

// nolint funlen
func newMetric(m *metric, subsystem string) prometheus.Collector {
	var metric prometheus.Collector
	switch m.Type {
	case "counter_vec":
		metric = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Subsystem: subsystem,
				Name:      m.Name,
				Help:      m.Description,
			},
			m.Args,
		)
	case "gauge_vec":
		metric = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Subsystem: subsystem,
				Name:      m.Name,
				Help:      m.Description,
			},
			m.Args,
		)
	case "histogram_vec":
		metric = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Subsystem: subsystem,
				Name:      m.Name,
				Help:      m.Description,
			},
			m.Args,
		)
	case "summary_vec":
		metric = prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Subsystem: subsystem,
				Name:      m.Name,
				Help:      m.Description,
			},
			m.Args,
		)
	}
	return metric
}
