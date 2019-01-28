package producer

// Hooks is an interface accepted by the producer. It can be used
// to extract metrics out of the producer about interesting events.
type Hooks interface {

	// OnPutErr hook is called whenever a put fails. Only argument passed is the err code string
	OnDrain(size, length int64)

	// OnPutRecords hook is called when batches are pushed to kinesis. It passes the number of
	// batches, number of records, milliseconds the push took to complete and then reason for
	// flushing as arguments
	OnPutRecords(numRecords, numTotalRecords, putLatencyMS int64, reason string)

	// OnDrain hook is called when records from the in-memory buffer are batched
	// up before being pushed to a stream. It passes the size (in bytes)
	// and length (number of records) as arguments.
	OnPutErr(errCode string)
}

type noopHooks struct{}

func (h *noopHooks) OnDrain(size, length int64) {}

func (h *noopHooks) OnPutRecords(batches, records, putLatencyMS int64, reason string) {}

func (h *noopHooks) OnPutErr(errCode string) {}
