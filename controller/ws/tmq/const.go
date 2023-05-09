package tmq

const TaosTMQKey = "taos_tmq"
const (
	TMQSubscribe     = "subscribe"
	TMQPoll          = "poll"
	TMQFetch         = "fetch"
	TMQFetchBlock    = "fetch_block"
	TMQFetchRaw      = "fetch_raw"
	TMQFetchJsonMeta = "fetch_json_meta"
	TMQCommit        = "commit"
	TMQUnsubscribe   = "unsubscribe"
)
const TMQRawMessage = 3
