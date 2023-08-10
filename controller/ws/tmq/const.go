package tmq

const TaosTMQKey = "taos_tmq"
const (
	TMQSubscribe          = "subscribe"
	TMQPoll               = "poll"
	TMQFetch              = "fetch"
	TMQFetchBlock         = "fetch_block"
	TMQFetchRaw           = "fetch_raw"
	TMQFetchJsonMeta      = "fetch_json_meta"
	TMQCommit             = "commit"
	TMQUnsubscribe        = "unsubscribe"
	TMQGetTopicAssignment = "assignment"
	TMQSeek               = "seek"
	TMQCommitOffset       = "commit_offset"
	TMQCommitted          = "committed"
	TMQPosition           = "position"
	TMQListTopics         = "list_topics"
)
const TMQRawMessage = 3

const OffsetInvalid = -2147467247
