package proto

type InfluxResult struct {
	SuccessCount int
	FailCount    int
	ErrorList    []string
}
