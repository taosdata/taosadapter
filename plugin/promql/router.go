package promql

import (
	"context"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/httputil"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/taosdata/taosadapter/db/async"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/plugin"
)

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"
)

type errorType string

const (
	errorNone        errorType = ""
	errorTimeout     errorType = "timeout"
	errorCanceled    errorType = "canceled"
	errorExec        errorType = "execution"
	errorBadData     errorType = "bad_data"
	errorInternal    errorType = "internal"
	errorUnavailable errorType = "unavailable"
	errorNotFound    errorType = "not_found"
)

var errorConnect = errors.New("connect to TDengine error")

type apiFunc func(c *gin.Context) apiFuncResult

func setUnavailStatusOnTSDBNotReady(r apiFuncResult) apiFuncResult {
	if r.err != nil && errors.Cause(r.err.err) == tsdb.ErrNotReady {
		r.err.typ = errorUnavailable
	}
	return r
}

type apiError struct {
	typ errorType
	err error
}
type apiFuncResult struct {
	data      interface{}
	err       *apiError
	warnings  storage.Warnings
	finalizer func()
}

type queryData struct {
	ResultType parser.ValueType  `json:"resultType"`
	Result     parser.Value      `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}
type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

func (p *Plugin) respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})

	if err != nil {
		logger.WithError(err).Errorln("error marshaling json response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = http.StatusUnprocessableEntity
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	case errorNotFound:
		code = http.StatusNotFound
	default:
		code = http.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if n, err := w.Write(b); err != nil {
		logger.WithError(err).Errorln("error writing response", "bytesWritten", n)
	}
}

func (p *Plugin) respond(w http.ResponseWriter, data interface{}, warnings storage.Warnings) {
	statusMessage := statusSuccess
	var warningStrings []string
	for _, warning := range warnings {
		warningStrings = append(warningStrings, warning.Error())
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:   statusMessage,
		Data:     data,
		Warnings: warningStrings,
	})
	if err != nil {
		logger.WithError(err).Errorln("error marshaling json response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		logger.WithError(err).Errorln("error writing response", "bytesWritten", n)
	}
}

func (p *Plugin) query(c *gin.Context) (result apiFuncResult) {
	db := c.Param("db")
	user, password, err := plugin.GetAuth(c)
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorInternal, errorConnect}, nil, nil}
	}
	defer taosConn.Put()
	err = async.GlobalAsync.TaosExecWithoutResult(taosConn.TaosConnection, "use "+db)
	if err != nil {
		err = errors.Wrapf(err, "invalid db")
		return apiFuncResult{nil, &apiError{errorInternal, err}, nil, nil}
	}
	r := c.Request
	ts, err := parseTimeParam(r, "time", time.Now())
	if err != nil {
		return invalidParamError(err, "time")
	}
	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := p.promController.NewInstantQuery(taosConn, r.FormValue("query"), ts)
	if err != nil {
		return invalidParamError(err, "query")
	}

	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	var qs *stats.QueryStats
	if r.FormValue("stats") != "" {
		qs = stats.NewQueryStats(qry.Stats())
	}

	return apiFuncResult{&queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}, nil, res.Warnings, qry.Close}
}

func (p *Plugin) queryRange(c *gin.Context) (result apiFuncResult) {
	db := c.Param("db")
	user, password, err := plugin.GetAuth(c)
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorInternal, errorConnect}, nil, nil}
	}
	defer taosConn.Put()
	err = async.GlobalAsync.TaosExecWithoutResult(taosConn.TaosConnection, "use "+db)
	if err != nil {
		err = errors.Wrapf(err, "invalid db")
		return apiFuncResult{nil, &apiError{errorInternal, err}, nil, nil}
	}
	r := c.Request
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return invalidParamError(err, "end")
	}
	if end.Before(start) {
		return invalidParamError(errors.New("end timestamp must not be before start time"), "end")
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return invalidParamError(err, "step")
	}

	if step <= 0 {
		return invalidParamError(errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer"), "step")
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return invalidParamError(err, "timeout")
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := p.promController.NewRangeQuery(taosConn, r.FormValue("query"), start, end, step)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call qry.Close ourselves (which is
	// required in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			qry.Close()
		}
	}()

	ctx = httputil.ContextFromRequest(ctx, r)

	res := qry.Exec(ctx)
	if res.Err != nil {
		return apiFuncResult{nil, returnAPIError(res.Err), res.Warnings, qry.Close}
	}

	// Optional stats field in response if parameter "stats" is not empty.
	var qs *stats.QueryStats
	if r.FormValue("stats") != "" {
		qs = stats.NewQueryStats(qry.Stats())
	}

	return apiFuncResult{&queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      qs,
	}, nil, res.Warnings, qry.Close}
}

func (p *Plugin) labelNames(c *gin.Context) apiFuncResult {
	db := c.Param("db")
	user, password, err := plugin.GetAuth(c)
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorInternal, errorConnect}, nil, nil}
	}
	defer taosConn.Put()
	err = async.GlobalAsync.TaosExecWithoutResult(taosConn.TaosConnection, "use "+db)
	if err != nil {
		err = errors.Wrapf(err, "invalid db")
		return apiFuncResult{nil, &apiError{errorInternal, err}, nil, nil}
	}
	r := c.Request
	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		return invalidParamError(err, "end")
	}

	matcherSets, err := parseMatchersParam(r.Form["match[]"])
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	q, err := p.promController.Querier(taosConn, r.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	defer q.Close()

	var (
		names    []string
		warnings storage.Warnings
	)
	if len(matcherSets) > 0 {
		labelNamesSet := make(map[string]struct{})

		for _, matchers := range matcherSets {
			vals, callWarnings, err := q.LabelNames(matchers...)
			if err != nil {
				return apiFuncResult{nil, &apiError{errorExec, err}, warnings, nil}
			}

			warnings = append(warnings, callWarnings...)
			for _, val := range vals {
				labelNamesSet[val] = struct{}{}
			}
		}

		// Convert the map to an array.
		names = make([]string, 0, len(labelNamesSet))
		for key := range labelNamesSet {
			names = append(names, key)
		}
		sort.Strings(names)
	} else {
		names, warnings, err = q.LabelNames()
		if err != nil {
			return apiFuncResult{nil, &apiError{errorExec, err}, warnings, nil}
		}
	}

	if names == nil {
		names = []string{}
	}
	return apiFuncResult{names, nil, warnings, nil}
}

func (p *Plugin) labelValues(c *gin.Context) (result apiFuncResult) {
	db := c.Param("db")
	user, password, err := plugin.GetAuth(c)
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorInternal, errorConnect}, nil, nil}
	}
	defer taosConn.Put()
	err = async.GlobalAsync.TaosExecWithoutResult(taosConn.TaosConnection, "use "+db)
	if err != nil {
		err = errors.Wrapf(err, "invalid db")
		return apiFuncResult{nil, &apiError{errorInternal, err}, nil, nil}
	}
	r := c.Request
	name := c.Param("name")

	if !model.LabelNameRE.MatchString(name) {
		return apiFuncResult{nil, &apiError{errorBadData, errors.Errorf("invalid label name: %q", name)}, nil, nil}
	}

	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		return invalidParamError(err, "end")
	}

	matcherSets, err := parseMatchersParam(r.Form["match[]"])
	if err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, err}, nil, nil}
	}

	q, err := p.promController.Querier(taosConn, r.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call q.Close ourselves (which is required
	// in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			q.Close()
		}
	}()
	closer := func() {
		q.Close()
	}

	var (
		vals     []string
		warnings storage.Warnings
	)
	if len(matcherSets) > 0 {
		var callWarnings storage.Warnings
		labelValuesSet := make(map[string]struct{})
		for _, matchers := range matcherSets {
			vals, callWarnings, err = q.LabelValues(name, matchers...)
			if err != nil {
				return apiFuncResult{nil, &apiError{errorExec, err}, warnings, closer}
			}
			warnings = append(warnings, callWarnings...)
			for _, val := range vals {
				labelValuesSet[val] = struct{}{}
			}
		}

		vals = make([]string, 0, len(labelValuesSet))
		for val := range labelValuesSet {
			vals = append(vals, val)
		}
	} else {
		vals, warnings, err = q.LabelValues(name)
		if err != nil {
			return apiFuncResult{nil, &apiError{errorExec, err}, warnings, closer}
		}

		if vals == nil {
			vals = []string{}
		}
	}

	sort.Strings(vals)

	return apiFuncResult{vals, nil, warnings, closer}
}

func (p *Plugin) series(c *gin.Context) (result apiFuncResult) {
	db := c.Param("db")
	user, password, err := plugin.GetAuth(c)
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		return apiFuncResult{nil, &apiError{errorInternal, errorConnect}, nil, nil}
	}
	defer taosConn.Put()
	err = async.GlobalAsync.TaosExecWithoutResult(taosConn.TaosConnection, "use "+db)
	if err != nil {
		err = errors.Wrapf(err, "invalid db")
		return apiFuncResult{nil, &apiError{errorInternal, err}, nil, nil}
	}
	r := c.Request
	if err := r.ParseForm(); err != nil {
		return apiFuncResult{nil, &apiError{errorBadData, errors.Wrapf(err, "error parsing form values")}, nil, nil}
	}
	if len(r.Form["match[]"]) == 0 {
		return apiFuncResult{nil, &apiError{errorBadData, errors.New("no match[] parameter provided")}, nil, nil}
	}

	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		return invalidParamError(err, "start")
	}
	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		return invalidParamError(err, "end")
	}

	matcherSets, err := parseMatchersParam(r.Form["match[]"])
	if err != nil {
		return invalidParamError(err, "match[]")
	}

	q, err := p.promController.Querier(taosConn, r.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return apiFuncResult{nil, &apiError{errorExec, err}, nil, nil}
	}
	// From now on, we must only return with a finalizer in the result (to
	// be called by the caller) or call q.Close ourselves (which is required
	// in the case of a panic).
	defer func() {
		if result.finalizer == nil {
			q.Close()
		}
	}()
	closer := func() {
		q.Close()
	}

	hints := &storage.SelectHints{
		Start: timestamp.FromTime(start),
		End:   timestamp.FromTime(end),
		Func:  "series", // There is no series function, this token is used for lookups that don't need samples.
	}

	var sets []storage.SeriesSet
	for _, mset := range matcherSets {
		// We need to sort this select results to merge (deduplicate) the series sets later.
		s := q.Select(true, hints, mset...)
		sets = append(sets, s)
	}

	set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
	metrics := []labels.Labels{}
	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}

	warnings := set.Warnings()
	if set.Err() != nil {
		return apiFuncResult{nil, &apiError{errorExec, set.Err()}, warnings, closer}
	}

	return apiFuncResult{metrics, nil, warnings, closer}
}

var (
	minTime          = time.Unix(0, 0).UTC()
	maxTime          = time.Unix(0, math.MaxInt64).UTC()
	minTimeFormatted = "1970-01-01T00:00:00.0Z"
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "Invalid time value for '%s'", paramName)
	}
	return result, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("cannot parse %q to a valid duration", s)
}

func returnAPIError(err error) *apiError {
	if err == nil {
		return nil
	}

	switch errors.Cause(err).(type) {
	case promql.ErrQueryCanceled:
		return &apiError{errorCanceled, err}
	case promql.ErrQueryTimeout:
		return &apiError{errorTimeout, err}
	case promql.ErrStorage:
		return &apiError{errorInternal, err}
	}

	return &apiError{errorExec, err}
}

func parseMatchersParam(matchers []string) ([][]*labels.Matcher, error) {
	var matcherSets [][]*labels.Matcher
	for _, s := range matchers {
		matchers, err := parser.ParseMetricSelector(s)
		if err != nil {
			return nil, err
		}
		matcherSets = append(matcherSets, matchers)
	}

OUTER:
	for _, ms := range matcherSets {
		for _, lm := range ms {
			if lm != nil && !lm.Matches("") {
				continue OUTER
			}
		}
		return nil, errors.New("match[] must contain at least one non-empty matcher")
	}
	return matcherSets, nil
}

func invalidParamError(err error, parameter string) apiFuncResult {
	return apiFuncResult{nil, &apiError{
		errorBadData, errors.Wrapf(err, "invalid parameter %q", parameter),
	}, nil, nil}
}
