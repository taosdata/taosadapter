package config

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/spf13/viper"
)

type Reject struct {
	sync.RWMutex
	rejectQuerySqlRegex []*regexp.Regexp
}

func (r *Reject) SetValue(v *viper.Viper) error {
	regexpStringSlice := v.GetStringSlice("rejectQuerySqlRegex")
	regexpSlice := make([]*regexp.Regexp, len(regexpStringSlice))
	if len(regexpStringSlice) > 0 {
		var err error
		for i := 0; i < len(regexpStringSlice); i++ {
			regexpSlice[i], err = regexp.Compile(regexpStringSlice[i])
			if err != nil {
				return fmt.Errorf("rejectQuerySqlRegex regexp compile error, string: %s, error: %s", regexpStringSlice[i], err)
			}
		}
	}
	r.Lock()
	defer r.Unlock()
	r.rejectQuerySqlRegex = regexpSlice
	return nil
}

func (r *Reject) GetRejectQuerySqlRegex() []*regexp.Regexp {
	r.RLock()
	defer r.RUnlock()
	return r.rejectQuerySqlRegex
}

func initRejectConfig(v *viper.Viper) {
	v.SetDefault("rejectQuerySqlRegex", nil)
}
