package config

import (
	"log"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type TMQ struct {
	ReleaseIntervalMultiplierForAutocommit int
}

func initTMQ() {
	viper.SetDefault("tmq.releaseIntervalMultiplierForAutocommit", 2)
	_ = viper.BindEnv("tmq.releaseIntervalMultiplierForAutocommit", "TAOS_ADAPTER_TMQ_RELEASE_INTERVAL_MULTIPLIER_FOR_AUTOCOMMIT")
	pflag.Int("tmq.releaseIntervalMultiplierForAutocommit", 2, `When set to autocommit, the interval for message release is a multiple of the autocommit interval, with a default value of 2 and a minimum value of 1 and a maximum value of 10. Env "TAOS_ADAPTER_TMQ_RELEASE_INTERVAL_MULTIPLIER_FOR_AUTOCOMMIT"`)
}

func (t *TMQ) setValue() {
	releaseIntervalMultiplierForAutocommit := viper.GetInt("tmq.releaseIntervalMultiplierForAutocommit")
	if releaseIntervalMultiplierForAutocommit < 1 || releaseIntervalMultiplierForAutocommit > 10 {
		log.Panicf("invalid parameter `tmq.releaseIntervalMultiplierForAutocommit`")
	}
	t.ReleaseIntervalMultiplierForAutocommit = releaseIntervalMultiplierForAutocommit
}
