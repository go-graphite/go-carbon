package carbon

import (
	"os"
	"strings"

	"github.com/lomik/go-carbon/persister"
)

type App struct {
	ConfigFilename string
	Config         *Config
	Schemas        *persister.WhisperSchemas
	Aggregation    *persister.WhisperAggregation
}

// New App instance
func New(configFilename string) *App {
	app := &App{
		ConfigFilename: configFilename,
		Config:         NewConfig(),
	}
	return app
}

// ParseConfig loads config from app.ConfigFilename
func (app *App) ParseConfig() error {
	cfg := NewConfig()
	if err := ParseConfig(app.ConfigFilename, cfg); err != nil {
		return err
	}

	// carbon-cache prefix
	if hostname, err := os.Hostname(); err == nil {
		hostname = strings.Replace(hostname, ".", "_", -1)
		cfg.Common.GraphPrefix = strings.Replace(cfg.Common.GraphPrefix, "{host}", hostname, -1)
	} else {
		cfg.Common.GraphPrefix = strings.Replace(cfg.Common.GraphPrefix, "{host}", "localhost", -1)
	}

	app.Config = cfg
	return nil
}

// ParseWhisperConf parse schemas.conf and aggregation.conf
func (app *App) ParseWhisperConf() error {
	var err error
	var newSchemas *persister.WhisperSchemas
	var newAggregation *persister.WhisperAggregation

	if app.Config.Whisper.Enabled {
		newSchemas, err = persister.ReadWhisperSchemas(app.Config.Whisper.Schemas)
		if err != nil {
			return err
		}

		if app.Config.Whisper.Aggregation != "" {
			newAggregation, err = persister.ReadWhisperAggregation(app.Config.Whisper.Aggregation)
			if err != nil {
				return err
			}
		} else {
			newAggregation = persister.NewWhisperAggregation()
		}
	}

	app.Schemas = newSchemas
	app.Aggregation = newAggregation

	return nil
}

// ReloadConfig reloads some settings from config
func (app *App) ReloadConfig() error {
	return nil
}
