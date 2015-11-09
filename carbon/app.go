package carbon

import "github.com/lomik/go-carbon/persister"

type App struct {
	Config      *Config
	Schemas     *persister.WhisperSchemas
	Aggregation *persister.WhisperAggregation
}

// New App instance
func New(config *Config) *App {
	app := &App{
		Config: config,
	}
	return app
}

// ParseWhisperConf parse schemas.conf and aggregation.conf
func (app *App) ParseWhisperConf() error {
	var err error
	if app.Config.Whisper.Enabled {
		app.Schemas, err = persister.ReadWhisperSchemas(app.Config.Whisper.Schemas)
		if err != nil {
			return err
		}

		if app.Config.Whisper.Aggregation != "" {
			app.Aggregation, err = persister.ReadWhisperAggregation(app.Config.Whisper.Aggregation)
			if err != nil {
				return err
			}
		} else {
			app.Aggregation = persister.NewWhisperAggregation()
		}
	}
	return err
}
