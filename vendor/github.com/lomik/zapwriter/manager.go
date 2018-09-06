package zapwriter

import (
	"fmt"
	"net/url"
	"strings"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _mutex sync.RWMutex
var _manager Manager

func init() {
	replaceGlobalManager(&manager{
		writers: make(map[string]WriteSyncer),
		cores:   make(map[string][]zapcore.Core),
		loggers: make(map[string]*zap.Logger),
	})
}

func replaceGlobalManager(m Manager) Manager {
	_mutex.Lock()
	prev := _manager
	_manager = m
	zap.ReplaceGlobals(_manager.Default())
	_mutex.Unlock()
	return prev
}

func CheckConfig(conf []Config, allowNames []string) error {
	_, err := makeManager(conf, true, allowNames)
	return err
}

func ApplyConfig(conf []Config) error {
	m, err := NewManager(conf)
	if err != nil {
		return err
	}

	replaceGlobalManager(m)

	return nil
}

func Default() *zap.Logger {
	_mutex.RLock()
	m := _manager
	_mutex.RUnlock()
	return m.Default()
}

func Logger(logger string) *zap.Logger {
	_mutex.RLock()
	m := _manager
	_mutex.RUnlock()
	return m.Logger(logger)
}

type Manager interface {
	Default() *zap.Logger
	Logger(logger string) *zap.Logger
}

type manager struct {
	writers map[string]WriteSyncer    // path -> writer
	cores   map[string][]zapcore.Core // logger name -> cores
	loggers map[string]*zap.Logger    // logger name -> logger
}

func NewManager(conf []Config) (Manager, error) {
	return makeManager(conf, false, nil)
}

func (m *manager) Default() *zap.Logger {
	if logger, ok := m.loggers[""]; ok {
		return logger
	}
	return zap.NewNop()
}

func (m *manager) Logger(name string) *zap.Logger {
	if logger, ok := m.loggers[name]; ok {
		return logger.Named(name)
	}
	return m.Default().Named(name)
}

func makeManager(conf []Config, checkOnly bool, allowNames []string) (Manager, error) {
	// check names
	if allowNames != nil {
		namesMap := make(map[string]bool)
		namesMap[""] = true
		for _, s := range allowNames {
			namesMap[s] = true
		}

		for _, cfg := range conf {
			if !namesMap[cfg.Logger] {
				return nil, fmt.Errorf("unknown logger name %#v", cfg.Logger)
			}
		}
	}

	// check config params
	for _, cfg := range conf {
		_, _, err := cfg.encoder()
		if err != nil {
			return nil, err
		}
	}

	// check complete
	if checkOnly {
		return nil, nil
	}

	m := &manager{
		writers: make(map[string]WriteSyncer),
		cores:   make(map[string][]zapcore.Core),
		loggers: make(map[string]*zap.Logger),
	}

	// create writers and cores
	for _, cfg := range conf {
		u, err := url.Parse(cfg.File)
		if err != nil {
			return nil, err
		}

		if _, ok := m.cores[cfg.Logger]; !ok {
			m.cores[cfg.Logger] = make([]zapcore.Core, 0)
		}

		if strings.ToLower(u.Path) == "none" {
			m.cores[cfg.Logger] = append(m.cores[cfg.Logger], zapcore.NewNopCore())
			continue
		}

		encoder, atomicLevel, err := cfg.encoder()
		if err != nil {
			return nil, err
		}

		ws, ok := m.writers[u.Path]
		if !ok {
			ws, err = New(cfg.File)
			if err != nil {
				return nil, err
			}
			m.writers[u.Path] = ws
		}

		core, err := cfg.core(encoder, ws, atomicLevel)
		if err != nil {
			return nil, err
		}

		m.cores[cfg.Logger] = append(m.cores[cfg.Logger], core)
	}

	// make loggers
	for k, cores := range m.cores {
		m.loggers[k] = zap.New(zapcore.NewTee(cores...))
	}

	return m, nil
}
