package main

import (
	"encoding/json"
	"errors"

	"github.com/philchia/agollo/v3"
	log "github.com/sirupsen/logrus"
)

var (
	errConfig = errors.New("config error")
	apolloTmp = "/tmp"
)

func parseApollo(apolloConfigServer, apolloCluster, apolloAppID, apolloNamespaceConfig string, f func(map[string]string) error) error {
	// apollo config
	cf := agollo.Conf{
		MetaAddr: apolloConfigServer,
		Cluster:  apolloCluster,
		AppID:    apolloAppID,
		CacheDir: apolloTmp,
		NameSpaceNames: []string{
			apolloNamespaceConfig,
		},
	}

	if err := agollo.StartWithConf(&cf); err != nil {
		log.Warn("Connect Apollo failed")
		return err
	}

	err := loadConfig(apolloNamespaceConfig, f)
	if err != nil {
		return err
	}

	go watchApollo(f)

	return nil
}

func watchApollo(f func(map[string]string) error) {
	for {
		select {
		case ev := <-agollo.WatchUpdate():
			log.Info("get apollo update")
			if err := loadConfig(ev.Namespace, f); err != nil {
				log.Error("Error reloading config")
				return
			}
		}
	}
}

func closeApollo() error {
	if err := agollo.Stop(); err != nil {
		return err
	}
	return nil
}

func loadConfig(ns string, f func(map[string]string) error) error {

	content := agollo.GetNameSpaceContent(ns, "")

	var d map[string]string

	if err := json.Unmarshal([]byte(content), &d); err != nil {
		return err
	}

	if err := f(d); err != nil {
		return err
	}

	return nil

}
