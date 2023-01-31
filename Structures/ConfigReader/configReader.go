package configreader

import (
	"io/ioutil"
	"log"
	"time"

	"gopkg.in/yaml.v2"
)

type ConfigReader struct {
	WalSize             int           `yaml:"wal_size"`
	MemtableTrashold    float64       `yaml:"memtable_trashold"`
	MemtableSize        int           `yaml:"memtable_size"`
	MemtableStructure   string        `yaml:"memtable_structure"`
	Compaction          string        `yaml:"compaction"`
	LSMLevelMax         int           `yaml:"lsm_level_max"`
	TokenBucketCapacity int           `yaml:"token_bucket_capacity"`
	TokenBucketDuration time.Duration `yaml:"token_bucket_duration"`
}

func (config *ConfigReader) ReadConfig() {
	configData, err := ioutil.ReadFile("./Data/ConfigurationFile/configuration.yaml")
	if err != nil || len(configData) == 0 {
		config.WalSize = 10
		config.MemtableSize = 10
		config.MemtableTrashold = 0.8
		config.MemtableStructure = "btree"
		config.Compaction = "size-tired"
		config.LSMLevelMax = 4
		config.TokenBucketCapacity = 3
		config.TokenBucketDuration = 3000000000
	} else {
		err := yaml.Unmarshal(configData, &config)
		if err != nil {
			log.Fatal(err)
		}
	}
	// fmt.Println(config.Compaction)
}
