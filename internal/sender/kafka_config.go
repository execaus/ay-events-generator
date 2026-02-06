package sender

type KafkaConfig struct {
	Broker string `yaml:"broker"`
	Topic  string `yaml:"topic"`
}
