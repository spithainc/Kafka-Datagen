package src

var Module *ConfigModule

// In order to handle empty values, a string format is necessary, since the default value for integers is 0.
type ConfigModule struct {
	BootstrapServer string         `yaml:"bootstrap-server"`
	Producer        ProducerModule `yaml:"producer"`
	Topic           TopicModule    `yaml:"topic"`
	Datagen         DatagenModule  `yaml:"datagen"`
}

type ProducerModule struct {
	MaxMessageBytes string `yaml:"max-message-bytes"`
	Lingers         string `yaml:"lingers"`
	CompressionType string `yaml:"compression-type"`
	Sasl            struct {
		Mechanism          string `yaml:"mechanism"` // sasl, plain
		Username           string `yaml:"username"`
		Password           string `yaml:"password"`
		AwsAccessKeyId     string `yaml:"aws-access-key-id"` // aws iam
		AwsSecretAccessKey string `yaml:"aws-secret-access-key"`
		ClientId           string `yaml:"client-id"` // oatuh
		ClientSecret       string `yaml:"client-secret"`
		TokenEndpoint      string `yaml:"token-endpoint"`
		KerberosConfig     string `yaml:"kerberos-config-path"` // kerberos
		KeyTab             string `yaml:"keytab-path"`
		Realm              string `yaml:"realm"`
		Servicename        string `yaml:"servicename"`
	} `yaml:"sasl"`
	Tls struct {
		Certfile   string `yaml:"certfile"`
		Keyfile    string `yaml:"keyfile"`
		Cafile     string `yaml:"cafile"`
		SkipVerify bool   `yaml:"skipverify"`
	} `yaml:"tls"`
}

type TopicModule struct {
	Name          string `yaml:"name"`
	Partition     string `yaml:"partition"`
	Replicafactor string `yaml:"replica-factor"`
}

type DatagenModule struct {
	QuickStart               string `yaml:"quickstart"`
	GoRoutine                string `yaml:"go-routine"`
	RatePerSecond            string `yaml:"rate-per-second"`
	Interval                 string `yaml:"interval"`
	MessageBytes             string `yaml:"message-bytes"`
	LimitDataAmountPerSecond string `yaml:"limit-data-amount-per-second"`
	RatePerSecondJitter      string `yaml:"rate-per-second-jitter"`
}
