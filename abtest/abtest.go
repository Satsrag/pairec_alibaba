package abtest

import (
	"log"
	"os"

	"github.com/aliyun/aliyun-pairec-config-go-sdk/v2/experiments"
	"github.com/aliyun/aliyun-pairec-config-go-sdk/v2/model"

	defaultabtest "github.com/alibaba/pairec/v2/abtest/default"
	paiabtest "github.com/alibaba/pairec/v2/abtest/pai"
)

var experimentClient *experiments.ExperimentClient

// LoadFromEnvironment create abtest instance use env, env list:
//
// ENV params list:
//
//	PAIREC_ENVIRONMENT is the environment type, valid values are: daily, prepub, product
//	PAIABTEST_ENVIRONMENT is the environment type, valid values are: daily, prepub, product
func LoadFromEnvironment() {
	env := os.Getenv("PAIREC_ENVIRONMENT")
	if env != "" {
		experimentClient = defaultabtest.NewDefaultAbTestClient()
	} else {
		env = os.Getenv("PAIABTEST_ENVIRONMENT")
		if env != "" {
			experimentClient = paiabtest.NewPaiAbTestClient()
		}
	}
	if experimentClient == nil {
		log.Println("not found abtest client")
	}
}

func GetExperimentClient() *experiments.ExperimentClient {
	return experimentClient
}

func GetParams(sceneName string) model.SceneParams {
	return experimentClient.GetSceneParams(sceneName)
}
