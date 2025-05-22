package paiabtest

import (
	"fmt"
	"os"

	"github.com/alibaba/pairec/v2/log" // Assuming pairec's logger can be reused
	// Placeholder for the actual PAI ABTest SDK client package
	// The SDK path is: github.com/aliyun/aliyun-pai-ab-go-sdk
	// We'll need to import specific subpackages from it, likely one for 'experiments' and one for 'api' or 'model'
	// For now, let's use a generic import and refine later if needed by the SDK's structure.
	paiABApi "github.com/aliyun/aliyun-pai-ab-go-sdk/api"
	paiABClient "github.com/aliyun/aliyun-pai-ab-go-sdk/experiments"
	paiModel "github.com/aliyun/aliyun-pai-ab-go-sdk/model" // Added for PAI ABTest models
)

var (
	paiExperimentClient    *paiABClient.ExperimentClient // Type will depend on actual SDK
	isPaiClientInitialized bool                          = false
)

// LoadFromEnvironment initializes the PAI A/B Test client using environment variables.
func LoadFromEnvironment() {
	accessKeyId := os.Getenv("PAI_ABTEST_ACCESS_KEY_ID")
	accessKeySecret := os.Getenv("PAI_ABTEST_ACCESS_KEY_SECRET")
	region := os.Getenv("PAI_ABTEST_REGION")
	// TODO: Add environment variable for PAI_ABTEST_PROJECT_NAME or similar if needed for MatchPaiExperiment later
	// projectName := os.Getenv("PAI_ABTEST_PROJECT_NAME")

	if accessKeyId == "" {
		panic("environment variable PAI_ABTEST_ACCESS_KEY_ID is empty")
	}
	if accessKeySecret == "" {
		panic("environment variable PAI_ABTEST_ACCESS_KEY_SECRET is empty")
	}
	if region == "" {
		panic("environment variable PAI_ABTEST_REGION is empty")
	}

	// Configure logging (similar to abtest/abtest.go)
	// The PAI AB SDK might have its own way to set a logger, adjust as needed.
	// pairecLogger := log.ABTestLogger{}

	// Initialize the PAI A/B Test client
	// This is based on the worker's report: api.NewConfiguration and experiments.NewExperimentClient
	// The exact calls and parameters might need adjustment once we have the SDK locally.
	// Example:
	config := paiABApi.NewConfiguration(region, accessKeyId, accessKeySecret)

	client, err := paiABClient.NewExperimentClient(config) // This is a guess based on the name pattern
	if err != nil {
		panic(fmt.Sprintf("Failed to create PAI A/B Test client: %v", err))
	}

	paiExperimentClient = client
	isPaiClientInitialized = true
	log.Info("PAI A/B Test client initialized successfully.")
}

// GetClient returns the initialized PAI A/B Test client.
func GetClient() *paiABClient.ExperimentClient { // Type will depend on actual SDK
	if !isPaiClientInitialized {
		// Or handle this case more gracefully, perhaps by attempting a load or returning an error
		log.Warning("PAI A/B Test client accessed before initialization.")
	}
	return paiExperimentClient
}

// MatchPaiExperiment retrieves experiment parameters from PAI A/B Test.
// The signature will depend on the PAI A/B SDK.
// It will need to take parameters like project name, domain name, layer id, and user context.
// MatchPaiExperiment retrieves experiment parameters from PAI A/B Test.
func MatchPaiExperiment(uid string, requestId string, sceneParams map[string]interface{}) (*paiModel.ExperimentResult, error) {
	if !isPaiClientInitialized || paiExperimentClient == nil {
		return nil, fmt.Errorf("PAI A/B Test client not initialized")
	}

	projectName := os.Getenv("PAI_ABTEST_PROJECT_NAME")
	domainName := os.Getenv("PAI_ABTEST_DOMAIN_NAME") // Read but not directly used in MatchExperiment call
	layerId := os.Getenv("PAI_ABTEST_LAYER_ID")       // Read but not directly used in MatchExperiment call

	if projectName == "" {
		panic("environment variable PAI_ABTEST_PROJECT_NAME is empty")
	}
	if domainName == "" {
		// Panic as per requirement, even if not directly used in this specific SDK call
		panic("environment variable PAI_ABTEST_DOMAIN_NAME is empty")
	}
	if layerId == "" {
		// Panic as per requirement, even if not directly used in this specific SDK call
		panic("environment variable PAI_ABTEST_LAYER_ID is empty")
	}

	paiContext := paiModel.ExperimentContext{
		RequestId:    requestId,
		Uid:          uid,
		FilterParams: sceneParams, // PAI SDK expects map[string]interface{}
	}

	// The PAI AB SDK's MatchExperiment method takes ProjectName and ExperimentContext.
	// DomainName and LayerId are typically part of the experiment configuration within the PAI platform.
	experimentResult := paiExperimentClient.MatchExperiment(projectName, &paiContext)

	if experimentResult == nil {
		// The PAI SDK's MatchExperiment returns *model.ExperimentResult, so a nil result might indicate an error
		// or simply no experiment matched. The SDK documentation should clarify error handling.
		// For now, we'll assume nil means no experiment or an issue, and the caller should check.
		// If the SDK itself can return an error from MatchExperiment, that should be handled.
		// Based on the SDK documentation (client.go), MatchExperiment does not return an error, only *model.ExperimentResult.
		// We might need to check experimentResult.Error() or similar if the result object carries error info.
		// For now, returning nil result and nil error if no experiment matched, or if an error occurred and is embedded.
		// The PAI SDK example shows checking params like:
		// param := experimentResult.GetExperimentParams().GetString("ab_param_name", "not_exist")
		// if param == "not_exist" { // default logic }
		// This implies a non-nil result is returned even if no params are found.
		// Let's log if the result itself is nil, as this would be unexpected based on the SDK example.
		log.Warning(fmt.Sprintf("PAI A/B Test MatchExperiment returned nil result for project: %s, uid: %s", projectName, uid))
		return nil, fmt.Errorf("PAI A/B Test MatchExperiment returned nil result for project: %s", projectName)
	}

	// Log experiment details if available and not nil
	// The Info() method is available on the PAI SDK's ExperimentResult
	log.Info(fmt.Sprintf("PAI A/B Test MatchExperiment result: %s for project: %s, uid: %s", experimentResult.Info(), projectName, uid))


	return experimentResult, nil
}
