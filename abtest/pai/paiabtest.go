package pai

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"

	paiabtestapi "github.com/aliyun/aliyun-pai-ab-go-sdk/api"
	paiabtestexperiments "github.com/aliyun/aliyun-pai-ab-go-sdk/experiments"
	paiabtestmodel "github.com/aliyun/aliyun-pai-ab-go-sdk/model"

	"github.com/aliyun/aliyun-pairec-config-go-sdk/v2/common"
	"github.com/aliyun/aliyun-pairec-config-go-sdk/v2/experiments"
	pairecmodel "github.com/aliyun/aliyun-pairec-config-go-sdk/v2/model"
)

// PaiAbTestClient 包装了Pairec ExperimentClient并提供定期更新功能
type PaiAbTestClient struct {
	*experiments.ExperimentClient
	paiClient *paiabtestexperiments.ExperimentClient
}

func NewPaiAbTestClient() *experiments.ExperimentClient {
	log.Printf("[PAI-AB] Initializing PAI AB Test Client...")

	// init config
	env := os.Getenv("PAIABTEST_ENVIRONMENT")
	if env == "" {
		panic("env PAIABTEST_ENVIRONMENT empty")
	}
	log.Printf("[PAI-AB] Environment: %s", env)

	region := os.Getenv("REGION")
	if region == "" {
		panic("env REGION empty")
	}
	log.Printf("[PAI-AB] Region: %s", region)

	accessId := os.Getenv("AccessKey")
	if accessId == "" {
		panic("env AccessKey empty")
	}
	log.Printf("[PAI-AB] AccessKey configured (length: %d)", len(accessId))

	accessSecret := os.Getenv("AccessSecret")
	if accessSecret == "" {
		panic("env AccessSecret empty")
	}
	log.Printf("[PAI-AB] AccessSecret configured (length: %d)", len(accessSecret))
	config := paiabtestapi.NewConfiguration(region, accessId, accessSecret)

	// init client
	paiAbTestClient, err := paiabtestexperiments.NewExperimentClient(config, paiabtestexperiments.WithLogger(paiabtestexperiments.LoggerFunc(log.Printf)))
	if err != nil {
		log.Printf("[PAI-AB] Failed to create PAI AB Test client: %v", err)
		log.Fatal(err)
	}
	log.Printf("[PAI-AB] PAI AB Test SDK client created successfully")

	client := &experiments.ExperimentClient{
		Environment: env,
		SceneMap:    make(map[string]*pairecmodel.Scene, 0),
	}

	paiClient := &PaiAbTestClient{
		ExperimentClient: client,
		paiClient:        paiAbTestClient,
	}

	log.Printf("[PAI-AB] Starting initial scene map synchronization...")
	paiClient.changePaiABTestProjectToDefaultTestSceneMap()

	log.Printf("[PAI-AB] Starting periodic update goroutine (interval: 1 minute)...")
	go paiClient.loopChangePaiABTestProjectToDefaultTestSceneMap()

	log.Printf("[PAI-AB] PAI AB Test Client initialization completed")
	return paiClient.ExperimentClient
}

func (c *PaiAbTestClient) loopChangePaiABTestProjectToDefaultTestSceneMap() {
	log.Printf("[PAI-AB] Periodic update loop started")

	for {
		time.Sleep(time.Minute)
		log.Printf("[PAI-AB] Starting periodic scene map sync...")
		start := time.Now()
		c.changePaiABTestProjectToDefaultTestSceneMap()
		duration := time.Since(start)
		log.Printf("[PAI-AB] Periodic scene map sync completed in %v", duration)
	}
}

func (c *PaiAbTestClient) changePaiABTestProjectToDefaultTestSceneMap() {
	log.Printf("[PAI-AB] Starting scene map synchronization...")

	// 使用反射获取PAI SDK客户端的私有projectMap字段
	paiClientValue := reflect.ValueOf(c.paiClient).Elem()
	projectMapField := paiClientValue.FieldByName("projectMap")

	if !projectMapField.IsValid() {
		log.Printf("[PAI-AB] ERROR: Failed to access projectMap field via reflection")
		return
	}
	log.Printf("[PAI-AB] Successfully accessed projectMap field")

	// SceneMap是公开字段，可以直接访问
	if c.ExperimentClient.SceneMap == nil {
		c.ExperimentClient.SceneMap = make(map[string]*pairecmodel.Scene)
		log.Printf("[PAI-AB] Initialized empty SceneMap")
	}

	// 转换projectMap到SceneMap，只处理以"pairec_"开头的项目
	// 使用unsafe指针直接访问私有字段
	projectMapPtr := unsafe.Pointer(projectMapField.UnsafeAddr())
	projectMap := *(*map[string]*paiabtestmodel.Project)(projectMapPtr)
	if projectMap != nil {
		// 创建新的临时SceneMap，避免在转换过程中影响现有的SceneMap
		newSceneMap := make(map[string]*pairecmodel.Scene, 0)
		convertedCount := 0

		for projectName, project := range projectMap {
			// 只转换以"pairec_"开头的项目
			if !strings.HasPrefix(projectName, "pairec_") {
				continue
			}

			// 创建对应的Scene对象
			scene := &pairecmodel.Scene{
				SceneId:         int64(project.ExpProjectId),                            // 项目ID映射到场景ID
				SceneName:       strings.Replace(project.ProjectName, "pairec_", "", 1), // 移除"pairec_"前缀
				SceneInfo:       project.ProjectInfo,                                    // 项目信息映射到场景信息
				ExperimentRooms: make([]*pairecmodel.ExperimentRoom, 0),
			}

			// 遍历project中的所有domain，将其转换为ExperimentRoom
			if project.DefaultDomain() != nil {
				room := convertDomainToRoom(project.DefaultDomain(), scene.SceneId)
				if room != nil {
					scene.AddExperimentRoom(room)
				}
			}

			// 遍历其他domains
			for _, domain := range project.GetDomains() {
				if domain != project.DefaultDomain() {
					room := convertDomainToRoom(domain, scene.SceneId)
					if room != nil {
						scene.AddExperimentRoom(room)
					}
				}
			}

			// 统计Scene中的ExperimentGroup数量
			totalExpGroups := 0
			for _, room := range scene.ExperimentRooms {
				for _, layer := range room.Layers {
					totalExpGroups += len(layer.ExperimentGroups)
				}
			}
			log.Printf("[PAI-AB] Scene '%s' created with %d ExperimentGroups total", projectName, totalExpGroups)

			// 添加到临时SceneMap中
			newSceneMap[strings.Replace(projectName, "pairec_", "", 1)] = scene
			convertedCount++
		}

		// 一次性替换整个SceneMap，确保线程安全
		c.ExperimentClient.SceneMap = newSceneMap

		log.Printf("[PAI-AB] Successfully converted %d projects to scenes (filtered from %d total projects)", convertedCount, len(projectMap))
		log.Printf("[PAI-AB] Current SceneMap contains %d scenes", len(c.ExperimentClient.SceneMap))

		// 打印场景名称便于调试
		for sceneName := range newSceneMap {
			log.Printf("[PAI-AB] Scene available: %s", sceneName)
		}
	} else {
		log.Printf("[PAI-AB] ERROR: Failed to convert projectMap to expected type")
	}
}

// convertDomainToRoom 将PAI SDK中的Domain转换为Pairec SDK中的ExperimentRoom
func convertDomainToRoom(domain *paiabtestmodel.Domain, sceneId int64) *pairecmodel.ExperimentRoom {
	if domain == nil {
		log.Printf("[PAI-AB] WARN: Received nil domain for conversion")
		return nil
	}
	log.Printf("[PAI-AB] Converting domain '%s' (ID: %d) to ExperimentRoom", domain.ExpDomainName, domain.ExpDomainId)

	room := &pairecmodel.ExperimentRoom{
		ExpRoomId:      int64(domain.ExpDomainId),     // Domain ID映射到Room ID
		SceneId:        sceneId,                       // 关联到所属的Scene
		ExpRoomName:    domain.ExpDomainName,          // Domain名称映射到Room名称
		ExpRoomInfo:    domain.ExpDomainInfo,          // Domain信息映射到Room信息
		DebugUsers:     domain.DebugUsers,             // 调试用户
		BucketCount:    100,                           // 桶数量（默认100）
		ExpRoomBuckets: domain.Buckets,                // 桶配置
		BucketType:     uint32(domain.BucketType),     // 桶类型（转换为uint32）
		Filter:         domain.Filter,                 // 过滤条件
		Environment:    1,                             // 设置默认环境
		Type:           1,                             // 设置默认类型
		Status:         1,                             // 设置默认狀態
		Layers:         make([]*pairecmodel.Layer, 0), // 初始化Layers
	}

	// 转换Domain中的Layer到Room中的Layer
	for _, layer := range domain.Layers() {
		pairecLayer := convertLayerToPairecLayer(layer, room.ExpRoomId, sceneId)
		if pairecLayer != nil {
			room.AddLayer(pairecLayer)
		}
	}

	// 初始化room
	if err := room.Init(); err != nil {
		log.Printf("[PAI-AB] ERROR: Failed to initialize ExperimentRoom '%s': %v", room.ExpRoomName, err)
		return nil
	}

	log.Printf("[PAI-AB] Domain conversion completed: %d layers converted", len(room.Layers))
	return room
}

// convertLayerToPairecLayer 将PAI SDK中的Layer转换为Pairec SDK中的Layer
func convertLayerToPairecLayer(layer *paiabtestmodel.Layer, roomId int64, sceneId int64) *pairecmodel.Layer {
	if layer == nil {
		log.Printf("[PAI-AB] WARN: Received nil layer for conversion")
		return nil
	}
	log.Printf("[PAI-AB] Converting layer '%s' (ID: %d) to Pairec Layer", layer.LayerName, layer.ExpLayerId)

	pairecLayer := &pairecmodel.Layer{
		LayerId:          int64(layer.ExpLayerId),                 // Layer ID映射
		ExpRoomId:        roomId,                                  // 所属的Room ID
		SceneId:          sceneId,                                 // 所属的Scene ID
		LayerName:        layer.LayerName,                         // Layer名称
		LayerInfo:        layer.LayerInfo,                         // Layer信息
		ExperimentGroups: make([]*pairecmodel.ExperimentGroup, 0), // 初始化ExperimentGroups
	}

	// 转换Layer中的Experiment到ExperimentGroup
	log.Printf("[PAI-AB] Converting %d experiments in layer '%s'", len(layer.Experiments()), layer.LayerName)
	for _, experiment := range layer.Experiments() {
		expGroup := convertExperimentToGroup(experiment, pairecLayer.LayerId, roomId, sceneId)
		if expGroup != nil {
			pairecLayer.AddExperimentGroup(expGroup)
			log.Printf("[PAI-AB] Successfully added ExperimentGroup '%s' (ID: %d) to layer", expGroup.ExpGroupName, expGroup.ExpGroupId)
		} else {
			log.Printf("[PAI-AB] ERROR: Failed to convert experiment '%s' to ExperimentGroup", experiment.ExpName)
		}
	}

	log.Printf("[PAI-AB] Layer conversion completed: %d experiment groups converted", len(pairecLayer.ExperimentGroups))
	return pairecLayer
}

// determineCrowdTargetType 根据PAI实验配置确定合适的分流类型
func determineCrowdTargetType(experiment *paiabtestmodel.Experiment) string {
	// 如果有调试用户配置，优先使用调试模式
	if experiment.DebugUsers != "" {
		// TODO: 确认调试模式的正确常量，曾时使用Filter模式
		return common.CrowdTargetType_Filter
	}

	// 如果有过滤条件，使用过滤分流
	if experiment.Filter != "" {
		return common.CrowdTargetType_Filter
	}

	// 如果配置了桶(分流比例)，使用随机分流
	if experiment.Buckets != "" {
		return common.CrowdTargetType_Random
	}

	// 默认情况下对所有用户开放
	return common.CrowdTargetType_ALL
}

// convertExperimentToGroup 将PAI SDK中的Experiment转换为Pairec SDK中的ExperimentGroup
func convertExperimentToGroup(experiment *paiabtestmodel.Experiment, layerId int64, roomId int64, sceneId int64) *pairecmodel.ExperimentGroup {
	if experiment == nil {
		log.Printf("[PAI-AB] WARN: Received nil experiment for conversion")
		return nil
	}
	log.Printf("[PAI-AB] Converting experiment '%s' (ID: %d) to ExperimentGroup", experiment.ExpName, experiment.ExpId)

	// 确定分流类型
	crowdTargetType := determineCrowdTargetType(experiment)
	log.Printf("[PAI-AB] Experiment '%s' crowd target type: %s (DebugUsers: '%s', Filter: '%s', Buckets: '%s')",
		experiment.ExpName, crowdTargetType, experiment.DebugUsers, experiment.Filter, experiment.Buckets)

	// 创建ExperimentGroup
	expGroup := &pairecmodel.ExperimentGroup{
		ExpGroupId:      int64(experiment.ExpId),            // 实验ID映射到实验组ID
		LayerId:         layerId,                            // 所属Layer ID
		ExpRoomId:       roomId,                             // 所属Room ID
		SceneId:         sceneId,                            // 所属Scene ID
		ExpGroupName:    experiment.ExpName,                 // 实验名称映射到实验组名称
		ExpGroupInfo:    experiment.ExpInfo,                 // 实验信息映射到实验组信息
		DebugUsers:      experiment.DebugUsers,              // 调试用户
		Owner:           experiment.Owner,                   // 负责人
		Filter:          experiment.Filter,                  // 过滤条件
		ReserveBuckets:  experiment.Buckets,                 // 桶配置
		HoldingBuckets:  experiment.Buckets,                 // 桶配置 (兼容性)
		Status:          experiment.Status,                  // 状态
		CrowdTargetType: crowdTargetType,                    // 根据实验配置确定分流类型
		Experiments:     make([]*pairecmodel.Experiment, 0), // 初始化实验列表
	}

	// 遍历PAI Experiment中的ExperimentVersions，将每个ExperimentVersion转换为Pairec Experiment
	for _, expVersion := range experiment.ExperimentVersions() {
		pairecExperiment := convertExperimentVersionToPairecExperiment(expVersion, expGroup.ExpGroupId, layerId, roomId, sceneId)
		if pairecExperiment != nil {
			expGroup.AddExperiment(pairecExperiment)
		}
	}

	// 初始化ExperimentGroup，设置内部分流逻辑
	if err := expGroup.Init(); err != nil {
		log.Printf("[PAI-AB] ERROR: Failed to initialize ExperimentGroup '%s': %v", expGroup.ExpGroupName, err)
		return nil
	}

	// 输出详细的分流配置用于调试
	log.Printf("[PAI-AB] ExperimentGroup '%s' initialized with:", expGroup.ExpGroupName)
	log.Printf("[PAI-AB]   - ExpGroupId: %d", expGroup.ExpGroupId)
	log.Printf("[PAI-AB]   - CrowdTargetType: %s", expGroup.CrowdTargetType)
	log.Printf("[PAI-AB]   - DebugUsers: '%s'", expGroup.DebugUsers)
	log.Printf("[PAI-AB]   - Filter: '%s'", expGroup.Filter)
	log.Printf("[PAI-AB]   - ReserveBuckets: '%s'", expGroup.ReserveBuckets)
	log.Printf("[PAI-AB]   - HoldingBuckets: '%s'", expGroup.HoldingBuckets)
	log.Printf("[PAI-AB]   - Status: %d", expGroup.Status)

	// 测试用户898964的hash计算
	testUserId := "898964"
	hashValue := hashString(testUserId + "_" + fmt.Sprintf("%d", expGroup.ExpGroupId))
	bucketId := hashValue % 100
	log.Printf("[PAI-AB] TEST: User '%s' hash=%d, bucket=%d (should match if bucket in 0-99)", testUserId, hashValue, bucketId)

	log.Printf("[PAI-AB] ExperimentGroup conversion completed: %d experiment versions converted", len(expGroup.Experiments))
	return expGroup
}

// convertExperimentVersionToPairecExperiment 将PAI SDK中的ExperimentVersion转换为Pairec SDK中的Experiment
func convertExperimentVersionToPairecExperiment(expVersion *paiabtestmodel.ExperimentVersion, groupId int64, layerId int64, roomId int64, sceneId int64) *pairecmodel.Experiment {
	if expVersion == nil {
		log.Printf("[PAI-AB] WARN: Received nil experiment version for conversion")
		return nil
	}
	log.Printf("[PAI-AB] Converting experiment version '%s' (ID: %d) to Pairec Experiment", expVersion.ExpVersionName, expVersion.ExpVersionId)

	// 创建Pairec Experiment
	pairecExperiment := &pairecmodel.Experiment{
		ExperimentId:      int64(expVersion.ExpVersionId),    // ExperimentVersion ID映射到Experiment ID
		ExpGroupId:        groupId,                           // 所属实验组ID
		LayerId:           layerId,                           // 所属Layer ID
		ExpRoomId:         roomId,                            // 所属Room ID
		SceneId:           sceneId,                           // 所属Scene ID
		ExperimentName:    expVersion.ExpVersionName,         // 实验版本名称映射到实验名称
		ExperimentInfo:    expVersion.ExpVersionInfo,         // 实验版本信息映射到实验信息
		Type:              uint32(expVersion.ExpVersionType), // 实验类型（1 对照组 2 实验组）
		ExperimentFlow:    uint32(expVersion.ExperimentFlow), // 实验流量
		ExperimentBuckets: expVersion.Buckets,                // 实验桶配置
		DebugUsers:        expVersion.DebugUsers,             // 调试用户
		ExperimentConfig:  convertPaiConfigToPairecConfig(expVersion.ExpVersionConfig), // 实验配置参数（格式转换）
		Status:            1,                                 // 默认状态
	}

	// 初始化Experiment
	if err := pairecExperiment.Init(); err != nil {
		log.Printf("[PAI-AB] ERROR: Failed to initialize Experiment '%s': %v", pairecExperiment.ExperimentName, err)
		return nil
	}

	return pairecExperiment
}

// hashString 计算字符串的hash值
func hashString(s string) uint32 {
	h := sha1.New()
	h.Write([]byte(s))
	hashBytes := h.Sum(nil)

	// 取前4个字节转换为uint32
	return uint32(hashBytes[0])<<24 | uint32(hashBytes[1])<<16 | uint32(hashBytes[2])<<8 | uint32(hashBytes[3])
}

// convertPaiConfigToPairecConfig 将PAI的配置格式转换为Pairec的配置格式
// PAI格式: [{"key": "x", "value": "[...]", "type": "string"}]
// Pairec格式: {"x": [...]}
func convertPaiConfigToPairecConfig(paiConfig string) string {
	if paiConfig == "" {
		return "{}"
	}

	log.Printf("[PAI-AB] Converting config from PAI format to Pairec format")
	log.Printf("[PAI-AB] Original PAI config: %s", paiConfig)

	// 解析PAI格式的配置
	type PaiConfigItem struct {
		Key   string `json:"key"`
		Value string `json:"value"`
		Type  string `json:"type"`
	}

	var paiItems []PaiConfigItem
	if err := json.Unmarshal([]byte(paiConfig), &paiItems); err != nil {
		log.Printf("[PAI-AB] ERROR: Failed to parse PAI config JSON: %v", err)
		return "{}"
	}

	// 转换为Pairec格式
	pairecConfig := make(map[string]interface{})
	for _, item := range paiItems {
		log.Printf("[PAI-AB] Processing config item: key=%s, value=%s, type=%s", item.Key, item.Value, item.Type)
		
		// 尝试解析value为JSON
		var value interface{}
		if err := json.Unmarshal([]byte(item.Value), &value); err != nil {
			// 如果解析失败，使用原始字符串值
			log.Printf("[PAI-AB] WARN: Failed to parse value as JSON, using string: %v", err)
			value = item.Value
		}
		
		pairecConfig[item.Key] = value
	}

	// 序列化为JSON字符串
	resultBytes, err := json.Marshal(pairecConfig)
	if err != nil {
		log.Printf("[PAI-AB] ERROR: Failed to marshal Pairec config to JSON: %v", err)
		return "{}"
	}

	result := string(resultBytes)
	log.Printf("[PAI-AB] Converted Pairec config: %s", result)
	return result
}
