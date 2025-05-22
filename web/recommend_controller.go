package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/alibaba/pairec/v2/abtest"
	"github.com/alibaba/pairec/v2/context"
	"github.com/alibaba/pairec/v2/log"
	"github.com/alibaba/pairec/v2/recconf"
	"github.com/alibaba/pairec/v2/service"
	"github.com/alibaba/pairec/v2/utils"
	pairecModel "github.com/aliyun/aliyun-pairec-config-go-sdk/v2/model" // Aliased
	"os"                                                                  // Added
	"github.com/alibaba/pairec/v2/paiabtest"                            // Added
	paiModel "github.com/aliyun/aliyun-pai-ab-go-sdk/model"             // Added
)

const (
	Default_Size int = 10
)

type RecommendParam struct {
	SceneId  string                 `json:"scene_id"`
	Category string                 `json:"category"`
	Uid      string                 `json:"uid"`  // user id
	Size     int                    `json:"size"` // get recommend items size
	Debug    bool                   `json:"debug"`
	Features map[string]interface{} `json:"features"`
}

func (r *RecommendParam) GetParameter(name string) interface{} {
	if name == "uid" {
		return r.Uid
	} else if name == "scene" {
		return r.SceneId
	} else if name == "category" {
		if r.Category != "" {
			return r.Category
		}
		return "default"
	} else if name == "features" {
		return r.Features
	}

	return nil
}

type RecommendResponse struct {
	Response
	Size  int         `json:"size"`
	Items []*ItemData `json:"items"`
}
type ItemData struct {
	ItemId     string `json:"item_id"`
	ItemType   string `json:"item_type"`
	RetrieveId string `json:"retrieve_id"`
}

func (r *RecommendResponse) ToString() string {
	j, _ := json.Marshal(r)
	return string(j)
}

type RecommendController struct {
	Controller
	param   RecommendParam
	context *context.RecommendContext
}

func (c *RecommendController) Process(w http.ResponseWriter, r *http.Request) {
	c.Start = time.Now()
	var err error
	c.RequestBody, err = io.ReadAll(r.Body)
	if err != nil {
		c.SendError(w, ERROR_PARAMETER_CODE, "read parammeter error")
		return
	}
	if len(c.RequestBody) == 0 {
		c.SendError(w, ERROR_PARAMETER_CODE, "request body empty")
		return
	}
	c.RequestId = utils.UUID()
	c.LogRequestBegin(r)
	if err := c.CheckParameter(); err != nil {
		c.SendError(w, ERROR_PARAMETER_CODE, err.Error())
		return
	}
	c.doProcess(w, r)
	c.End = time.Now()
	c.LogRequestEnd(r)
}
func (r *RecommendController) CheckParameter() error {
	if err := json.Unmarshal(r.RequestBody, &r.param); err != nil {
		return err
	}

	if len(r.param.Uid) == 0 {
		return errors.New("uid not empty")
	}
	if r.param.Size <= 0 {
		r.param.Size = Default_Size
	}
	if r.param.SceneId == "" {
		r.param.SceneId = "default_scene"
	}
	if r.param.Category == "" {
		r.param.Category = "default"
	}

	return nil
}
func (c *RecommendController) doProcess(w http.ResponseWriter, r *http.Request) {
	c.makeRecommendContext()
	userRecommendService := service.NewUserRecommendService()
	items := userRecommendService.Recommend(c.context)
	data := make([]*ItemData, 0)
	for _, item := range items {
		if c.param.Debug {
			fmt.Println(item)
		}

		idata := &ItemData{
			ItemId:     string(item.Id),
			ItemType:   item.ItemType,
			RetrieveId: item.RetrieveId,
		}

		data = append(data, idata)
	}

	if len(data) < c.param.Size {
		response := RecommendResponse{
			Size:  len(data),
			Items: data,
			Response: Response{
				RequestId: c.RequestId,
				Code:      299,
				Message:   "items size not enough",
			},
		}
		io.WriteString(w, response.ToString())
		return
	}

	response := RecommendResponse{
		Size:  len(data),
		Items: data,
		Response: Response{
			RequestId: c.RequestId,
			Code:      200,
			Message:   "success",
		},
	}
	io.WriteString(w, response.ToString())
}
func (c *RecommendController) makeRecommendContext() {
	c.context = context.NewRecommendContext()
	c.context.Size = c.param.Size
	c.context.Debug = c.param.Debug
	c.context.Param = &c.param
	c.context.RecommendId = c.RequestId
	c.context.Config = recconf.Config

	abProvider := os.Getenv("ABTEST_PROVIDER")
	if abProvider == "" {
		abProvider = "pairec" // Default to pairec
	}

	log.Info(fmt.Sprintf("Using ABTEST_PROVIDER: %s", abProvider))

	if abProvider == "pai" {
		paiClient := paiabtest.GetClient()
		if paiClient == nil {
			log.Warning("PAI A/B Test client (paiabtest.GetClient) is nil. Skipping PAI A/B experiment.")
		} else {
			// Ensure c.param.Features is not nil, as PAI SDK might expect non-nil map
			sceneParams := c.param.Features
			if sceneParams == nil {
				sceneParams = make(map[string]interface{})
			}
			paiResult, err := paiabtest.MatchPaiExperiment(c.param.Uid, c.RequestId, sceneParams)
			if err != nil {
				log.Error(fmt.Sprintf("Error calling MatchPaiExperiment: %v", err))
			} else if paiResult == nil {
				log.Warning("MatchPaiExperiment returned nil result.")
			} else {
				c.context.ExperimentResult = paiResult // Assigning *paiModel.ExperimentResult to interface{}
				log.Info(fmt.Sprintf("PAI A/B Test Result Info: %s", paiResult.Info()))
			}
		}
	} else { // "pairec" or default
		if abtest.GetExperimentClient() != nil {
			abcontext := pairecModel.ExperimentContext{ // Use aliased model
				Uid:          c.param.Uid,
				RequestId:    c.RequestId,
				FilterParams: c.param.Features, // Pass c.param.Features, ensure it's map[string]interface{}
			}
			if abcontext.FilterParams == nil {
				abcontext.FilterParams = make(map[string]interface{})
			}

			pairecResult := abtest.GetExperimentClient().MatchExperiment(c.param.SceneId, &abcontext)
			if pairecResult != nil {
				c.context.ExperimentResult = pairecResult // Assigning *pairecModel.ExperimentResult to interface{}
				log.Info(fmt.Sprintf("Pairec A/B Test Result Info: %s", pairecResult.Info()))
			} else {
				log.Warning("Pairec A/B Test MatchExperiment returned nil result.")
			}
		} else {
			log.Warning("Pairec A/B Test client (abtest.GetExperimentClient) is nil. Skipping Pairec A/B experiment.")
		}
	}
}
