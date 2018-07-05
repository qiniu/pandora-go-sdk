package logkit

import (
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
	"github.com/qiniu/pandora-go-sdk/base/config"
	"github.com/qiniu/pandora-go-sdk/logkit"
)

var (
	cfg      *config.Config
	client   *logkit.Logkit
	endpoint = config.DefaultPipelineEndpoint
	ak       = "<AccessKey>"
	sk       = "<SecretKey>"
	logger   base.Logger
)

func init() {
	var err error
	logger = base.NewDefaultLogger()
	cfg = logkit.NewConfig().
		WithEndpoint(endpoint).
		WithAccessKeySecretKey(ak, sk).
		WithLogger(logger).
		WithLoggerLevel(base.LogDebug)

	client, err = logkit.New(cfg)
	if err != nil {
		logger.Fatalf("Failed to create new logkit client: %v", err)
	}
}

func Sample_Agents() {
	// Get all agents
	agents, total, err := client.GetAgents(&logkit.GetAgentsOptions{})
	if err != nil {
		logger.Fatalf("Failed to get all agents: %v", err)
	}
	logger.Info(agents, total)

	// Get online agents
	agents, total, err = client.GetAgents(&logkit.GetAgentsOptions{
		State: logkit.StateOnline,
	})
	if err != nil {
		logger.Fatalf("Failed to get online agents: %v", err)
	}
	logger.Info(agents, total)

	// Delete an agent
	err = client.DeleteAgents(&logkit.DeleteAgentsOptions{
		ID: "<put_your_agent_id_here>",
	})
	if err != nil {
		logger.Fatalf("Failed to delete an agent: %v", err)
	}

	// Delete offline agents
	err = client.DeleteAgents(&logkit.DeleteAgentsOptions{
		State: logkit.StateOffline,
	})
	if err != nil {
		logger.Fatalf("Failed to delete offline agents: %v", err)
	}

	// Delete some agents
	err = client.BatchDeleteAgents(&logkit.BatchDeleteAgentsOptions{
		IDs: []string{
			"<put_your_agent_id_here>",
			"<put_another_agent_id_here>",
		},
	})
	if err != nil {
		logger.Fatalf("Failed to delete some agents: %v", err)
	}
}

func Sample_Configs() {
	// Get all configs
	configs, total, err := client.GetConfigs(&logkit.GetConfigsOptions{})
	if err != nil {
		logger.Fatalf("Failed to get all configs: %v", err)
	}
	logger.Info(configs, total)

	// Get a config
	configs, total, err = client.GetConfigs(&logkit.GetConfigsOptions{
		Name: "<put_your_config_name_here>",
	})
	if err != nil {
		logger.Fatalf("Failed to get a config: %v", err)
	}
	logger.Info(configs, total)

	// Delete a config
	err = client.DeleteConfig(&logkit.DeleteConfigOptions{
		Name: "<put_your_config_name_here>",
	})
	if err != nil {
		logger.Fatalf("Failed to delete a config: %v", err)
	}
}

func Sample_Metrics() {
	// Get agent metrics for last hour
	metrics, err := client.GetAgentMetrics(&logkit.GetAgentMetricsOptions{
		AgentID:   "<put_your_agent_id_here>",
		BeginTime: time.Now().Add(-1 * time.Hour).Unix(),
		EndTime:   time.Now().Unix(),
	})
	if err != nil {
		logger.Fatalf("Failed to get agent metrics: %v", err)
	}
	logger.Info(metrics)
}

func Sample_Runners() {
	// Get all runners without agents info
	runners, _, total, err := client.GetRunners(&logkit.GetRunnersOptions{})
	if err != nil {
		logger.Fatalf("Failed to get all runners: %v", err)
	}
	logger.Info(runners, total)

	// Get all runners with agents info
	var agents map[string]interface{}
	runners, agents, total, err = client.GetRunners(&logkit.GetRunnersOptions{
		IncludeAgents: true,
	})
	if err != nil {
		logger.Fatalf("Failed to get all runners: %v", err)
	}
	logger.Info(runners, agents, total)

	// Start runner(s)
	err = client.StartRunners(&logkit.BatchRunnersOptions{
		RunnerConds: []logkit.RunnerCond{
			{
				ConfigName: "<put_your_config_name_here>",
				AgentID:    "<put_your_agent_id_here>",
			},
		},
	})
	if err != nil {
		logger.Fatalf("Failed to start runner(s): %v", err)
	}

	// Stop runner(s)
	err = client.StopRunners(&logkit.BatchRunnersOptions{
		RunnerConds: []logkit.RunnerCond{
			{
				ConfigName: "<put_your_config_name_here>",
				AgentID:    "<put_your_agent_id_here>",
			},
		},
	})
	if err != nil {
		logger.Fatalf("Failed to stop runner(s): %v", err)
	}

	// Delete runner(s)
	err = client.DeleteRunners(&logkit.BatchRunnersOptions{
		RunnerConds: []logkit.RunnerCond{
			{
				ConfigName: "<put_your_config_name_here>",
				AgentID:    "<put_your_agent_id_here>",
			},
		},
	})
	if err != nil {
		logger.Fatalf("Failed to delete runner(s): %v", err)
	}
}

func Sample_Tags() {
	// Get all tags without agents info
	tags, _, total, err := client.GetTags(&logkit.GetTagsOptions{})
	if err != nil {
		logger.Fatalf("Failed to get all tags: %v", err)
	}
	logger.Info(tags, total)

	// Create a new tag
	err = client.NewTag(&logkit.NewTagOptions{
		Tag: &logkit.Tag{
			Name: "<put_your_tag_name_here>",
			Note: "<put_your_tag_note_here>",
		},
	})
	if err != nil {
		logger.Fatalf("Failed to creata a new tag: %v", err)
	}

	// Update tag's note
	err = client.UpdateTagNote(&logkit.UpdateTagNoteOptions{
		Note: "<put_your_tag_note_here>",
	})
	if err != nil {
		logger.Fatalf("Failed to update tag's note: %v", err)
	}

	// Delete a tag
	err = client.DeleteTag(&logkit.DeleteTagOptions{
		Name: "<put_your_tag_name_here>",
	})
	if err != nil {
		logger.Fatalf("Failed to deleta a tag: %v", err)
	}
}
