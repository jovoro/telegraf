package slurm

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	goslurm "github.com/jovoro/goslurm/v0044"

	"github.com/influxdata/telegraf"
)

type slurmV0044 struct {
	client   *goslurm.APIClient
	username string
	token    string
}

func newV0044Client(host, scheme, userAgent string, httpClient *http.Client, username, token string) *slurmV0044 {
	cfg := goslurm.NewConfiguration()
	cfg.Host = host
	cfg.Scheme = scheme
	cfg.UserAgent = userAgent
	cfg.HTTPClient = httpClient
	return &slurmV0044{
		client:   goslurm.NewAPIClient(cfg),
		username: username,
		token:    token,
	}
}

func (s *slurmV0044) authCtx() context.Context {
	return context.WithValue(
		context.Background(),
		goslurm.ContextAPIKeys,
		map[string]goslurm.APIKey{
			"user":  {Key: s.username},
			"token": {Key: s.token},
		},
	)
}

func (s *slurmV0044) gatherDiag(acc telegraf.Accumulator, source string) error {
	resp, raw, err := s.client.SlurmAPI.SlurmV0044GetDiag(s.authCtx()).Execute()
	if err != nil {
		return fmt.Errorf("error getting diag: %w", err)
	}
	raw.Body.Close()

	diag, ok := resp.GetStatisticsOk()
	if !ok {
		return nil
	}

	records := make(map[string]interface{}, 13)
	tags := map[string]string{"source": source}

	if v, ok := diag.GetServerThreadCountOk(); ok {
		records["server_thread_count"] = *v
	}
	if v, ok := diag.GetJobsCanceledOk(); ok {
		records["jobs_canceled"] = *v
	}
	if v, ok := diag.GetJobsSubmittedOk(); ok {
		records["jobs_submitted"] = *v
	}
	if v, ok := diag.GetJobsStartedOk(); ok {
		records["jobs_started"] = *v
	}
	if v, ok := diag.GetJobsCompletedOk(); ok {
		records["jobs_completed"] = *v
	}
	if v, ok := diag.GetJobsFailedOk(); ok {
		records["jobs_failed"] = *v
	}
	if v, ok := diag.GetJobsPendingOk(); ok {
		records["jobs_pending"] = *v
	}
	if v, ok := diag.GetJobsRunningOk(); ok {
		records["jobs_running"] = *v
	}
	if v, ok := diag.GetScheduleCycleLastOk(); ok {
		records["schedule_cycle_last"] = *v
	}
	if v, ok := diag.GetScheduleCycleMeanOk(); ok {
		records["schedule_cycle_mean"] = *v
	}
	if v, ok := diag.GetBfQueueLenOk(); ok {
		records["bf_queue_len"] = *v
	}
	if v, ok := diag.GetBfQueueLenMeanOk(); ok {
		records["bf_queue_len_mean"] = *v
	}
	if v, ok := diag.GetBfActiveOk(); ok {
		records["bf_active"] = *v
	}

	acc.AddFields("slurm_diag", records, tags)
	return nil
}

func (s *slurmV0044) gatherJobs(acc telegraf.Accumulator, source string) error {
	resp, raw, err := s.client.SlurmAPI.SlurmV0044GetJobs(s.authCtx()).Execute()
	if err != nil {
		return fmt.Errorf("error getting jobs: %w", err)
	}
	raw.Body.Close()

	jobs, ok := resp.GetJobsOk()
	if !ok {
		return nil
	}

	for i := range jobs {
		records := make(map[string]interface{}, 21)
		tags := make(map[string]string, 3)

		tags["source"] = source
		if v, ok := jobs[i].GetNameOk(); ok {
			tags["name"] = *v
		}
		if v, ok := jobs[i].GetJobIdOk(); ok {
			tags["job_id"] = strconv.Itoa(int(*v))
		}

		if v, ok := jobs[i].GetJobStateOk(); ok {
			records["state"] = v
		}
		if v, ok := jobs[i].GetStateReasonOk(); ok {
			records["state_reason"] = *v
		}
		if v, ok := jobs[i].GetPartitionOk(); ok {
			records["partition"] = *v
		}
		if v, ok := jobs[i].GetNodesOk(); ok {
			records["nodes"] = *v
		}
		if v, ok := jobs[i].GetNodeCountOk(); ok {
			records["node_count"] = v.GetNumber()
		}
		if v, ok := jobs[i].GetPriorityOk(); ok {
			records["priority"] = v.GetNumber()
		}
		if v, ok := jobs[i].GetNiceOk(); ok {
			records["nice"] = *v
		}
		if v, ok := jobs[i].GetGroupIdOk(); ok {
			records["group_id"] = *v
		}
		if v, ok := jobs[i].GetCommandOk(); ok {
			records["command"] = *v
		}
		if v, ok := jobs[i].GetStandardOutputOk(); ok {
			records["standard_output"] = strings.ReplaceAll(*v, "\\", "")
		}
		if v, ok := jobs[i].GetStandardErrorOk(); ok {
			records["standard_error"] = strings.ReplaceAll(*v, "\\", "")
		}
		if v, ok := jobs[i].GetStandardInputOk(); ok {
			records["standard_input"] = strings.ReplaceAll(*v, "\\", "")
		}
		if v, ok := jobs[i].GetCurrentWorkingDirectoryOk(); ok {
			records["current_working_directory"] = strings.ReplaceAll(*v, "\\", "")
		}
		if v, ok := jobs[i].GetSubmitTimeOk(); ok {
			records["submit_time"] = v.GetNumber()
		}
		if v, ok := jobs[i].GetStartTimeOk(); ok {
			records["start_time"] = v.GetNumber()
		}
		if v, ok := jobs[i].GetCpusOk(); ok {
			records["cpus"] = v.GetNumber()
		}
		if v, ok := jobs[i].GetTasksOk(); ok {
			records["tasks"] = v.GetNumber()
		}
		if v, ok := jobs[i].GetTimeLimitOk(); ok {
			records["time_limit"] = v.GetNumber()
		}
		if v, ok := jobs[i].GetTresReqStrOk(); ok {
			for k, val := range parseTres(*v) {
				records["tres_"+k] = val
			}
		}

		acc.AddFields("slurm_jobs", records, tags)
	}
	return nil
}

func (s *slurmV0044) gatherNodes(acc telegraf.Accumulator, source string) error {
	resp, raw, err := s.client.SlurmAPI.SlurmV0044GetNodes(s.authCtx()).Execute()
	if err != nil {
		return fmt.Errorf("error getting nodes: %w", err)
	}
	raw.Body.Close()

	nodes, ok := resp.GetNodesOk()
	if !ok {
		return nil
	}

	for i := range nodes {
		records := make(map[string]interface{}, 13)
		tags := make(map[string]string, 2)

		tags["source"] = source
		if v, ok := nodes[i].GetNameOk(); ok {
			tags["name"] = *v
		}

		if v, ok := nodes[i].GetStateOk(); ok {
			records["state"] = strings.Join(v, ",")
		}
		if v, ok := nodes[i].GetCoresOk(); ok {
			records["cores"] = *v
		}
		if v, ok := nodes[i].GetCpusOk(); ok {
			records["cpus"] = *v
		}
		if v, ok := nodes[i].GetCpuLoadOk(); ok {
			records["cpu_load"] = *v
		}
		if v, ok := nodes[i].GetAllocCpusOk(); ok {
			records["alloc_cpu"] = *v
		}
		if v, ok := nodes[i].GetRealMemoryOk(); ok {
			records["real_memory"] = *v
		}
		if v, ok := nodes[i].GetFreeMemOk(); ok {
			records["free_memory"] = v.GetNumber()
		}
		if v, ok := nodes[i].GetAllocMemoryOk(); ok {
			records["alloc_memory"] = *v
		}
		if v, ok := nodes[i].GetTresOk(); ok {
			for k, val := range parseTres(*v) {
				records["tres_"+k] = val
			}
		}
		if v, ok := nodes[i].GetTresUsedOk(); ok {
			for k, val := range parseTres(*v) {
				records["tres_used_"+k] = val
			}
		}
		if v, ok := nodes[i].GetWeightOk(); ok {
			records["weight"] = *v
		}
		if v, ok := nodes[i].GetVersionOk(); ok {
			records["slurmd_version"] = *v
		}
		if v, ok := nodes[i].GetArchitectureOk(); ok {
			records["architecture"] = *v
		}

		acc.AddFields("slurm_nodes", records, tags)
	}
	return nil
}

func (s *slurmV0044) gatherPartitions(acc telegraf.Accumulator, source string) error {
	resp, raw, err := s.client.SlurmAPI.SlurmV0044GetPartitions(s.authCtx()).Execute()
	if err != nil {
		return fmt.Errorf("error getting partitions: %w", err)
	}
	raw.Body.Close()

	partitions, ok := resp.GetPartitionsOk()
	if !ok {
		return nil
	}

	for _, partition := range partitions {
		records := make(map[string]interface{}, 5)
		tags := make(map[string]string, 2)

		tags["source"] = source
		if v, ok := partition.GetNameOk(); ok {
			tags["name"] = *v
		}

		if p, ok := partition.GetPartitionOk(); ok {
			if v, ok := p.GetStateOk(); ok {
				records["state"] = strings.Join(v, ",")
			}
		}
		if cpus, ok := partition.GetCpusOk(); ok {
			if v, ok := cpus.GetTotalOk(); ok {
				records["total_cpu"] = *v
			}
		}
		if nodes, ok := partition.GetNodesOk(); ok {
			if v, ok := nodes.GetTotalOk(); ok {
				records["total_nodes"] = *v
			}
			if v, ok := nodes.GetConfiguredOk(); ok {
				records["nodes"] = *v
			}
		}
		if tres, ok := partition.GetTresOk(); ok {
			if v, ok := tres.GetConfiguredOk(); ok {
				for k, val := range parseTres(*v) {
					records["tres_"+k] = val
				}
			}
		}

		acc.AddFields("slurm_partitions", records, tags)
	}
	return nil
}

func (s *slurmV0044) gatherReservations(acc telegraf.Accumulator, source string) error {
	resp, raw, err := s.client.SlurmAPI.SlurmV0044GetReservations(s.authCtx()).Execute()
	if err != nil {
		return fmt.Errorf("error getting reservations: %w", err)
	}
	raw.Body.Close()

	reservations, ok := resp.GetReservationsOk()
	if !ok {
		return nil
	}

	for _, reservation := range reservations {
		records := make(map[string]interface{}, 8)
		tags := make(map[string]string, 2)

		tags["source"] = source
		if v, ok := reservation.GetNameOk(); ok {
			tags["name"] = *v
		}

		if v, ok := reservation.GetCoreCountOk(); ok {
			records["core_count"] = *v
		}
		if v, ok := reservation.GetGroupsOk(); ok {
			records["groups"] = *v
		}
		if v, ok := reservation.GetUsersOk(); ok {
			records["users"] = *v
		}
		if v, ok := reservation.GetStartTimeOk(); ok {
			records["start_time"] = v.GetNumber()
		}
		if v, ok := reservation.GetPartitionOk(); ok {
			records["partition"] = *v
		}
		if v, ok := reservation.GetAccountsOk(); ok {
			records["accounts"] = *v
		}
		if v, ok := reservation.GetNodeCountOk(); ok {
			records["node_count"] = *v
		}
		if v, ok := reservation.GetNodeListOk(); ok {
			records["node_list"] = *v
		}

		acc.AddFields("slurm_reservations", records, tags)
	}
	return nil
}
