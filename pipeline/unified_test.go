package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAutoExport(t *testing.T) {
	xxx := map[string]interface{}{
		"abc": "123",
	}
	/*
	   kodoExportSpecStr := `
	   	"bucket":"bucket1",
	   		"email": "abc@qiniu.com",
	   		"accessKey": "ak",
	   		"fields": {
	   			"key1": "#key1",
	   			"key2": "#key2",
	   		},
	   			"format": "parquet",
	   			"retention": 30
	   		}`
	*/
	_, ok := xxx["xsxs"].(string)
	assert.Equal(t, ok, false)

}

func TestGetSeriesName(t *testing.T) {
	seriesTag := map[string][]string{
		"a":     []string{},
		"a_b":   []string{},
		"a_b_c": []string{},
	}
	series := getSeriesName(seriesTag, "")
	assert.Equal(t, series, "")
	series = getSeriesName(seriesTag, "a")
	assert.Equal(t, series, "")
	series = getSeriesName(seriesTag, "b")
	assert.Equal(t, series, "")
	series = getSeriesName(seriesTag, "a_")
	assert.Equal(t, series, "a")
	series = getSeriesName(seriesTag, "b_")
	assert.Equal(t, series, "")
	series = getSeriesName(seriesTag, "a_b")
	assert.Equal(t, series, "a")
	series = getSeriesName(seriesTag, "a_b_")
	assert.Equal(t, series, "a_b")
	series = getSeriesName(seriesTag, "a_b_c")
	assert.Equal(t, series, "a_b")
	series = getSeriesName(seriesTag, "a_b_c_ddd")
	assert.Equal(t, series, "a_b_c")

	seriesTag = map[string][]string{
		"cpu":             []string{},
		"system":          []string{},
		"processes":       []string{},
		"netstat":         []string{},
		"net":             []string{},
		"mem":             []string{},
		"swap":            []string{},
		"kernel_vmstat":   []string{},
		"kernel":          []string{},
		"disk":            []string{},
		"diskio":          []string{},
		"linux_sysctl_fs": []string{},
	}

	series = getSeriesName(seriesTag, "system_load1")
	assert.Equal(t, "system", series)
	series = getSeriesName(seriesTag, "system_n_users")
	assert.Equal(t, "system", series)
	series = getSeriesName(seriesTag, "processes_total")
	assert.Equal(t, "processes", series)
	series = getSeriesName(seriesTag, "processes_total_threads")
	assert.Equal(t, "processes", series)
	series = getSeriesName(seriesTag, "netstat_tcp_syn_sent")
	assert.Equal(t, "netstat", series)
	series = getSeriesName(seriesTag, "netstat_tcp_none")
	assert.Equal(t, "netstat", series)
	series = getSeriesName(seriesTag, "net_err_in")
	assert.Equal(t, "net", series)
	series = getSeriesName(seriesTag, "net_packets_recv")
	assert.Equal(t, "net", series)
	series = getSeriesName(seriesTag, "mem_total")
	assert.Equal(t, "mem", series)
	series = getSeriesName(seriesTag, "mem_available_percent")
	assert.Equal(t, "mem", series)
	series = getSeriesName(seriesTag, "swap_total")
	assert.Equal(t, "swap", series)
	series = getSeriesName(seriesTag, "swap_used_percent")
	assert.Equal(t, "swap", series)
	series = getSeriesName(seriesTag, "cpu_usage_cpu")
	assert.Equal(t, "cpu", series)
	series = getSeriesName(seriesTag, "cpu_time_cpu")
	assert.Equal(t, "cpu", series)
	series = getSeriesName(seriesTag, "kernel_vmstat_kswapd_inodesteal")
	assert.Equal(t, "kernel_vmstat", series)
	series = getSeriesName(seriesTag, "kernel_vmstat_thp_zero_page_alloc_failed")
	assert.Equal(t, "kernel_vmstat", series)
	series = getSeriesName(seriesTag, "linux_sysctl_fs_super_nr")
	assert.Equal(t, "linux_sysctl_fs", series)
	series = getSeriesName(seriesTag, "linux_sysctl_fs_inode_preshrink_nr")
	assert.Equal(t, "linux_sysctl_fs", series)
	series = getSeriesName(seriesTag, "kernel_context_switches")
	assert.Equal(t, "kernel", series)
	series = getSeriesName(seriesTag, "kernel_interrupts")
	assert.Equal(t, "kernel", series)
	series = getSeriesName(seriesTag, "disk_free")
	assert.Equal(t, "disk", series)
	series = getSeriesName(seriesTag, "disk_inodes_free")
	assert.Equal(t, "disk", series)
	series = getSeriesName(seriesTag, "diskio_read_time")
	assert.Equal(t, "diskio", series)
	series = getSeriesName(seriesTag, "diskio_name")
	assert.Equal(t, "diskio", series)
}
