package odps

const DefaultJobPriority = 9

// Job 用于获取instance接口返回的Job信息, 接口为"instance接口?source"
type Job struct {
	Name             string
	Comment          string
	Owner            string
	CreationTime     GMTTime
	LastModifiedTime GMTTime
	Priority         int
	RunningCluster   string
	Tasks            []Task `xml:"Tasks>Task"`
}
