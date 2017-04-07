package redigo

var (
	MaxIntsetEntries = 512
)

type RedigoServer struct {
	DB []*RedigoDB
	// DB persistence
	Dirty int // changes to DB from the last save
	// Fields used only for stas
	KeyspaceHits   int
	KeyspaceMisses int
}
