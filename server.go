package redigo

type RedigoServer struct {
	DB *RedigoDB
	// DB persistence
	Dirty uint // changes to DB from the last save
}
