package redigo

type RedigoDB struct {
	ID int
}

func (r *RedigoDB) Add(key string, val interface{}) {

}

func (r *RedigoDB) Delete(key string) {

}

func (r *RedigoDB) SetKey(key string, val interface{}) {

}

func (r *RedigoDB) LookupKey(key string) interface{} {
	return nil
}

func (r *RedigoDB) LookupKeyRead(key string) interface{} {
	return nil
}

func (r *RedigoDB) LookupKeyWrite(key string) interface{} {
	return nil
}

func (r *RedigoDB) SignalModifyKey(key string) {

}
