package redigo

type RedigoDB struct {
	ID int
}

func (r *RedigoDB) Add(key []byte, val interface{}) {

}

func (r *RedigoDB) Delete(key []byte) {

}

func (r *RedigoDB) SetKey(key []byte, val interface{}) {

}

func (r *RedigoDB) LookupKey(key []byte) interface{} {
	return nil
}

func (r *RedigoDB) LookupKeyRead(key []byte) interface{} {
	return nil
}

func (r *RedigoDB) LookupKeyWrite(key []byte) interface{} {
	return nil
}

func (r *RedigoDB) SignalModifyKey(key []byte) {

}
